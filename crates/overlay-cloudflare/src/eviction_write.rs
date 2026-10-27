//! The eviction's atomic move, release list and released-hop evidence (#893).

use crate::admission_abort::{AbortRows, PendingAbort};
use crate::admit_fast::{
    ensure_shadow, merge_released, move_sql, readmitted_after, release_pot_spends_sql,
    restore_twins, select_pot_spends_sql, ColumnInfo, EvictionCore, ReleasedSpend, ShadowDb,
    CLOSE_EVICTION_MARKER_SQL, MOVED_TABLES, OPEN_EVICTION_MARKER_SQL, RELEASED_SPENDS_SQL,
};
use crate::d1::Query;
use worker::D1Database;

/// Fourth marker source for a hop. The ledger's readmission supersedes the row without deleting it.
/// Matching the eviction epoch also hides old evidence while a later eviction is still being prepared.
pub const RELEASED_HOP_READ_SQL: &str = "SELECT h.hopTxid,h.hopVout,h.identity,h.gameId,h.potTxid,h.seatSettlePubkey,h.hopSats,h.evictedAt FROM released_hop_evidence h JOIN pot_evictions e ON e.txid=h.potTxid WHERE h.hopTxid=?1 AND h.hopVout=?2 AND e.readmittedAt IS NULL AND h.evictedAt=e.evictedAt";

/// The same fourth marker source for one identity's game, using its migration's index.
pub const RELEASED_GAME_READ_SQL: &str = "SELECT h.hopTxid,h.hopVout,h.identity,h.gameId,h.potTxid,h.seatSettlePubkey,h.hopSats,h.evictedAt FROM released_hop_evidence h INDEXED BY idx_released_hop_evidence_identity_game JOIN pot_evictions e ON e.txid=h.potTxid WHERE h.identity=?1 AND h.gameId=?2 AND e.readmittedAt IS NULL AND h.evictedAt=e.evictedAt";

/// Read the candidate keys only. The INSERT rechecks their signatures and commitment on the
/// actual evicted rows inside the transaction, after all the move statements have run.
pub const SEAT_KEYS_SQL: &str = "SELECT DISTINCT lower(seatSettlePubkey) AS key FROM potparty_records_evicted INDEXED BY idx_evicted_party_pot WHERE potTxid=?1 AND sigValid=1 AND seatSettlePubkey IS NOT NULL UNION SELECT DISTINCT lower(seatSettlePubkey) AS key FROM potparty_records INDEXED BY idx_potparty_pot WHERE potTxid=?1 AND sigValid=1 AND seatSettlePubkey IS NOT NULL";

// A newer readmission must abort the whole batch before its first move. Deliberately duplicate
// that ledger row's primary key: unlike a read outside the batch this cannot race the moves.
const READMISSION_FENCE_SQL: &str = "INSERT INTO pot_evictions (txid,reason,evictedAt) SELECT txid,reason,evictedAt FROM pot_evictions WHERE txid=?1 AND readmittedAt IS NOT NULL";

/// Checks 1 and 2 of owed::markerless_released_hops, on the rows this batch moved:
/// one verified game/key per identity, all structurally valid commitments agreeing, K on exactly one seat.
/// Check 1's attestation is ANY ONE of the identity's verified twin party row or its VERIFIED hop marker for this
/// hop (`?2:?3`; register row D1's #610, D2-OWED-HOME-MARKER): the marker arm names only an identity with no
/// verified twin party row, so a seat the party row binds is judged exactly as before.
/// Check 3 is the hash-verified BEEF output comparison in evidence_inserts below.
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

/// D2-OWED-FOLD-8: the released outpoints of `?2` (a JSON array of `{"txid","vout"}`; `?3` whether the bytes
/// parsed) the evicted JOIN's stored bytes cannot judge (absent, undecodable, or not carrying the hop's transaction), named for each verified seat of the JOIN
/// by check 1's seats (one game and key per identity), each a released `pot_records` spend of the open eviction (a
/// ledger entry with no `table` field is a `pot_records` spend, as the old walk read it), and for each identity with
/// no verified twin party row whose VERIFIED hop marker names a listed hop (D2-OWED-HOME-MARKER). The judge serves
/// each as the old walk's could-not-judge row; a later evidence row for the hop takes it over.
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

const COUNT_SKIPPED_SQL: &str = "INSERT INTO ops_counters (name,value) SELECT 'released_hop_evidence_skipped_total',COUNT(*) FROM pot_evictions e,json_each(e.releasedSpends) r WHERE e.txid=?1 AND e.readmittedAt IS NULL AND json_extract(r.value,'$.table')='pot_records' AND NOT EXISTS (SELECT 1 FROM released_hop_evidence h WHERE h.hopTxid=lower(json_extract(r.value,'$.txid')) AND h.hopVout=json_extract(r.value,'$.vout') AND h.potTxid=e.txid AND h.evictedAt=e.evictedAt) ON CONFLICT(name) DO UPDATE SET value=ops_counters.value+excluded.value";

#[derive(Default)]
pub(crate) struct Snapshot {
    pub tables: Vec<(&'static str, Vec<ColumnInfo>)>,
    pub released: Vec<ReleasedSpend>,
    pub moved: u64,
    pub beef: Option<Vec<u8>>,
    pub seat_keys: Vec<String>,
    pub pot_vouts: Vec<u32>,
    pub advert_vouts: Vec<u32>,
    pub fresh_spends: bool,
}

/// The writer's evidence for one eviction from its JOIN's bytes (check 3, the shared
/// [`overlay_discovery::pot::released_evidence::evidence_writes`]): an evidence statement per hop output paying a
/// candidate key, then one missing-evidence statement naming the released hops the bytes cannot judge (every one when
/// the bytes are absent or undecodable). The app layer's `owed-v2` backfill binds the same writes to the same two
/// statements over an eviction made before this writer existed (D2-OWED-FOLD-8).
fn evidence_inserts(snapshot: &Snapshot, txid: &str) -> Vec<Query> {
    use overlay_discovery::pot::released_evidence::{evidence_writes, EvidenceWrite};
    let released: Vec<(String, u32)> = snapshot
        .released
        .iter()
        .filter(|r| r.table == "pot_records")
        .map(|r| (r.txid.to_ascii_lowercase(), r.vout))
        .collect();
    evidence_writes(
        snapshot.beef.as_deref(),
        txid,
        &released,
        &snapshot.seat_keys,
    )
    .into_iter()
    .map(|w| match w {
        EvidenceWrite::Found(f) => Query::new(EVIDENCE_INSERT_SQL)
            .bind(txid)
            .bind(f.hop_txid)
            .bind(f.hop_vout)
            .bind(f.seat_key)
            .bind(f.sats),
        EvidenceWrite::Missing { list, bytes_read } => Query::new(MISSING_INSERT_SQL)
            .bind(txid)
            .bind(list)
            .bind(i64::from(bytes_read)),
    })
    .collect()
}

fn merge_releases_sql(cols: &[ColumnInfo]) -> String {
    let confirmed = if cols.iter().any(|c| c.name == "spentConfirmed") {
        " AND +spentConfirmed=0"
    } else {
        ""
    };
    format!("UPDATE pot_evictions SET releasedSpends=(SELECT json_group_array(json_object('table',r.tbl,'txid',r.txid,'vout',r.vout)) FROM (SELECT json_extract(value,'$.table') AS tbl,lower(json_extract(value,'$.txid')) AS txid,json_extract(value,'$.vout') AS vout FROM json_each(CASE WHEN json_valid(releasedSpends) THEN releasedSpends ELSE '[]' END) UNION SELECT 'pot_records',txid,outputIndex FROM pot_records INDEXED BY idx_pot_spending WHERE spendingTxid=?1{confirmed} UNION SELECT json_extract(value,'$.table'),lower(json_extract(value,'$.txid')),json_extract(value,'$.vout') FROM json_each(?2)) r) WHERE txid=?1 AND readmittedAt IS NULL")
}

/// bsv-low #911: the pairs one committed [`plan`] marked: the eviction's own (it names every party, shadow party and
/// released hop it moves) and each released hop's evidence row. Noted AFTER the plan's batch answered (the
/// D2-ACTOR-A lens's LOW-8: a note made at plan time could be published by a concurrent request before its batch
/// landed, reading no mark).
pub(crate) fn plan_pairs(snapshot: &Snapshot, txid: &str) -> Vec<(&'static str, String)> {
    use overlay_discovery::pot::released_evidence::{evidence_writes, EvidenceWrite};
    let released: Vec<(String, u32)> = snapshot
        .released
        .iter()
        .filter(|r| r.table == "pot_records")
        .map(|r| (r.txid.to_ascii_lowercase(), r.vout))
        .collect();
    let mut out = vec![("eviction", txid.to_string())];
    for w in evidence_writes(snapshot.beef.as_deref(), txid, &released, &snapshot.seat_keys) {
        if let EvidenceWrite::Found(f) = w {
            out.push((
                "hop",
                overlay_discovery::owed_event::outpoint_key(&f.hop_txid, f.hop_vout),
            ));
        }
    }
    out
}

pub(crate) fn plan(snapshot: &Snapshot, txid: &str, reason: &str, now_ms: u64) -> Vec<Query> {
    let mut out = vec![Query::new(OPEN_EVICTION_MARKER_SQL)
        .bind(txid)
        .bind(reason)
        .bind(now_ms)];
    out.push(Query::new(READMISSION_FENCE_SQL).bind(txid));
    for (table, keys) in MOVED_TABLES {
        let Some((_, cols)) = snapshot.tables.iter().find(|(name, _)| name == table) else {
            continue;
        };
        for key in *keys {
            if !cols.iter().any(|c| c.name == *key) {
                continue;
            }
            let (copy, delete) = move_sql(table, key, cols);
            out.push(Query::new(copy).bind(now_ms).bind(reason).bind(txid));
            out.push(Query::new(delete).bind(txid));
        }
    }
    if let Some((_, cols)) = snapshot.tables.iter().find(|(t, _)| *t == "pot_records") {
        // Merge in SQL at execution time, including a pointer that landed after the snapshot.
        out.push(
            Query::new(merge_releases_sql(cols))
                .bind(txid)
                .bind(serde_json::to_string(&snapshot.released).unwrap_or_else(|_| "[]".into())),
        );
        out.push(Query::new(release_pot_spends_sql(cols)).bind(txid));
    }
    out.push(
        Query::new(CLOSE_EVICTION_MARKER_SQL)
            .bind(txid)
            .bind(reason)
            .bind(now_ms)
            .bind(snapshot.moved)
            .bind("[]"), // keep the list this batch just merged
    );
    out.extend(evidence_inserts(snapshot, txid));
    out.push(Query::new(COUNT_SKIPPED_SQL).bind(txid));
    out
}

#[derive(serde::Deserialize)]
struct SeatKey {
    key: String,
}

#[derive(serde::Deserialize)]
struct Spend {
    txid: String,
    #[serde(rename = "outputIndex")]
    vout: u32,
    #[serde(rename = "spentConfirmed")]
    confirmed: i64,
}

#[derive(serde::Deserialize)]
struct Ledger {
    #[serde(rename = "releasedSpends")]
    released: Option<String>,
}

async fn snapshot(
    db: &D1Database,
    txid: &str,
    repointed: &[ReleasedSpend],
) -> Result<Snapshot, String> {
    let mut out = Snapshot::default();
    for (table, keys) in MOVED_TABLES {
        let cols = db.columns(table).await?;
        if cols.is_empty() {
            continue;
        }
        ensure_shadow(db, table, &cols).await?;
        let keys: Vec<_> = keys
            .iter()
            .filter(|key| cols.iter().any(|c| c.name == **key))
            .collect();
        if keys.is_empty() {
            continue;
        }
        let predicate = keys
            .iter()
            .map(|key| format!("\"{key}\"=?1"))
            .collect::<Vec<_>>()
            .join(" OR ");
        #[derive(serde::Deserialize)]
        struct Count {
            n: u64,
        }
        let count = Query::new(format!(
            "SELECT COUNT(*) AS n FROM \"{table}\" WHERE {predicate}"
        ))
        .bind(txid)
        .fetch_optional::<Count>(db)
        .await?
        .ok_or("count returned no row")?
        .n;
        out.moved += count;
        if count > 0 && *table == "pot_records" {
            out.pot_vouts = db.vouts(table, txid).await?;
        }
        if count > 0 && *table == "low_records" {
            out.advert_vouts = db.vouts(table, txid).await?;
        }
        out.tables.push((table, cols));
    }
    if let Some((_, cols)) = out.tables.iter().find(|(t, _)| *t == "pot_records") {
        for spend in Query::new(select_pot_spends_sql(cols))
            .bind(txid)
            .fetch_all::<Spend>(db)
            .await?
        {
            if spend.confirmed == 0 {
                out.fresh_spends = true;
                out.released.push(ReleasedSpend {
                    table: "pot_records".into(),
                    txid: spend.txid.to_ascii_lowercase(),
                    vout: spend.vout,
                });
            }
        }
    }
    out.released.extend(repointed.iter().cloned());
    let existing = Query::new(RELEASED_SPENDS_SQL)
        .bind(txid)
        .fetch_optional::<Ledger>(db)
        .await?;
    out.released = merge_released(
        existing.as_ref().and_then(|r| r.released.as_deref()),
        &out.released,
    );
    out.seat_keys = Query::new(SEAT_KEYS_SQL)
        .bind(txid)
        .fetch_all::<SeatKey>(db)
        .await?
        .into_iter()
        .map(|r| r.key)
        .collect();
    out.beef = db.stored_beef(txid).await?;
    Ok(out)
}

/// D1's two passes now each commit a complete move, release and evidence transaction.
/// OPEN still precedes the manager's snapshot; the second pass catches late admissions.
pub(crate) async fn evict(
    db: &D1Database,
    txid: &str,
    reason: &str,
    now_ms: u64,
    repointed: &[ReleasedSpend],
) -> (EvictionCore, Option<PendingAbort>, Vec<ReleasedSpend>) {
    let mut core = EvictionCore::default();
    let mut released = Vec::new();
    if let Err(e) = Query::new(OPEN_EVICTION_MARKER_SQL)
        .bind(txid)
        .bind(reason)
        .bind(now_ms)
        .execute(db)
        .await
    {
        core.faults.push(format!("open marker: {e}"));
        return (core, None, released);
    }
    match readmitted_after(db, txid, now_ms).await {
        Ok(Some(at)) => {
            core.yielded_to_readmission = Some(at);
            return (core, None, released);
        }
        Ok(None) => {}
        Err(e) => {
            core.faults.push(format!("readmission check: {e}"));
            return (core, None, released);
        }
    }
    let abort = crate::admission_abort::read_before_eviction(
        db,
        txid,
        crate::admission_abort::managers_that_abort(),
    )
    .await;
    let mut first_fault = None;
    let mut committed = false;
    let mut verify = Vec::new();
    for pass in 0..2 {
        let attempt = async {
            let snapshot = snapshot(db, txid, repointed).await?;
            if pass == 1 && first_fault.is_none() && snapshot.moved == 0 && !snapshot.fresh_spends {
                return Ok(None);
            }
            let queries = plan(&snapshot, txid, reason, now_ms);
            let statements = queries
                .iter()
                .map(|q| q.prepare(db))
                .collect::<Result<Vec<_>, _>>()?;
            crate::d1_ledger::counted_batch(db, statements)
                .await
                .map_err(|e| e.to_string())?;
            for (source, key) in plan_pairs(&snapshot, txid) {
                crate::owed_events::note(source, &key);
            }
            Ok::<_, String>(Some(snapshot))
        }
        .await;
        match attempt {
            Ok(s) => {
                if let Some(s) = s {
                    committed = true;
                    core.moved += s.moved;
                    core.pot_vouts.extend(s.pot_vouts);
                    core.advert_vouts.extend(s.advert_vouts);
                    released = s.released;
                    // The batch also moves rows and releases pointers that landed after
                    // the snapshot. Read its actual result back for the event's outpoints.
                    for (table, vouts) in [
                        ("pot_records_evicted", &mut core.pot_vouts),
                        ("low_records_evicted", &mut core.advert_vouts),
                    ] {
                        match db.vouts(table, txid).await {
                            Ok(actual) => vouts.extend(actual),
                            Err(e) => core.faults.push(format!("moved outpoints in {table}: {e}")),
                        }
                    }
                    match Query::new(RELEASED_SPENDS_SQL)
                        .bind(txid)
                        .fetch_optional::<Ledger>(db)
                        .await
                    {
                        Ok(Some(row)) => {
                            released = merge_released(row.released.as_deref(), &released)
                        }
                        Ok(None) => core
                            .faults
                            .push("committed eviction has no ledger row".into()),
                        Err(e) => core.faults.push(format!("committed release list: {e}")),
                    }
                    if pass == 1 {
                        verify = s.tables;
                    }
                }
                if let Some(e) = first_fault.take() {
                    core.retried.push(e);
                }
            }
            Err(e) => {
                match readmitted_after(db, txid, now_ms).await {
                    Ok(Some(at)) => {
                        core.yielded_to_readmission = Some(at);
                        break;
                    }
                    Ok(None) => {}
                    Err(f) => core
                        .faults
                        .push(format!("readmission after batch fault: {f}")),
                }
                if pass == 0 {
                    first_fault = Some(e);
                } else {
                    core.faults.push(e);
                }
            }
        }
    }
    for (table, cols) in verify {
        for key in MOVED_TABLES
            .iter()
            .find(|(name, _)| *name == table)
            .into_iter()
            .flat_map(|(_, keys)| *keys)
        {
            if !cols.iter().any(|c| c.name == *key) {
                continue;
            }
            match db.count(table, key, txid).await {
                Ok(0) => {}
                Ok(n) => core.faults.push(format!(
                    "{n} row(s) still in {table} by {key} after verification"
                )),
                Err(e) => core
                    .faults
                    .push(format!("verification of {table} by {key}: {e}")),
            }
        }
    }

    // A readmission can land immediately after either batch. Its stamp wins and the
    // idempotent restore covers a readmission that had already started restoring.
    match readmitted_after(db, txid, now_ms).await {
        Ok(Some(at)) => {
            core.yielded_to_readmission = Some(at);
            let (restored, faults) = restore_twins(db, txid).await;
            core.restored = restored;
            core.restore_faults = faults;
            core.faults.extend(core.restore_faults.iter().cloned());
        }
        Ok(None) => {}
        Err(e) => core.faults.push(format!("readmission after batches: {e}")),
    }
    (core, committed.then_some(abort), released)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admit_fast::{create_shadow_sql, shadow_table};
    use crate::d1::{QVal, OVERLAY_MIGRATIONS};
    use bsv_rs::{
        script::LockingScript,
        transaction::{Beef, Transaction, TransactionInput, TransactionOutput},
    };
    use rusqlite::{params, Connection};

    fn sql_value(v: &QVal) -> rusqlite::types::Value {
        match v {
            QVal::Null => rusqlite::types::Value::Null,
            QVal::Int(v) => (*v).into(),
            QVal::Text(v) => v.clone().into(),
            QVal::Bool(v) => i64::from(*v).into(),
            QVal::Blob(v) => v.clone().into(),
            QVal::Float(v) => (*v).into(),
        }
    }

    fn execute(db: &Connection, queries: &[Query]) -> rusqlite::Result<()> {
        let transaction = db.unchecked_transaction()?;
        for q in queries {
            transaction.execute(
                q.sql(),
                rusqlite::params_from_iter(q.params().iter().map(sql_value)),
            )?;
        }
        transaction.commit()
    }

    fn columns(db: &Connection, table: &str) -> Vec<ColumnInfo> {
        db.prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .unwrap()
            .query_map([], |r| {
                Ok(ColumnInfo {
                    name: r.get(1)?,
                    ty: r.get(2)?,
                    notnull: r.get(3)?,
                    dflt_value: r.get(4)?,
                })
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    struct Fixture {
        db: Connection,
        snapshot: Snapshot,
        join: String,
        hops: Vec<String>,
        keys: Vec<String>,
    }

    fn fixture() -> Fixture {
        let db = Connection::open_in_memory().unwrap();
        for sql in OVERLAY_MIGRATIONS {
            if let Err(e) = db.execute_batch(sql) {
                assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
            }
        }
        let keys = vec![
            format!("02{}", "aa".repeat(32)),
            format!("03{}", "bb".repeat(32)),
        ];
        let mut beef = Beef::new();
        let mut hops = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            let mut tx = Transaction::new();
            tx.inputs
                .push(TransactionInput::new(format!("{i:064x}"), 0));
            tx.outputs.push(TransactionOutput {
                satoshis: Some(20_190 + i as u64),
                locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(
                    &bsv_rs::primitives::hash::hash160(&hex::decode(key).unwrap()),
                ))
                .unwrap(),
                change: false,
            });
            hops.push(tx.id());
            beef.merge_raw_tx(tx.to_binary(), None);
        }
        let mut join_tx = Transaction::new();
        for hop in &hops {
            join_tx.inputs.push(TransactionInput::new(hop.clone(), 0));
        }
        join_tx.outputs.push(TransactionOutput {
            satoshis: Some(40_000),
            locking_script: LockingScript::from_hex("51").unwrap(),
            change: false,
        });
        let join = join_tx.id();
        beef.merge_raw_tx(join_tx.to_binary(), None);
        db.execute("INSERT INTO pot_records (txid,outputIndex,spent,createdAt,pubA,pubB,payPkhA,payPkhB) VALUES (?1,0,0,1,?2,?3,?4,?5)", params![join,keys[0],keys[1],"dd".repeat(20),"ee".repeat(20)]).unwrap();
        for (i, hop) in hops.iter().enumerate() {
            db.execute("INSERT INTO pot_records (txid,outputIndex,spent,spendingTxid,spentConfirmed,createdAt) VALUES (?1,0,1,?2,0,1)", params![hop,join]).unwrap();
            db.execute("INSERT INTO potparty_records (txid,outputIndex,identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,seatSettlePubkey,sigValid,createdAt) VALUES (?1,0,?2,'opponent',?3,?4,0,99,?5,1,1)", params![format!("{i:064x}"),format!("identity-{i}"),"game",join,keys[i]]).unwrap();
        }
        let mut snapshot = Snapshot {
            beef: Some(beef.to_binary()),
            seat_keys: keys.clone(),
            moved: 3,
            ..Snapshot::default()
        };
        for (table, _) in MOVED_TABLES {
            let cols = columns(&db, table);
            if !cols.is_empty() {
                db.execute_batch(&create_shadow_sql(table, &cols)).unwrap();
                snapshot.tables.push((table, cols));
            }
        }
        snapshot.released = hops
            .iter()
            .map(|hop| ReleasedSpend {
                table: "pot_records".into(),
                txid: hop.clone(),
                vout: 0,
            })
            .collect();
        Fixture {
            db,
            snapshot,
            join,
            hops,
            keys,
        }
    }

    fn count(db: &Connection, table: &str) -> i64 {
        db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn d2_owed_fix518_two_released_hops_have_their_own_keys_and_sats() {
        let f = fixture();
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        let rows: Vec<(String,String,String,i64)> = f.db.prepare("SELECT hopTxid,identity,seatSettlePubkey,hopSats FROM released_hop_evidence ORDER BY identity").unwrap()
            .query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap().collect::<Result<_,_>>().unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    f.hops[0].clone(),
                    "identity-0".into(),
                    f.keys[0].clone(),
                    20_190
                ),
                (
                    f.hops[1].clone(),
                    "identity-1".into(),
                    f.keys[1].clone(),
                    20_191
                )
            ]
        );
    }

    #[test]
    fn d2_owed_unverified_party_has_no_evidence_and_is_counted() {
        let f = fixture();
        f.db.execute("UPDATE potparty_records SET sigValid=0", [])
            .unwrap();
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        assert_eq!(count(&f.db, "released_hop_evidence"), 0);
        let skipped: i64 =
            f.db.query_row(
                "SELECT value FROM ops_counters WHERE name='released_hop_evidence_skipped_total'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(skipped, 2);
    }

    #[test]
    fn d2_owed_readmission_supersedes_without_deleting_evidence() {
        let f = fixture();
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        assert_eq!(count(&f.db, "released_hop_evidence"), 2);
        execute(
            &f.db,
            &[Query::new(crate::admit_fast::READMIT_EVICTION_SQL)
                .bind(2_000u64)
                .bind(f.join.as_str())],
        )
        .unwrap();
        assert_eq!(count(&f.db, "released_hop_evidence"), 2);
        assert_eq!(active(&f), 0);
        // Opening a later eviction cannot make the earlier evidence active again.
        execute(
            &f.db,
            &[Query::new(OPEN_EVICTION_MARKER_SQL)
                .bind(f.join.as_str())
                .bind("REJECTED")
                .bind(3_000u64)],
        )
        .unwrap();
        assert_eq!(active(&f), 0);
        let readmit = include_str!("admit_fast.rs");
        let readmit = &readmit[readmit.find("pub async fn readmit_if_evicted(").unwrap()..];
        assert!(
            readmit[..readmit.find("\n}\n").unwrap()].contains("Query::new(READMIT_EVICTION_SQL)")
        );
    }

    #[test]
    fn d2_owed_faulted_evidence_insert_leaves_no_shadow_move_or_release() {
        let f = fixture();
        let dirty = count(&f.db, "owed_dirty");
        f.db.execute_batch("CREATE TRIGGER fault_evidence BEFORE INSERT ON released_hop_evidence BEGIN SELECT RAISE(ABORT,'fault evidence'); END").unwrap();
        assert!(execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).is_err());
        assert_eq!(count(&f.db, "pot_records"), 3);
        assert_eq!(count(&f.db, "potparty_records"), 2);
        assert_eq!(count(&f.db, &shadow_table("pot_records")), 0);
        assert_eq!(count(&f.db, &shadow_table("potparty_records")), 0);
        assert_eq!(count(&f.db, "pot_evictions"), 0);
        assert_eq!(
            count(&f.db, "owed_dirty"),
            dirty,
            "dirty triggers roll back too"
        );
        let spent: i64 =
            f.db.query_row(
                "SELECT COUNT(*) FROM pot_records WHERE spendingTxid=?1",
                [&f.join],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(spent, 2);
    }

    #[test]
    fn d2_owed_eviction_move_statements_keep_their_order_and_text() {
        assert_eq!(
            MOVED_TABLES,
            &[
                ("pot_records", &["txid"][..]),
                ("outputs", &["txid"][..]),
                ("applied_transactions", &["txid"][..]),
                ("transactions", &["txid"][..]),
                ("low_records", &["txid"][..]),
                ("potparty_records", &["txid", "potTxid"][..]),
                ("potrefund_records", &["txid", "potTxid"][..]),
                ("result_markers", &["txid", "potTxid"][..]),
                ("result_markers_v2", &["txid", "potTxid"][..]),
                ("hand_markers", &["txid", "potTxid"][..]),
                ("lb_marker_rows", &["txid", "potTxid"][..]),
            ]
        );
        let f = fixture();
        let queries = plan(&f.snapshot, &f.join, "REJECTED", 1_000);
        let actual: Vec<_> = queries
            .iter()
            .filter(|q| {
                q.sql().starts_with("INSERT INTO \"") || q.sql().starts_with("DELETE FROM \"")
            })
            .map(Query::sql)
            .collect();
        let mut expected = Vec::new();
        for (table, keys) in MOVED_TABLES {
            let cols = columns(&f.db, table);
            for key in *keys {
                if cols.iter().any(|c| c.name == *key) {
                    let (copy, delete) = move_sql(table, key, &cols);
                    expected.extend([copy, delete]);
                }
            }
        }
        assert_eq!(actual, expected);
    }

    fn active(f: &Fixture) -> usize {
        f.db.prepare(RELEASED_GAME_READ_SQL)
            .unwrap()
            .query_map(["identity-0", "game"], |_| Ok(()))
            .unwrap()
            .count()
    }

    #[test]
    fn d2_owed_bad_commitment_and_ambiguous_party_cannot_authorize_a_hop() {
        for sql in [
            "UPDATE pot_records SET pubB=pubA WHERE pubA IS NOT NULL",
            "UPDATE pot_records SET pubA='garbage',pubB='garbage' WHERE pubA IS NOT NULL",
            "INSERT INTO pot_records (txid,outputIndex,spent,createdAt,pubA,pubB,payPkhA,payPkhB) SELECT txid,1,spent,createdAt,pubA,pubB,payPkhB,payPkhA FROM pot_records WHERE pubA IS NOT NULL",
            "INSERT INTO potparty_records (txid,outputIndex,identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,seatSettlePubkey,sigValid) SELECT txid,1,identity,opponentIdentity,'other-game',potTxid,potVout,recoveryHeight,seatSettlePubkey,1 FROM potparty_records",
        ] {
            let f=fixture();
            f.db.execute(sql,[]).unwrap();
            execute(&f.db,&plan(&f.snapshot,&f.join,"REJECTED",1_000)).unwrap();
            assert_eq!(count(&f.db,"released_hop_evidence"),0,"{sql}");
        }
    }

    #[test]
    fn d2_owed_stored_bytes_must_name_the_exact_p2pkh_outpoint() {
        for bad in 0..3 {
            let mut f = fixture();
            match bad {
                0 => f.snapshot.beef = None,
                1 => f.snapshot.beef = Some(vec![0, 1, 2]),
                _ => {
                    for hop in &mut f.snapshot.released {
                        hop.vout = 1;
                    }
                    f.db.execute(
                        "UPDATE pot_records SET outputIndex=1 WHERE spendingTxid=?1",
                        [&f.join],
                    )
                    .unwrap();
                }
            }
            execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
            assert_eq!(count(&f.db, "released_hop_evidence"), 0);
        }
    }

    /// D2-OWED-FOLD-8: the bytes that cannot judge a released hop are never silence. An absent or undecodable JOIN
    /// names every released hop per verified seat (`bytesRead` 0); bytes that parse but do not carry the hop's output
    /// name that hop (`bytesRead` 1); readable bytes name none. Each is the judge's could-not-judge row.
    #[test]
    fn fold8_an_unreadable_join_names_its_released_hops_for_each_verified_seat() {
        for bad in 0..4 {
            let mut f = fixture();
            match bad {
                0 => f.snapshot.beef = None,
                1 => f.snapshot.beef = Some(vec![0, 1, 2]),
                2 => {
                    for hop in &mut f.snapshot.released {
                        hop.vout = 1;
                    }
                    f.db.execute(
                        "UPDATE pot_records SET outputIndex=1 WHERE spendingTxid=?1",
                        [&f.join],
                    )
                    .unwrap();
                }
                _ => {}
            }
            execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
            let rows: Vec<(String, String, i64, i64)> = f
                .db
                .prepare("SELECT identity,hopTxid,hopVout,bytesRead FROM released_hop_evidence_missing ORDER BY identity,hopTxid")
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            if bad == 3 {
                assert!(rows.is_empty(), "{rows:?}");
                assert_eq!(count(&f.db, "released_hop_evidence"), 2);
                continue;
            }
            let mut hops = f.hops.clone();
            hops.sort();
            let vout = i64::from(bad == 2);
            let want: Vec<_> = ["identity-0", "identity-1"]
                .iter()
                .flat_map(|id| {
                    hops.iter()
                        .map(move |h| (id.to_string(), h.clone(), vout, vout))
                })
                .collect();
            assert_eq!(rows, want, "case {bad}");
            assert_eq!(count(&f.db, "released_hop_evidence"), 0);
            // each named hop marks its seat's game (the dirty trigger, keyed per hop)
            assert_eq!(
                f.db.query_row(
                    "SELECT COUNT(*) FROM owed_dirty WHERE source='hop' AND gameId='game'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                4,
                "each seat's game is marked for each hop"
            );
        }
    }

    /// The seat of hop 0 has NO party row (its filing never landed: beta's courier-retire hand 3, game 39cf...);
    /// its VERIFIED hop marker names the hop, the identity, the game and K. `game` is the marker's game.
    fn home_marker_fixture(marker_valid: i64, marker_key: usize, game: &str) -> Fixture {
        let f = fixture();
        f.db.execute(
            "DELETE FROM potparty_records WHERE identity='identity-0'",
            [],
        )
        .unwrap();
        f.db.execute("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,markerValid,txid,outputIndex,createdAt) VALUES ('identity-0','opponent',?1,0,20190,?2,'00','00',2,?3,?4,1,1)", params![game, f.keys[marker_key], marker_valid, f.hops[0]]).unwrap();
        f
    }

    fn evidence(db: &Connection) -> Vec<(String, String, String, String, i64)> {
        db.prepare("SELECT hopTxid,identity,gameId,seatSettlePubkey,hopSats FROM released_hop_evidence ORDER BY identity")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn skipped(db: &Connection) -> i64 {
        db.query_row(
            "SELECT IFNULL((SELECT value FROM ops_counters WHERE name='released_hop_evidence_skipped_total'),0)",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// D2-OWED-HOME-MARKER (a): the identity's signed attestation binding a hop is ANY ONE of its verified party row
    /// or its VERIFIED hop marker (register row D1's #610). A seat with a verified marker and no party row gets its
    /// evidence row from the marker's K and game, through the same checks 2 and 3 (the twin committing K on exactly
    /// one seat, the JOIN's bytes paying hash160(K) at the hop). An unverified marker, or one naming a key the hop
    /// does not pay, writes nothing.
    #[test]
    fn home_marker_a_a_verified_marker_without_a_party_row_writes_the_evidence() {
        let f = home_marker_fixture(1, 0, "game-m");
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        assert_eq!(
            evidence(&f.db),
            vec![
                (
                    f.hops[0].clone(),
                    "identity-0".into(),
                    "game-m".into(),
                    f.keys[0].clone(),
                    20_190
                ),
                (
                    f.hops[1].clone(),
                    "identity-1".into(),
                    "game".into(),
                    f.keys[1].clone(),
                    20_191
                ),
            ],
            "the marker's K binds hop 0 as the party row would"
        );
        assert_eq!(skipped(&f.db), 0);
        for (valid, key) in [(0, 0), (1, 1)] {
            let f = home_marker_fixture(valid, key, "game-m");
            execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
            let rows = evidence(&f.db);
            assert_eq!(rows.len(), 1, "markerValid={valid} key={key}: {rows:?}");
            assert_eq!(rows[0].1, "identity-1");
            assert_eq!(skipped(&f.db), 1);
        }
    }

    /// D2-OWED-HOME-MARKER (b): a marker-bound seat whose JOIN's bytes cannot judge the hop is never silence: the
    /// missing row names it (keyed by the marker's identity and game), and the hop is counted unwritten. A hop with
    /// NEITHER a party row nor a verified marker has no identity to key a row by: it is counted, and nothing names it.
    #[test]
    fn home_marker_b_unreadable_bytes_name_the_marker_seat_and_neither_is_counted() {
        let mut f = home_marker_fixture(1, 0, "game-m");
        f.snapshot.beef = None;
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        let rows: Vec<(String, String, String)> = f
            .db
            .prepare("SELECT identity,gameId,hopTxid FROM released_hop_evidence_missing ORDER BY identity,hopTxid")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let mut hops = f.hops.clone();
        hops.sort();
        let mut want = vec![(
            "identity-0".to_string(),
            "game-m".to_string(),
            f.hops[0].clone(),
        )];
        want.extend(
            hops.iter()
                .map(|h| ("identity-1".to_string(), "game".to_string(), h.clone())),
        );
        assert_eq!(
            rows, want,
            "the marker's seat names its own hop, the party seat every released hop"
        );
        assert_eq!(skipped(&f.db), 2);

        let f = home_marker_fixture(0, 0, "game-m");
        f.db.execute("DELETE FROM potparty_records", []).unwrap();
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        assert_eq!(count(&f.db, "released_hop_evidence"), 0);
        assert_eq!(count(&f.db, "released_hop_evidence_missing"), 0);
        assert_eq!(skipped(&f.db), 2, "neither: counted, no identity to name");
    }

    #[test]
    fn d2_owed_a_newer_readmission_between_snapshot_and_batch_keeps_the_rows_live() {
        let f = fixture();
        let queries = plan(&f.snapshot, &f.join, "REJECTED", 1_000);
        f.db.execute("INSERT INTO pot_evictions(txid,reason,evictedAt,readmittedAt) VALUES (?1,'REJECTED',500,2000)",[&f.join]).unwrap();
        assert!(execute(&f.db, &queries).is_err());
        assert_eq!(count(&f.db, "pot_records"), 3);
        assert_eq!(count(&f.db, "potparty_records"), 2);
        assert_eq!(count(&f.db, "pot_records_evicted"), 0);
        assert_eq!(count(&f.db, "released_hop_evidence"), 0);
        let stamp: i64 =
            f.db.query_row(
                "SELECT readmittedAt FROM pot_evictions WHERE txid=?1",
                [&f.join],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stamp, 2000);
    }

    #[test]
    fn d2_owed_a_pointer_arriving_after_snapshot_gets_its_evidence_in_the_same_batch() {
        let mut f = fixture();
        // The JOIN bytes already commit both inputs. Their pointers can land after the read.
        f.snapshot.released.clear();
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        assert_eq!(count(&f.db, "released_hop_evidence"), 2);
        let released: String =
            f.db.query_row(
                "SELECT releasedSpends FROM pot_evictions WHERE txid=?1",
                [&f.join],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Vec<ReleasedSpend>>(&released)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn d2_owed_seat_and_judge_reads_use_the_twin_pot_and_evidence_indexes() {
        let f = fixture();
        let plan_for = |sql: &str, binds: &[&str]| {
            f.db.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(rusqlite::params_from_iter(binds), |r| r.get::<_, String>(3))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .join("\n")
        };
        let seats = plan_for(SEAT_KEYS_SQL, &[&f.join]);
        assert!(
            seats.contains("SEARCH potparty_records_evicted USING INDEX idx_evicted_party_pot"),
            "{seats}"
        );
        let judge = plan_for(RELEASED_GAME_READ_SQL, &["identity-0", "game"]);
        assert!(
            judge.contains("SEARCH h USING INDEX idx_released_hop_evidence_identity_game"),
            "{judge}"
        );
    }

    #[test]
    fn d2_owed_production_eviction_executes_the_tested_plan_as_one_counted_batch() {
        let source = include_str!("admit_fast.rs");
        let wrapper = &source[source
            .find("pub async fn evict_txid_everywhere_repointed(")
            .unwrap()..];
        let wrapper = &wrapper[..wrapper.find("\n}\n").unwrap()];
        assert!(
            wrapper.contains("crate::eviction_write::evict(db, &txid, reason, now_ms, repointed)")
        );
        assert!(!wrapper.contains("CLOSE_EVICTION_MARKER_SQL"));
        let source = include_str!("eviction_write.rs");
        let writer = &source[source.find("pub(crate) async fn evict(").unwrap()..];
        let writer = &writer[..writer.find("\n}\n").unwrap()];
        assert!(writer.contains("let queries = plan(&snapshot, txid, reason, now_ms)"));
        assert!(writer.contains("crate::d1_ledger::counted_batch(db, statements)"));
    }
}
