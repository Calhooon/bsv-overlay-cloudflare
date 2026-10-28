//! Writer-side drains. Source triggers own the marks; every failed subject stays pending.
use super::*;

/// The partial gate index is the sole fleet entry point. No facts JSON is read. The window is the design's (section
/// 2.1, the tip row): `gateHeight BETWEEN tip - 6 AND tip`, so a row past its gate long ago is never re-marked at
/// every block (the D2-OWED lens's HIGH-2); a gate the tip skipped is judged by its own source write or the age repair.
pub const MARK_TIP_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'tip','gate',strftime('%s','now') * 1000 FROM owed_rows INDEXED BY idx_owed_rows_gate WHERE gateHeight BETWEEN ?1-6 AND ?1 GROUP BY identity,gameId ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
/// The design's tip row for a claimable sweep (the D2-OWED lens's HIGH-1), narrowed by the delta lens's D-H1 (the
/// captain's ruling): only a sweep whose press rests on a COURIER probe alone (the hop's index status, the stored
/// fact `status`, is not `unspent`) is due at the belt NOW when judged below the new tip; the read degrades it until
/// its look (`Snapshot::body`). An index-UNSPENT sweep is pressable on the index's own word (main's #669b and #808
/// (b) rule), so a block neither withholds it nor dates it: it keeps the belt's own back-off cadence and its look is
/// a downgrade only. Re-dating every claimable sweep at every block let K strangers' own sweeps fill the belt's 8
/// looks a tick fleet-wide. A judgement with no tip stores no claimable sweep (TIP-877's end-of-derive degrade), and
/// an old worker's row has no `claimable` column, so neither is in the partial index; the read withholds both. Cost:
/// the partial index's range below the tip, which since D2-OWED-FOLD-3 holds only the rows this statement writes (its
/// WHERE carries the status term, textually this statement's), so the reads are the courier-dependent sweeps and no
/// index-unspent sweep a stranger plants (the stated ceiling, `owed-tip-mark`).
pub const DATE_SWEEPS_SQL: &str = "UPDATE owed_rows INDEXED BY idx_owed_rows_sweep SET beltDueAtMs=?2 WHERE family='hop-stranded' AND claimable=1 AND probeTip<?1 AND (beltDueAtMs IS NULL OR beltDueAtMs>?2) AND json_extract(facts,'$.status') IS NOT 'unspent'";
/// A row judged with no chain tip (TIP-877: its tip-dependent claims withheld, `tipUnknown`) is re-judged on the next
/// tip; bounded per tip, the rest left to the next tip and the age repair.
pub const MARK_UNTIPPED_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'tip','untipped',strftime('%s','now') * 1000 FROM (SELECT DISTINCT identity,gameId FROM owed_rows INDEXED BY idx_owed_rows_untipped WHERE probeTip IS NULL LIMIT ?1) WHERE 1 ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
/// The tipless rows one tip re-marks at most.
pub const UNTIPPED_MARKS_PER_TIP: usize = 128;

pub async fn mark_tip(db: &impl JudgeDb, height: u64) -> Result<DrainReport, String> {
    write_tip_marks(db, height).await?;
    drain_changed(db, &[], &["gate".into(), "untipped".into()], "tip").await
}

/// bsv-low #911 (D2-ACTOR-A): the tip's marks and sweep dates alone; the `tip` events' consumer judges them.
pub async fn write_tip_marks(db: &impl SqlPort, height: u64) -> Result<(), String> {
    let now = db.now_ms() as i64;
    db.run_batch(vec![
        Stmt {
            sql: MARK_TIP_SQL.into(),
            binds: vec![Bind::Int(height as i64)],
        },
        Stmt {
            sql: DATE_SWEEPS_SQL.into(),
            binds: vec![Bind::Int(height as i64), Bind::Int(now)],
        },
        Stmt {
            sql: MARK_UNTIPPED_SQL.into(),
            binds: vec![Bind::Int(UNTIPPED_MARKS_PER_TIP as i64)],
        },
    ])
    .await?;
    Ok(())
}

pub async fn drain_identities(
    db: &impl JudgeDb,
    identities: &[String],
    source: &str,
) -> Result<DrainReport, String> {
    drain_changed(db, identities, &[], source).await
}

/// Scope a hook to the identities or source keys carried by its change note.
pub async fn drain_changed(
    db: &impl JudgeDb,
    identities: &[String],
    keys: &[String],
    source: &str,
) -> Result<DrainReport, String> {
    drain_changed_within(
        db,
        identities,
        keys,
        source,
        OWED_JUDGE_SUBJECTS_PER_INVOCATION,
    )
    .await
}

/// [`drain_changed`] under a subject limit of its own (at most the hook's sixteen): the detached word judge's re-run
/// takes what its first run left of the invocation's sixteen (bsv-low #934, the READ-934 lens's LOW-1).
pub async fn drain_changed_within(
    db: &impl JudgeDb,
    identities: &[String],
    keys: &[String],
    source: &str,
    limit: usize,
) -> Result<DrainReport, String> {
    let limit = limit.min(OWED_JUDGE_SUBJECTS_PER_INVOCATION);
    // bsv-low #912 (the D2-ACTOR-B delta lens's NEW-H1): no D1-only judge under the actor's read (its acknowledgement
    // would remove marks the actor never applied); the marks stand for the consumer.
    if limit == 0 || db.actor_reads() {
        return Ok(DrainReport::default());
    }
    let candidates: Vec<Value> = hydrate::read(
        db,
        CHANGED_SQL,
        vec![
            Bind::Text(json!(identities).to_string()),
            Bind::Text(json!(keys).to_string()),
            Bind::Int(limit as i64),
        ],
    )
    .await?;
    let mut report = DrainReport::default();
    for row in candidates {
        let subject = Subject {
            identity: row["identity"]
                .as_str()
                .ok_or("dirty identity missing")?
                .into(),
            game_id: row["gameId"].as_str().ok_or("dirty game missing")?.into(),
        };
        match judge_subjects(
            db,
            &subject.identity,
            std::slice::from_ref(&subject.game_id),
            source,
        )
        .await
        {
            Ok(done) => report.subjects += done.subjects,
            Err(error) => report.faults.push((subject, error)),
        }
    }
    Ok(report)
}

/// bsv-low #934 (READ-934): the word's write alone, the tracker's UPSERT in its own batch. Its `tx_words` triggers write
/// the marks of every subject naming the word, so the write is durable and judgeable before any judge runs.
pub async fn write_word(
    db: &impl SqlPort,
    row: &crate::tracker_host::stored::Row,
) -> Result<(), String> {
    let binds = crate::tracker_host::stored::upsert_binds(row)
        .into_iter()
        .map(|value| match value {
            Value::String(s) => Bind::Text(s),
            Value::Number(n) => Bind::Int(n.as_f64().unwrap_or_default() as i64),
            _ => Bind::Null,
        })
        .collect();
    db.run_batch(vec![Stmt {
        sql: crate::tracker_host::stored::UPSERT.into(),
        binds,
    }])
    .await?;
    Ok(())
}

/// bsv-low #934 (READ-934): the judgement of a written word, its marked subjects by the word's txid (at most the hook's
/// sixteen), after the write and never inside the frame's answer.
pub async fn judge_written_word(db: &impl JudgeDb, txid: &str) -> Result<DrainReport, String> {
    judge_written_words(db, &[txid.to_string()], OWED_JUDGE_SUBJECTS_PER_INVOCATION).await
}

/// bsv-low #934 (the READ-934 lens's LOW-1): the judgement of several written words at once, their marked subjects
/// by the words' txids, at most `limit` (itself at most the hook's sixteen): the detached judge's re-run over the
/// frames that landed while it ran.
pub async fn judge_written_words(
    db: &impl JudgeDb,
    txids: &[String],
    limit: usize,
) -> Result<DrainReport, String> {
    drain_changed_within(db, &[], txids, "word", limit).await
}
