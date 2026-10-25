//! Writer-side drains. Source triggers own the marks; every failed subject stays pending.
use super::*;

/// The partial gate index is the sole fleet entry point. No facts JSON is read. The window is the design's (section
/// 2.1, the tip row): `gateHeight BETWEEN tip - 6 AND tip`, so a row past its gate long ago is never re-marked at
/// every block (the D2-OWED lens's HIGH-2); a gate the tip skipped is judged by its own source write or the age repair.
pub const MARK_TIP_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'tip','gate',?2 FROM owed_rows INDEXED BY idx_owed_rows_gate WHERE gateHeight BETWEEN ?1-6 AND ?1 GROUP BY identity,gameId ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
/// The design's tip row for a claimable sweep (the D2-OWED lens's HIGH-1): judged below the new tip, it is due at the
/// belt NOW, so its probe is re-taken first; the read degrades it until then (`Snapshot::body`). A judgement with no
/// tip stores no claimable sweep (TIP-877's end-of-derive degrade), and an old worker's row has no `claimable`
/// column, so neither is in the partial index; the read withholds both.
pub const DATE_SWEEPS_SQL: &str = "UPDATE owed_rows INDEXED BY idx_owed_rows_sweep SET beltDueAtMs=?2 WHERE family='hop-stranded' AND claimable=1 AND probeTip<?1 AND (beltDueAtMs IS NULL OR beltDueAtMs>?2)";
/// A row judged with no chain tip (TIP-877: its tip-dependent claims withheld, `tipUnknown`) is re-judged on the next
/// tip; bounded per tip, the rest left to the next tip and the age repair.
pub const MARK_UNTIPPED_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'tip','untipped',?1 FROM (SELECT DISTINCT identity,gameId FROM owed_rows INDEXED BY idx_owed_rows_untipped WHERE probeTip IS NULL LIMIT ?2) WHERE 1 ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
/// The tipless rows one tip re-marks at most.
pub const UNTIPPED_MARKS_PER_TIP: usize = 128;

pub async fn mark_tip(db: &impl JudgeDb, height: u64) -> Result<DrainReport, String> {
    let now = db.now_ms() as i64;
    db.run_batch(vec![
        Stmt {
            sql: MARK_TIP_SQL.into(),
            binds: vec![Bind::Int(height as i64), Bind::Int(now)],
        },
        Stmt {
            sql: DATE_SWEEPS_SQL.into(),
            binds: vec![Bind::Int(height as i64), Bind::Int(now)],
        },
        Stmt {
            sql: MARK_UNTIPPED_SQL.into(),
            binds: vec![Bind::Int(now), Bind::Int(UNTIPPED_MARKS_PER_TIP as i64)],
        },
    ])
    .await?;
    drain_changed(db, &[], &["gate".into(), "untipped".into()], "tip").await
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
    let candidates: Vec<Value> = hydrate::read(db,
        CHANGED_SQL,
        vec![Bind::Text(json!(identities).to_string()),Bind::Text(json!(keys).to_string()),Bind::Int(OWED_JUDGE_SUBJECTS_PER_INVOCATION as i64)]).await?;
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

/// The word lands first, so the trigger and the hydration see the same accepted UPSERT.
/// A refused downgrade therefore cannot be applied to the inputs by accident.
pub async fn judge_word_write(
    db: &impl JudgeDb,
    row: &crate::tracker_host::stored::Row,
) -> Result<DrainReport, String> {
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
    drain_changed(db, &[], std::slice::from_ref(&row.txid), "word").await
}
