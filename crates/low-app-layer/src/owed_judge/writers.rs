//! Writer-side drains. Source triggers own the marks; every failed subject stays pending.
use super::*;

/// The partial gate index is the sole fleet entry point. No facts JSON is read.
pub const MARK_TIP_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'tip','gate',?2 FROM owed_rows INDEXED BY idx_owed_rows_gate WHERE gateHeight IS NOT NULL AND gateHeight<=?1 GROUP BY identity,gameId ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";

pub async fn mark_tip(db: &impl JudgeDb, height: u64) -> Result<DrainReport, String> {
    db.run_batch(vec![Stmt {
        sql: MARK_TIP_SQL.into(),
        binds: vec![Bind::Int(height as i64), Bind::Int(db.now_ms() as i64)],
    }])
    .await?;
    drain_changed(db, &[], &["gate".into()], "tip").await
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
        "SELECT identity,gameId FROM owed_dirty WHERE gameId<>'' AND (identity IN (SELECT value FROM json_each(?1)) OR sourceKey IN (SELECT value FROM json_each(?2))) GROUP BY identity,gameId ORDER BY MAX(source='wanted') DESC,MIN(markedAtMs),MIN(generation) LIMIT ?3",
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
