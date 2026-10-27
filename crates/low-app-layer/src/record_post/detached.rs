//! The source transaction is already durable. All subject discovery and judgement is after the answer.
use super::{owed_write, VerifiedRecord};
use crate::owed_judge::{self, DrainReport, JudgeDb};

pub(crate) async fn judge(
    db: &impl JudgeDb,
    filings: &[VerifiedRecord],
    source: &str,
) -> Result<DrainReport, String> {
    let mut identities = Vec::new();
    for filing in filings {
        for subject in owed_write::subjects(db, filing).await? {
            if !identities.contains(&subject.identity) {
                identities.push(subject.identity);
            }
        }
    }
    // Reads only these identities' durable marks, at most the hook's sixteen subjects.
    // No owned-mark override: a newer generation always survives an older judgement.
    owed_judge::drain_identities(db, &identities, source).await
}

pub(crate) fn schedule(
    ctx: &worker::RouteContext<crate::auth::AuthState>,
    filings: Vec<VerifiedRecord>,
    source: &'static str,
) {
    let Some(wait) = &ctx.data.wait else {
        worker::console_warn!("[owed] record {source} judgement deferred: no wait_until context; marks left for the belt");
        return;
    };
    let env = ctx.env.clone();
    wait.wait_until(async move {
        let started = worker::Date::now().as_millis();
        let deadline = crate::routes::worker_deadline(
            crate::owed_belt::OWED_DRAIN_WALL_MS,
            crate::owed_belt::OWED_DRAIN_RESERVE_MS,
        );
        // The tip lookup belongs to the judge's wall too, never the request's.
        let work = async {
            let tip = crate::routes::chaintracks_present_height_env(&env, "owed")
                .await
                .ok();
            let db =
                owed_judge::D1Judge::new(&env, tip, crate::routes::written_off_before_ms_env(&env))
                    .map_err(|e| e.to_string())?;
            judge(&db, &filings, source).await
        };
        match deadline.run_reserving(work).await {
            Some(Ok(report)) => {
                for (_, error) in report.faults {
                    worker::console_warn!("[owed] record {source} judgement deferred: {error}");
                }
            }
            Some(Err(error)) => {
                worker::console_warn!("[owed] record {source} judgement deferred: {error}")
            }
            None => worker::console_warn!(
                "[owed] record {source} judgement deferred: wall reached; marks left for the belt"
            ),
        }
        let ms = worker::Date::now().as_millis().saturating_sub(started);
        worker::console_log!("[record] {source} detached judge;dur={ms}");
        // FIX-935-FOLD: the judge's queued pushes, outside its wall.
        crate::push_coalesce::flush_queued(env).await;
    });
}
