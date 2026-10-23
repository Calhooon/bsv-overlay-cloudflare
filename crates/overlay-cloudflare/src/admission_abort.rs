//! The managers' abort at the eviction (bsv-low #558).
//!
//! The reference tells a topic manager `abortAdmissibleOutputs(beef,
//! outputsToAbort)` when a `current-tx` broadcast throws after the manager
//! identified its outputs (`Engine.ts:1219-1223`, `abortProvisionalAdmissions`
//! at `Engine.ts:685-700`, ts-stack `f999e0c1a`): the manager releases the
//! provisional state of an admission the network did not take. This door
//! (register row D1) broadcasts BEFORE it admits and admits on the network's
//! evidence; the network's refusal of a transaction it already wrote is
//! learned AFTER the write, and every such refusal ends in ONE place,
//! [`crate::admit_fast::evict_txid_everywhere_repointed`] (the evidence check,
//! the refusal callback and push, the pending watch, the retire pass, the
//! conflict pass). That is where the managers are told: once per evicted
//! admission, for the outputs the index held live under each topic, and never
//! when the pass yielded to a readmission. A refusal BEFORE the write (the
//! gated arm's broadcast refused) asked no manager a real admission (the door
//! walks scripts only, `Engine::verify_scripts_only`), so there is nothing to
//! abort.
//!
//! What it costs: nothing while no registered manager answers
//! `TopicManager::aborts_admissions` (all sixteen of this workspace's today,
//! pinned by `i558_every_workspace_manager_keeps_the_default`): no read is
//! made. Otherwise two reads an eviction: the txid's live `outputs` rows, and
//! its stored BEEF when one of them is under an aborting manager.
//!
//! Stated: a READMISSION restores the rows without asking the manager again
//! (the reference has no readmission), so a stateful manager told of an
//! eviction that is later reversed is not told the admission stands.

use async_trait::async_trait;
use overlay_engine::topic_manager::TopicManager;
use worker::D1Database;

use crate::d1::Query;

/// The live `outputs` rows of a txid, by topic and index.
pub const ADMITTED_OUTPUTS_SQL: &str =
    "SELECT topic, outputIndex FROM outputs WHERE txid = ? ORDER BY topic, outputIndex";

/// The stored BEEF of a txid, read back as hex ([`crate::d1::beef_of_hex_column`]).
pub const STORED_BEEF_SQL: &str = "SELECT hex(beef) AS beef FROM transactions WHERE txid = ?";

/// What the abort reads before the eviction moves the rows.
#[async_trait(?Send)]
pub trait AbortRows {
    /// The txid's live `outputs` rows as `(topic, outputIndex)`.
    async fn admitted_outputs(&self, txid: &str) -> Result<Vec<(String, u32)>, String>;
    /// The txid's stored BEEF, if any.
    async fn stored_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, String>;
}

#[derive(serde::Deserialize)]
struct AdmittedRow {
    topic: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
}

#[derive(serde::Deserialize)]
struct BeefRow {
    beef: Option<String>,
}

#[async_trait(?Send)]
impl AbortRows for D1Database {
    async fn admitted_outputs(&self, txid: &str) -> Result<Vec<(String, u32)>, String> {
        Ok(Query::new(ADMITTED_OUTPUTS_SQL)
            .bind(txid)
            .fetch_all::<AdmittedRow>(self)
            .await?
            .into_iter()
            .map(|r| (r.topic, r.output_index as u32))
            .collect())
    }
    async fn stored_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(Query::new(STORED_BEEF_SQL)
            .bind(txid)
            .fetch_optional::<BeefRow>(self)
            .await?
            .and_then(|r| crate::d1::beef_of_hex_column(r.beef)))
    }
}

/// The registered managers that implement the abort, by topic.
pub type AbortingManagers = Vec<(String, Box<dyn TopicManager>)>;

/// Every workspace manager whose `aborts_admissions` is true
/// ([`crate::WORKSPACE_TOPIC_MANAGERS`]). Empty today.
pub fn managers_that_abort() -> AbortingManagers {
    crate::WORKSPACE_TOPIC_MANAGERS
        .iter()
        .filter_map(|name| crate::topic_manager_named(name).map(|m| (name.to_string(), m)))
        .filter(|(_, m)| m.aborts_admissions())
        .collect()
}

/// The abort of one eviction, read BEFORE the eviction moves the rows.
pub struct PendingAbort {
    managers: AbortingManagers,
    beef: Vec<u8>,
    admissions: Vec<(String, Vec<u32>)>,
}

impl PendingAbort {
    /// The `(topic, outputs)` the managers will be told.
    pub fn admissions(&self) -> &[(String, Vec<u32>)] {
        &self.admissions
    }
}

/// Read what the abort of `txid` needs before its eviction: the live rows of
/// each topic an aborting manager holds, and the stored BEEF. No manager that
/// aborts: no read. A read that faults is logged and aborts nothing (the
/// eviction goes on: the abort never stands in its way).
pub async fn read_before_eviction(
    rows: &dyn AbortRows,
    txid: &str,
    managers: AbortingManagers,
) -> PendingAbort {
    let mut pending = PendingAbort {
        managers,
        beef: Vec::new(),
        admissions: Vec::new(),
    };
    if pending.managers.is_empty() {
        return pending;
    }
    let admitted = match rows.admitted_outputs(txid).await {
        Ok(admitted) => admitted,
        Err(e) => {
            crate::proof_fetcher::push_log(&format!(
                "[admit-fast] evict {txid}: the managers' abort could not read the admitted outputs ({e}): none told (bsv-low #558)"
            ));
            return pending;
        }
    };
    for (topic, vout) in admitted {
        if !pending.managers.iter().any(|(t, _)| *t == topic) {
            continue;
        }
        match pending.admissions.iter_mut().find(|(t, _)| *t == topic) {
            Some((_, outputs)) => outputs.push(vout),
            None => pending.admissions.push((topic, vec![vout])),
        }
    }
    if pending.admissions.is_empty() {
        return pending;
    }
    match rows.stored_beef(txid).await {
        Ok(beef) => pending.beef = beef.unwrap_or_default(),
        Err(e) => crate::proof_fetcher::push_log(&format!(
            "[admit-fast] evict {txid}: the managers' abort could not read the stored BEEF ({e}): told with none (bsv-low #558)"
        )),
    }
    pending
}

/// Tell each aborting manager its evicted outputs (the engine's
/// `abort_provisional_admissions`, one spelling). Answers how many were told.
pub async fn tell(pending: &PendingAbort) -> u32 {
    overlay_engine::topic_manager::abort_provisional_admissions(
        |topic| {
            pending
                .managers
                .iter()
                .find(|(t, _)| t == topic)
                .map(|(_, m)| m.as_ref())
        },
        &pending.beef,
        &pending.admissions,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every manager this worker registers keeps the trait's DEFAULT abort:
    /// it answers `aborts_admissions` false and its abort is a no-op `Ok`,
    /// so the eviction reads nothing and tells nobody. The list is the
    /// factory's own (sixteen, as the D13 audit and `fix791c_e` count). A
    /// manager that implements the abort turns this RED, and the eviction
    /// then tells it (`i558_the_eviction_tells_an_aborting_manager_once`).
    #[tokio::test]
    async fn i558_every_workspace_manager_keeps_the_default() {
        assert_eq!(crate::WORKSPACE_TOPIC_MANAGERS.len(), 16);
        for name in crate::WORKSPACE_TOPIC_MANAGERS {
            let manager = crate::topic_manager_named(name).expect(name);
            assert!(!manager.aborts_admissions(), "{name}");
            assert!(
                manager.abort_admissible_outputs(&[], &[0]).await.is_ok(),
                "{name}"
            );
        }
        assert!(crate::topic_manager_named("tm_unknown").is_none());
        assert!(managers_that_abort().is_empty());
    }
}
