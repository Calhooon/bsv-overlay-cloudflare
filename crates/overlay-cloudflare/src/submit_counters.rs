//! Durable accounting for one HTTP submit, including its early answers.
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

macro_rules! reasons {
    ($($variant:ident => $slug:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) enum Reason { $($variant),+ }
        impl Reason {
            pub(crate) const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub(crate) fn slug(self) -> &'static str {
                match self { $(Self::$variant => $slug),+ }
            }
        }
    };
}

reasons! {
    Admit => "admit",
    AdmitFastEcho => "admit_fast_echo",
    AdmitQueued => "admit_queued",
    Evicted => "evicted",
    ScriptRefused => "script_refused",
    WalkCouldNotRun => "walk_could_not_run",
    TerminalAbsent => "terminal_judged_absent",
    TerminalPresent => "terminal_judged_present",
    TerminalRace => "terminal_judged_race",
    TerminalYoung => "terminal_judged_young",
    TerminalInconclusive => "terminal_judged_inconclusive",
    TerminalCapped => "terminal_judged_capped",
    TerminalNotFinal => "terminal_judged_not_final",
    CorroboratorUnconfirmed => "corroborating_broadcaster_could_not_confirm",
    BroadcastFault => "broadcast_fault",
    NetworkRejected => "network_rejected",
    Queued => "queued",
    QueueFault => "queue_fault",
    OperatorAuthRequired => "operator_auth_required",
    MissingTopics => "missing_topics",
    InvalidTopics => "invalid_topics",
    TooManyTopics => "too_many_topics",
    InvalidBeef => "invalid_beef",
    MinedClaimUnsanitized => "mined_claim_unsanitized",
    LedgerUnreadable => "ledger_unreadable",
    ReadmitFault => "readmit_fault",
    MutationRefused => "mutation_refused",
    NoOutputsAdmitted => "no_outputs_admitted",
    UnsupportedTopic => "unsupported_topic",
    InvalidProof => "invalid_proof",
    StorageFault => "storage_fault",
    EngineFault => "engine_fault",
    RequestFault => "request_fault",
    Unclassified => "unclassified",
}

impl Reason {
    pub(crate) fn engine(error: &overlay_engine::engine::EngineError) -> Self {
        use overlay_engine::engine::EngineError;
        match error {
            EngineError::ScriptVerificationFailed { .. } => Self::ScriptRefused,
            EngineError::WalkCouldNotRun(_) => Self::WalkCouldNotRun,
            EngineError::BeefParseError(_) => Self::InvalidBeef,
            EngineError::SpvError(_) => Self::InvalidProof,
            EngineError::UnsupportedTopic(_) => Self::UnsupportedTopic,
            EngineError::StorageError(_) => Self::StorageFault,
            EngineError::BroadcastError(_) => Self::BroadcastFault,
            _ => Self::EngineFault,
        }
    }

    pub(crate) fn terminal(judgement: crate::broadcaster::TerminalJudgement) -> Self {
        use crate::broadcaster::TerminalJudgement;
        match judgement {
            TerminalJudgement::Absent => Self::TerminalAbsent,
            TerminalJudgement::Present => Self::TerminalPresent,
            TerminalJudgement::Race => Self::TerminalRace,
            TerminalJudgement::Young => Self::TerminalYoung,
            TerminalJudgement::Inconclusive => Self::TerminalInconclusive,
            TerminalJudgement::Capped => Self::TerminalCapped,
            TerminalJudgement::NotFinal => Self::TerminalNotFinal,
        }
    }

    pub(crate) fn broadcast(error: &str) -> Self {
        // The broadcaster's fixed class, never a slug derived from its detail.
        if error.contains("but the corroborating broadcaster could not confirm it") {
            Self::CorroboratorUnconfirmed
        } else {
            Self::BroadcastFault
        }
    }
}

pub(crate) fn counter_names(status: u16, reason: Reason) -> [String; 2] {
    [
        format!("submit_answer_{status}_total"),
        format!("submit_answer_reason_{}_total", reason.slug()),
    ]
}

pub(crate) fn all_names() -> Vec<String> {
    [200, 202, 400, 401, 422, 500, 502, 503]
        .into_iter()
        .map(|status| format!("submit_answer_{status}_total"))
        .chain(
            Reason::ALL
                .iter()
                .map(|reason| format!("submit_answer_reason_{}_total", reason.slug())),
        )
        .collect()
}

/// Request owned, with no isolate tail to lose or mix into another answer.
pub(crate) struct SubmitCounters {
    /// No default: an answer whose arm named none counts `unclassified`, never
    /// an admission (the W1-OBS lens's LOW-2).
    reason: Cell<Option<Reason>>,
    couriers: crate::courier_counters::Counters,
    deltas: RefCell<BTreeMap<String, u64>>,
}

impl SubmitCounters {
    pub(crate) fn new() -> Self {
        Self {
            reason: Cell::new(None),
            couriers: crate::courier_counters::Counters::new(crate::courier_counters::Reader::Door),
            deltas: RefCell::default(),
        }
    }

    pub(crate) fn couriers(&self) -> crate::courier_counters::Counters {
        self.couriers.clone()
    }

    pub(crate) fn reason(&self, reason: Reason) {
        self.reason.set(Some(reason));
    }

    /// The admission went durable: `admit`, unless an earlier arm already
    /// named a more specific 2xx reason (a fast echo, a terminal judgement).
    pub(crate) fn admitted(&self) {
        if self.reason.get().is_none() {
            self.reason.set(Some(Reason::Admit));
        }
    }

    pub(crate) fn bump(&self, name: impl Into<String>, delta: u64) {
        if delta > 0 {
            *self.deltas.borrow_mut().entry(name.into()).or_default() += delta;
        }
    }

    /// Consuming the request prevents a second answer from being counted.
    pub(crate) fn finish_answer(self, status: u16) -> Self {
        // A deferred job can fail before it is queued; only a 202 is queued.
        if self.reason.get() == Some(Reason::Queued) && status != 202 {
            self.reason.set(Some(Reason::QueueFault));
        }
        for name in counter_names(status, self.reason.get().unwrap_or(Reason::Unclassified)) {
            self.bump(name, 1);
        }
        self
    }

    #[cfg(test)]
    pub(crate) fn take_deltas(self) -> BTreeMap<String, u64> {
        self.deltas.into_inner()
    }

    pub(crate) async fn flush(self, db: worker::D1Database) {
        for (name, delta) in self.couriers.take() {
            self.bump(name, delta);
        }
        crate::ops::bump_counters(&db, self.deltas.into_inner()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_answer_sql_counts_each_status_and_reason_once_with_existing_deltas() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE ops_counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL)",
        )
        .unwrap();
        let cases = [
            (200, Reason::Admit),
            (200, Reason::AdmitFastEcho),
            (422, Reason::Evicted),
            (400, Reason::ScriptRefused),
            (503, Reason::WalkCouldNotRun),
            (502, Reason::CorroboratorUnconfirmed),
            (202, Reason::Queued),
            (401, Reason::OperatorAuthRequired),
            (422, Reason::NetworkRejected),
            (502, Reason::Queued),
            (200, Reason::AdmitQueued),
        ];
        for (status, reason) in cases {
            let counters = SubmitCounters::new();
            counters.reason(reason);
            counters.bump("submit_existing_total", 1);
            let counters = counters.finish_answer(status);
            for (name, delta) in counters.deltas.into_inner() {
                db.execute(crate::ops::BUMP_COUNTER_SQL, rusqlite::params![name, delta])
                    .unwrap();
            }
        }
        let count = |name: &str| {
            db.query_row(
                "SELECT value FROM ops_counters WHERE name = ?",
                [name],
                |r| r.get::<_, u64>(0),
            )
            .unwrap()
        };
        assert_eq!(count("submit_answer_200_total"), 3);
        assert_eq!(count("submit_answer_reason_admit_queued_total"), 1);
        assert_eq!(count("submit_answer_422_total"), 2);
        assert_eq!(count("submit_answer_reason_queued_total"), 1);
        assert_eq!(count("submit_answer_reason_queue_fault_total"), 1);
        assert_eq!(count("submit_existing_total"), cases.len() as u64);
        let total: u64 = db
            .query_row(
                "SELECT SUM(value) FROM ops_counters WHERE name LIKE 'submit_answer_reason_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(total, cases.len() as u64);
        for reason in Reason::ALL {
            assert!(reason
                .slug()
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b == b'_'));
        }
    }

    #[test]
    fn submit_612_reason_is_named_without_provider_detail() {
        let message = "Arcade claims abcd accepted but the corroborating broadcaster could not confirm it; arbitrary provider detail";
        assert_eq!(Reason::broadcast(message), Reason::CorroboratorUnconfirmed);
        assert_eq!(
            Reason::broadcast("provider timeout"),
            Reason::BroadcastFault
        );
    }
}
