//! Per-reader courier outcomes, accumulated for a durable request or pass flush.
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Reader {
    Door,
    Janitor,
}

impl Reader {
    fn name(self) -> &'static str {
        match self {
            Self::Door => "door",
            Self::Janitor => "janitor",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Ok,
    NotFound,
    Fault,
    Shape,
}

impl Outcome {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotFound => "notfound",
            Self::Fault => "fault",
            Self::Shape => "shape",
        }
    }

    pub(crate) fn response(status: u16, parsed: bool, found: bool) -> Self {
        match status {
            404 | 410 => Self::NotFound,
            200..=299 if !parsed => Self::Shape,
            200..=299 if !found => Self::NotFound,
            200..=299 => Self::Ok,
            _ => Self::Fault,
        }
    }
}

const PROVIDERS: [&str; 4] = ["arcade", "bitails", "woc", "bananablocks"];

fn provider(rung: &str) -> &'static str {
    match rung {
        "arcade" => "arcade",
        "bitails" | "bitails_tx" | "bitails_history" => "bitails",
        "woc" | "woc_history" => "woc",
        "bananablocks" | "bananablocks_txo" => "bananablocks",
        _ => "other",
    }
}

fn name(reader: Reader, rung: &str, outcome: Outcome) -> String {
    format!(
        "courier_{}_{}_{}_total",
        reader.name(),
        provider(rung),
        outcome.name()
    )
}

pub(crate) fn all_names() -> Vec<String> {
    let mut names = Vec::new();
    for reader in [Reader::Door, Reader::Janitor] {
        for provider in PROVIDERS.into_iter().chain(["other"]) {
            for outcome in [
                Outcome::Ok,
                Outcome::NotFound,
                Outcome::Fault,
                Outcome::Shape,
            ] {
                names.push(name(reader, provider, outcome));
            }
        }
    }
    names
}

/// Shared only within one request or maintenance pass, never between isolates or requests.
#[derive(Clone, Debug)]
pub(crate) struct Counters {
    reader: Reader,
    pending: Rc<RefCell<BTreeMap<String, u64>>>,
}

impl Counters {
    pub(crate) fn new(reader: Reader) -> Self {
        Self {
            reader,
            pending: Rc::default(),
        }
    }

    pub(crate) fn note(&self, rung: &str, outcome: Outcome) {
        *self
            .pending
            .borrow_mut()
            .entry(name(self.reader, rung, outcome))
            .or_default() += 1;
    }

    pub(crate) fn attempt(&self, rung: &str) -> Attempt {
        Attempt {
            counters: self.clone(),
            provider: provider(rung),
            outcome: Outcome::Fault,
        }
    }

    pub(crate) fn take(&self) -> BTreeMap<String, u64> {
        std::mem::take(&mut *self.pending.borrow_mut())
    }

    pub(crate) async fn flush(&self, db: &worker::D1Database) {
        crate::ops::bump_counters(db, self.take()).await;
    }
}

/// A cancelled HTTP future still made an ask and therefore records a fault.
#[derive(Debug)]
pub(crate) struct Attempt {
    counters: Counters,
    provider: &'static str,
    outcome: Outcome,
}

impl Attempt {
    pub(crate) fn set(attempt: &mut Option<Self>, outcome: Outcome) {
        if let Some(attempt) = attempt {
            attempt.outcome = outcome;
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.counters.note(self.provider, self.outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn obs_625_outcomes_share_app_provider_names_and_are_durable_sql_rows() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE ops_counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL)",
        )
        .unwrap();
        for reader in [Reader::Door, Reader::Janitor] {
            let counts = Counters::new(reader);
            for (status, parsed, found) in [
                (200, true, true),
                (404, false, false),
                (503, false, false),
                (200, false, false),
            ] {
                counts.note("bitails_tx", Outcome::response(status, parsed, found));
            }
            for (name, delta) in counts.take() {
                db.execute(crate::ops::BUMP_COUNTER_SQL, rusqlite::params![name, delta])
                    .unwrap();
            }
            assert!(counts.take().is_empty());
        }
        let rows: u64 = db
            .query_row(
                "SELECT COUNT(*) FROM ops_counters WHERE value = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 8);
        assert_eq!(
            name(Reader::Janitor, "bitails_tx", Outcome::NotFound),
            "courier_janitor_bitails_notfound_total"
        );
        assert_eq!(Outcome::response(404, false, false), Outcome::NotFound);
        assert_eq!(Outcome::response(200, false, false), Outcome::Shape);
        assert_eq!(Outcome::response(503, false, false), Outcome::Fault);
    }

    #[test]
    fn obs_625_cancelled_ask_is_a_fault_and_a_completed_ask_counts_once() {
        let counts = Counters::new(Reader::Door);
        drop(counts.attempt("woc"));
        let mut completed = Some(counts.attempt("woc"));
        Attempt::set(&mut completed, Outcome::Shape);
        drop(completed);
        let rows = counts.take();
        assert_eq!(rows["courier_door_woc_fault_total"], 1);
        assert_eq!(rows["courier_door_woc_shape_total"], 1);
        assert_eq!(rows.len(), 2);
    }
}
