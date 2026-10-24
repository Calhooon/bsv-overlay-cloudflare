//! Per-reader courier outcomes, accumulated for a durable request or pass flush.
//!
//! bsv-low #834 (FIX-834): a pass's counters may also PACE its asks ([`Counters::paced`], the janitor's tick and
//! `/admin/complete-proofs` only). Every courier GET of the pass goes through `proof_fetcher::http_get_counted`,
//! which asks [`Counters::pace`] first and reports the answer to [`Counters::pace_result`], so the pacing is one
//! state over every fetcher the pass builds (each of them keeps its own per-rung circuit as before).
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
    /// bsv-low #834: the rung cannot answer this question by construction (pruned Bitails' `/tx` 404 for a subject
    /// the index holds unmined, its `spent: ""` for an unspent output). Not an ask the rung could answer: never a
    /// fault, never `notfound`, so the fleet cell's denominator (`ok + notfound + fault + shape`) excludes it.
    CouldNotLook,
    /// bsv-low #834: not asked (the pacer's per-tick budget, its back-off or its cool-down, or a rung skipped by
    /// construction). No subrequest was made.
    Skipped,
}

impl Outcome {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotFound => "notfound",
            Self::Fault => "fault",
            Self::Shape => "shape",
            Self::CouldNotLook => "couldnotlook",
            Self::Skipped => "skipped",
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

pub(crate) fn provider(rung: &str) -> &'static str {
    match rung {
        "arcade" => "arcade",
        "bitails" | "bitails_tx" | "bitails_history" | "bitails_presence" => "bitails",
        "woc" | "woc_history" | "woc_presence" => "woc",
        "bananablocks" | "bananablocks_txo" | "bananablocks_presence" => "bananablocks",
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
    // bsv-low #834: the janitor's paced outcomes and its back-offs, served from 0.
    for provider in PROVIDERS {
        for outcome in [Outcome::CouldNotLook, Outcome::Skipped] {
            names.push(name(Reader::Janitor, provider, outcome));
        }
        names.push(backoff_name(provider));
    }
    names
}

/// bsv-low #834: the durable count of a provider's back-offs (a tick in which its third 429, 5xx or transport fault
/// stopped the janitor asking it, and started its cool-down).
pub(crate) fn backoff_name(provider: &str) -> String {
    format!("courier_janitor_{provider}_backoff_total")
}

/// bsv-low #834: the janitor's per-tick ask budget per provider. A pass that has spent it skips the provider for
/// the rest of the pass (counted `skipped`) and the next rung of each ladder takes the ask. Arcade is LOW's own
/// broadcaster (asked first for our own transactions) and gets twice the third-party share.
pub(crate) const COURIER_TICK_BUDGET: [(&str, u32); 4] = [
    ("arcade", 128),
    ("bitails", 64),
    ("woc", 64),
    ("bananablocks", 64),
];

/// bsv-low #834: a provider's 429, 5xx or transport faults in one pass that back it off for the rest of the pass
/// (the owner's convention: "a rung that faults three times in a tick is skipped for the rest of it").
pub(crate) const COURIER_BACKOFF_FAULTS: u32 = 3;

/// bsv-low #834: how long a backed-off provider stays off ACROSS passes (an isolate memo, never D1: a fresh isolate
/// asks it again, the fail-open direction for a cost bound). 20 minutes is one `*/15` tick and a third, so the tick
/// after a back-off rests the provider and the one after that asks it again.
pub(crate) const COURIER_COOLDOWN_MS: u64 = 20 * 60 * 1000;

fn tick_budget(provider: &str) -> u32 {
    COURIER_TICK_BUDGET
        .iter()
        .find(|(p, _)| *p == provider)
        .map_or(0, |(_, b)| *b)
}

thread_local! {
    /// bsv-low #834: per provider, the end of its cool-down (ms since the epoch), this isolate only.
    static COOLDOWN_UNTIL: RefCell<BTreeMap<&'static str, u64>> = const { RefCell::new(BTreeMap::new()) };
}

#[cfg(test)]
pub(crate) fn clear_cooldowns() {
    COOLDOWN_UNTIL.with(|c| c.borrow_mut().clear());
}

/// Why the pacer did not let an ask go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PaceSkip {
    /// the per-tick budget is spent
    Budget,
    /// the provider faulted [`COURIER_BACKOFF_FAULTS`] times in this pass
    BackedOff,
    /// a previous pass backed it off less than [`COURIER_COOLDOWN_MS`] ago
    CoolDown,
    /// the rung answered "could not look" [`COURIER_COULD_NOT_LOOK_CAP`] times in this pass: it cannot answer this
    /// question here (pruned Bitails), so it is skipped by construction for the rest of the pass
    CannotAnswer,
}

impl PaceSkip {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Budget => "budget",
            Self::BackedOff => "backed-off",
            Self::CoolDown => "cool-down",
            Self::CannotAnswer => "cannot-answer",
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ProviderPace {
    asked: u32,
    budget_left: u32,
    trip_faults: u32,
    skipped: u32,
    backed_off: bool,
    cooled_until: Option<u64>,
}

#[derive(Debug)]
struct Pacer {
    now_ms: u64,
    providers: BTreeMap<&'static str, ProviderPace>,
    /// per RUNG, its "could not look" answers in this pass
    could_not_look: BTreeMap<String, u32>,
}

/// bsv-low #834: a rung's "could not look" answers in one pass after which it is skipped by construction for the
/// rest of the pass (it is not a fault and does not back the provider off: the provider's other rungs still ask).
pub(crate) const COURIER_COULD_NOT_LOOK_CAP: u32 = 3;

impl Pacer {
    fn new(now_ms: u64) -> Self {
        let cooled = COOLDOWN_UNTIL.with(|c| c.borrow().clone());
        let providers = PROVIDERS
            .into_iter()
            .map(|p| {
                let until = cooled.get(p).copied().filter(|until| *until > now_ms);
                (
                    p,
                    ProviderPace {
                        budget_left: tick_budget(p),
                        cooled_until: until,
                        ..ProviderPace::default()
                    },
                )
            })
            .collect();
        Self {
            now_ms,
            providers,
            could_not_look: BTreeMap::new(),
        }
    }
}

/// PURE (bsv-low #834): is this answer one that backs a provider off? A 429, a 5xx, or no answer at all (transport,
/// a timeout). A 404, a 4xx other than 429, and a body the parser refuses are not: those are the rung's word.
pub(crate) fn backs_off(status: Option<u16>) -> bool {
    match status {
        None => true,
        Some(s) => s == 429 || s >= 500,
    }
}

/// Shared only within one request or maintenance pass, never between isolates or requests.
#[derive(Clone, Debug)]
pub(crate) struct Counters {
    reader: Reader,
    pending: Rc<RefCell<BTreeMap<String, u64>>>,
    pacer: Option<Rc<RefCell<Pacer>>>,
}

impl Counters {
    pub(crate) fn new(reader: Reader) -> Self {
        Self {
            reader,
            pending: Rc::default(),
            pacer: None,
        }
    }

    /// bsv-low #834: counters that also PACE the pass's courier asks: a per-provider budget per pass, a back-off
    /// after the provider's third 429/5xx/transport fault, and a cool-down across passes (this isolate's memo).
    pub(crate) fn paced(reader: Reader, now_ms: u64) -> Self {
        Self {
            pacer: Some(Rc::new(RefCell::new(Pacer::new(now_ms)))),
            ..Self::new(reader)
        }
    }

    /// bsv-low #834: may this rung's provider be asked now? `Err` = skipped, counted `skipped` (no subrequest).
    /// Unpaced counters always answer `Ok`. An `Ok` spends one unit of the provider's per-pass budget.
    pub(crate) fn pace(&self, rung: &str) -> Result<(), PaceSkip> {
        let Some(pacer) = &self.pacer else {
            return Ok(());
        };
        let provider = provider(rung);
        let verdict = {
            let mut pacer = pacer.borrow_mut();
            let cannot_answer = pacer
                .could_not_look
                .get(rung)
                .is_some_and(|n| *n >= COURIER_COULD_NOT_LOOK_CAP);
            let Some(p) = pacer.providers.get_mut(provider) else {
                return Ok(());
            };
            let verdict = if cannot_answer {
                Err(PaceSkip::CannotAnswer)
            } else if p.cooled_until.is_some() {
                Err(PaceSkip::CoolDown)
            } else if p.backed_off {
                Err(PaceSkip::BackedOff)
            } else if p.budget_left == 0 {
                Err(PaceSkip::Budget)
            } else {
                p.budget_left -= 1;
                p.asked += 1;
                Ok(())
            };
            if verdict.is_err() {
                p.skipped += 1;
            }
            verdict
        };
        if verdict.is_err() {
            self.note(provider, Outcome::Skipped);
        }
        verdict
    }

    /// bsv-low #834: the rung answered "could not look" (counted `couldnotlook`); its
    /// [`COURIER_COULD_NOT_LOOK_CAP`]th in a paced pass skips it by construction for the rest of the pass.
    pub(crate) fn could_not_look(&self, rung: &str) {
        if let Some(pacer) = &self.pacer {
            *pacer
                .borrow_mut()
                .could_not_look
                .entry(rung.to_string())
                .or_default() += 1;
        }
    }

    /// bsv-low #834: a rung skipped outside the pacer (BY CONSTRUCTION, it cannot answer the question asked, or by
    /// its fetcher's own open circuit): counted `skipped`, no subrequest, no budget spent.
    /// The janitor's reader only: the door's family has no `skipped` outcome.
    pub(crate) fn note_skip(&self, rung: &str) {
        if !matches!(self.reader, Reader::Janitor) {
            return;
        }
        if let Some(pacer) = &self.pacer {
            if let Some(p) = pacer.borrow_mut().providers.get_mut(provider(rung)) {
                p.skipped += 1;
            }
        }
        self.note(rung, Outcome::Skipped);
    }

    /// bsv-low #834: the answer of an ask [`Self::pace`] let go (`None` = no answer). The provider's
    /// [`COURIER_BACKOFF_FAULTS`]th 429/5xx/transport fault of the pass backs it off for the rest of the pass and
    /// for [`COURIER_COOLDOWN_MS`] in this isolate, counted `courier_janitor_<provider>_backoff_total`.
    pub(crate) fn pace_result(&self, rung: &str, status: Option<u16>) {
        let Some(pacer) = &self.pacer else {
            return;
        };
        if !backs_off(status) {
            return;
        }
        let provider = provider(rung);
        let tripped = {
            let mut pacer = pacer.borrow_mut();
            let now = pacer.now_ms;
            let Some(p) = pacer.providers.get_mut(provider) else {
                return;
            };
            p.trip_faults += 1;
            if p.trip_faults >= COURIER_BACKOFF_FAULTS && !p.backed_off {
                p.backed_off = true;
                let until = now.saturating_add(COURIER_COOLDOWN_MS);
                COOLDOWN_UNTIL.with(|c| {
                    if let Some(key) = PROVIDERS.into_iter().find(|k| *k == provider) {
                        c.borrow_mut().insert(key, until);
                    }
                });
                true
            } else {
                false
            }
        };
        if tripped {
            *self
                .pending
                .borrow_mut()
                .entry(backoff_name(provider))
                .or_default() += 1;
        }
    }

    /// bsv-low #834: this pass's pacing, per provider (`null` for unpaced counters): asked, skipped, the budget
    /// left, whether it backed off in this pass, and the cool-down it started under.
    pub(crate) fn pacing_view(&self) -> serde_json::Value {
        let Some(pacer) = &self.pacer else {
            return serde_json::Value::Null;
        };
        let pacer = pacer.borrow();
        let mut out = serde_json::Map::new();
        for (provider, p) in &pacer.providers {
            let could_not_look: u32 = pacer
                .could_not_look
                .iter()
                .filter(|(rung, _)| self::provider(rung) == *provider)
                .map(|(_, n)| *n)
                .sum();
            out.insert(
                (*provider).to_string(),
                serde_json::json!({
                    "asked": p.asked,
                    "skipped": p.skipped,
                    "budgetLeft": p.budget_left,
                    "tripFaults": p.trip_faults,
                    "couldNotLook": could_not_look,
                    "backedOff": p.backed_off,
                    "cooledUntilMs": p.cooled_until,
                }),
            );
        }
        serde_json::Value::Object(out)
    }

    /// bsv-low #834: the pass's subrequests to couriers (every ask the pacer let go).
    pub(crate) fn paced_asks(&self) -> u32 {
        self.pacer
            .as_ref()
            .map_or(0, |p| p.borrow().providers.values().map(|p| p.asked).sum())
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
