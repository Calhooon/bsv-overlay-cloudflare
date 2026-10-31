//! D2-ACTOR-F-FOLD (bsv-low #959, the lens's LOW-2: a NEW filing reaches zero reads): the app layer's statement for an
//! owed note's publish read (`owed_events_publish::statement_of`). Each source that carries its row (D2-ACTOR-F) reads,
//! in the SAME statement, the carried CLOSURE ([`crate::owed_actor::closure_column`]): the record's rows of every
//! mirrored family for the txids its rows name, at one high-water. The records door's filings (a collected marker
//! naming its payout, a refund naming its pot, a result naming its settle, a sweep naming its hop and its sweep, a
//! party naming its pot) are the app layer's publishes, so a warm actor applies a new filing with no D1 call.
use std::collections::BTreeMap;
use std::sync::LazyLock;

/// The carrier statements with their closure, built once an isolate from the shared statements.
static STATEMENTS: LazyLock<BTreeMap<&'static str, String>> = LazyLock::new(|| {
    crate::owed_events_publish::CARRIERS
        .iter()
        .filter_map(|s| {
            Some((
                *s,
                crate::owed_actor::with_closure(crate::owed_events_publish::read_sql(s)?),
            ))
        })
        .collect()
});

/// The app layer's statement of `source`: a carrier's with its closure; `None` for the rest (the shared one).
pub fn statement(source: &str) -> Option<&'static str> {
    STATEMENTS.get(source).map(String::as_str)
}
