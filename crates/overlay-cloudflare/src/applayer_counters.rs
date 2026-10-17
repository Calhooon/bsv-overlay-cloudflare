//! bsv-low #625, #626 (the observability bar, the app layer's half): the names the app layer writes on this
//! worker's `ops_counters` through its batched courier-census flush, and the names of this worker's own tx-any
//! memo writes. `read_counters` seeds every one at 0, so a family that never bumped reads an explicit 0 on
//! `/health/invariants.counters` and a delta rule can tell "never bumped" from "renamed".

/// The courier providers and the per-reader outcomes of the app layer (`courier_applayer_<provider>_<outcome>_total`).
const PROVIDERS: [&str; 5] = ["woc", "bananablocks", "bitails", "arcade", "other"];
const SHAPED_OUTCOMES: [&str; 5] = ["ok", "notfound", "ratelimited", "fault", "shape"];
/// The tx-any memo kinds the app layer reads and writes (`applayer_txany_memo_<op>_<kind>_total`).
const MEMO_HIT_KINDS: [&str; 6] = [
    "absent",
    "unconfirmable",
    "refused",
    "retired",
    "anchor",
    "isolate_unconfirmable",
];
const MEMO_WRITE_KINDS: [&str; 2] = ["anchor", "isolate_unconfirmable"];
/// bsv-low #626: this worker's durable verdict memo writes (`tx_any_verdicts`), by the kind written: the retire
/// pass's `refused` / `retired` and the eviction's `refused`. The app layer only reads these rows.
pub const COUNTER_TXANY_MEMO_WRITE_REFUSED: &str = "txany_memo_write_refused_total";
/// The retire pass's absence memo (`retired`): see [`COUNTER_TXANY_MEMO_WRITE_REFUSED`].
pub const COUNTER_TXANY_MEMO_WRITE_RETIRED: &str = "txany_memo_write_retired_total";

/// PURE: the durable name of one of this worker's verdict memo writes, by its kind.
pub fn memo_write_counter(kind: &str) -> &'static str {
    match kind {
        "refused" => COUNTER_TXANY_MEMO_WRITE_REFUSED,
        _ => COUNTER_TXANY_MEMO_WRITE_RETIRED,
    }
}

/// Every app-layer name of the observability bar's families (the seed list).
pub fn names() -> Vec<String> {
    let mut v: Vec<String> = PROVIDERS
        .iter()
        .flat_map(|p| {
            SHAPED_OUTCOMES
                .iter()
                .map(move |o| format!("courier_applayer_{p}_{o}_total"))
        })
        .collect();
    v.extend(
        MEMO_WRITE_KINDS
            .iter()
            .map(|k| format!("applayer_txany_memo_write_{k}_total")),
    );
    v.extend(
        MEMO_HIT_KINDS
            .iter()
            .map(|k| format!("applayer_txany_memo_hit_{k}_total")),
    );
    v
}

/// Every name [`crate::ops`]'s `read_counters` seeds for this module: the app layer's and this worker's memo writes.
pub fn seeded_names() -> Vec<String> {
    let mut v = names();
    v.push(COUNTER_TXANY_MEMO_WRITE_REFUSED.to_string());
    v.push(COUNTER_TXANY_MEMO_WRITE_RETIRED.to_string());
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn obs_625_the_seed_list_is_distinct_and_names_each_outcome() {
        let names = seeded_names();
        let set: std::collections::BTreeSet<&String> = names.iter().collect();
        assert_eq!(set.len(), names.len(), "no name is seeded twice");
        for name in [
            "courier_applayer_bitails_notfound_total",
            "courier_applayer_woc_shape_total",
            "courier_applayer_woc_ratelimited_total",
            "applayer_txany_memo_hit_absent_total",
            "applayer_txany_memo_write_anchor_total",
            "txany_memo_write_retired_total",
        ] {
            assert!(names.iter().any(|n| n == name), "{name} is seeded");
        }
        assert_eq!(
            memo_write_counter("refused"),
            "txany_memo_write_refused_total"
        );
        assert_eq!(
            memo_write_counter("retired"),
            "txany_memo_write_retired_total"
        );
    }
}
