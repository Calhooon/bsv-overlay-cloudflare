//! bsv-low #625 (the observability bar, the app layer's half): the names the app layer writes on this worker's
//! `ops_counters` through its batched courier-census flush. `read_counters` seeds every one at 0, so a family that
//! never bumped reads an explicit 0 on `/health/invariants.counters` and a delta rule can tell "never bumped" from
//! "renamed".

/// The courier providers and the per-reader outcomes of the app layer (`courier_applayer_<provider>_<outcome>_total`).
const PROVIDERS: [&str; 5] = ["woc", "bananablocks", "bitails", "arcade", "other"];
const SHAPED_OUTCOMES: [&str; 5] = ["ok", "notfound", "ratelimited", "fault", "shape"];
/// Every app-layer name of the observability bar's families (the seed list).
pub fn names() -> Vec<String> {
    PROVIDERS
        .iter()
        .flat_map(|p| {
            SHAPED_OUTCOMES
                .iter()
                .map(move |o| format!("courier_applayer_{p}_{o}_total"))
        })
        .collect()
}

/// Every name [`crate::ops`]'s `read_counters` seeds for this module.
pub fn seeded_names() -> Vec<String> {
    names()
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
        ] {
            assert!(names.iter().any(|n| n == name), "{name} is seeded");
        }
    }
}
