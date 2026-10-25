//! bsv-low #625, #626, #628 (the observability bar, the app layer's half): the names the app layer writes on
//! this worker's `ops_counters` through its batched courier-census flush, and the names of this worker's own
//! tx-any memo writes. `read_counters` seeds every one at 0, so a family that never bumped reads an explicit 0
//! on `/health/invariants.counters` and a delta rule can tell "never bumped" from "renamed". The app layer's
//! `obs_counters_tests` pins its own names equal to [`names`].

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
const OWED: [&str; 9] = [
    "applayer_owed_recompute_faults_total",
    "applayer_owed_recompute_shed_total",
    "applayer_owed_recompute_deferred_abandoned_total",
    "applayer_owed_recompute_deferred_faulted_total",
    "applayer_owed_recompute_arm_failed_total",
    "applayer_owed_recompute_lock_takeovers_total",
    "applayer_owed_sweep_proof_contradictions_total",
    "applayer_owed_recompute_walk_deadline_total",
    "applayer_owed_recompute_no_slice_total",
];
const OWED_READ_FAULT_KINDS: [&str; 9] = [
    "spender",
    "withheld_previous",
    "spender_store",
    "collected",
    "pay_vout",
    "pot_spenders",
    "hop_sweeps",
    "sweep_proofs",
    "markerless",
];
const RECORD_REFUSAL_CLASSES: [&str; 23] = [
    "bad_kind",
    "bad_script_hex",
    "body_too_large",
    "script_too_large",
    "not_canonical",
    "not_a_marker",
    "poster_mismatch",
    "signature_invalid",
    "refund_does_not_spend_the_pot",
    "refund_not_the_presigned_spend",
    "too_many_filed",
    "daily_cap_reached",
    "hop_not_indexed",
    "hop_marker_unverified",
    "sweep_does_not_spend_the_hop",
    "sweep_not_the_seats_spend",
    // bsv-low #848 (FAST-C): the handfile arm's refusals
    "handfile_shape",
    "handfile_payload_not_a_marker",
    "handfile_inconsistent",
    "settle_signature_invalid",
    "hop_marker_refuted",
    // bsv-low #663: a hop sweep filed under another game than its hop's
    "sweep_game_mismatch",
    "bad_body",
];
const AUTH_REFUSAL_REASONS: [&str; 19] = [
    "lane_unknown_session",
    "lane_expired",
    "lane_replay",
    "lane_bad_mac",
    "lane_malformed",
    "lane_unsupported",
    "lane_unavailable",
    "lane_other",
    "unauthorized",
    "invalid_auth",
    "session_not_found",
    "replayed_request",
    "server_misconfigured",
    "middleware_other",
    "handshake_fault",
    "no_session",
    "mismatch",
    "strict_unauthenticated",
    "misconfigured",
];
const IDENTITY_ROUTES: [&str; 5] = ["batch", "single", "picture", "verify", "kill"];
const IDENTITY_OUTCOMES: [&str; 4] = [
    "upstream_fault",
    "hash_mismatch",
    "no_resolver",
    "kill_list_fault",
];

/// bsv-low #893 section 5: the app layer's owed belt (`low_app_layer::owed_belt::counter_names`, pinned equal there).
/// The backlog `owed_belt_due_total` is a gauge the tick SETS; the rest are totals it adds.
pub const OWED_BELT: [&str; 8] = [
    "owed_belt_ticks_total",
    "owed_belt_asked_total",
    "owed_belt_found_total",
    "owed_belt_due_total",
    "owed_belt_faults_woc_total",
    "owed_belt_faults_bitails_total",
    "owed_belt_faults_bananablocks_total",
    "owed_belt_faults_other_total",
];

/// `/health/invariants.owedBelt`: the belt's durable totals and its backlog, from the counters read.
pub fn owed_belt_view(counters: &serde_json::Value) -> serde_json::Value {
    let n = |k: &str| {
        counters
            .get(k)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    serde_json::json!({
        "ticks": n("owed_belt_ticks_total"),
        "asked": n("owed_belt_asked_total"),
        "found": n("owed_belt_found_total"),
        "due": n("owed_belt_due_total"),
        "faults": {
            "woc": n("owed_belt_faults_woc_total"),
            "bitails": n("owed_belt_faults_bitails_total"),
            "bananablocks": n("owed_belt_faults_bananablocks_total"),
            "other": n("owed_belt_faults_other_total"),
        },
        "bounds": {"tickMs": 120_000, "outpointsPerTick": 8, "callsPerTick": 24, "callsPerOutpoint": 3, "unansweredRetryMs": 300_000},
    })
}

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
    v.extend(OWED.iter().map(|n| n.to_string()));
    v.push("applayer_owed_recomputes_total".to_string());
    // bsv-low #831 (the FIX-831 lens's L3): the refund-due pots the owed judgement found funding-not-held
    v.push("owed_refund_funding_unheld_total".to_string());
    v.extend(OWED_BELT.iter().map(|n| n.to_string()));
    // bsv-low #608 (the B-OWED-2 lens's LOW-1): the reads whose recompute only the unread-spender rule asked for
    v.push("owed_spender_rejudge_total".to_string());
    v.push("applayer_owed_read_faults_total".to_string());
    v.extend(
        OWED_READ_FAULT_KINDS
            .iter()
            .map(|k| format!("applayer_owed_read_faults_{k}_total")),
    );
    v.push("applayer_record_refused_total".to_string());
    v.extend(
        RECORD_REFUSAL_CLASSES
            .iter()
            .map(|c| format!("applayer_record_refused_{c}_total")),
    );
    v.push("applayer_auth_refused_total".to_string());
    v.extend(
        AUTH_REFUSAL_REASONS
            .iter()
            .map(|r| format!("applayer_auth_refused_{r}_total")),
    );
    v.extend(IDENTITY_ROUTES.iter().flat_map(|r| {
        IDENTITY_OUTCOMES
            .iter()
            .map(move |o| format!("applayer_identity_{r}_{o}_total"))
    }));
    // bsv-low #660 (B-OWED-3-FOLD L2): a word frame `/internal/tx-word` dropped as unheld
    v.push("applayer_tracker_event_unheld_total".to_string());
    // bsv-low #862 (L7-DEPTH): a missing or malformed SPEND_DEPTH var (the floor served), once per isolate
    v.push("spend_depth_var_malformed_total".to_string());
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
    fn obs_628_the_seed_list_is_distinct_and_names_each_family() {
        let names = seeded_names();
        let set: std::collections::BTreeSet<&String> = names.iter().collect();
        assert_eq!(set.len(), names.len(), "no name is seeded twice");
        for family in [
            "courier_applayer_bitails_notfound_total",
            "courier_applayer_woc_shape_total",
            "applayer_txany_memo_hit_absent_total",
            "applayer_txany_memo_write_anchor_total",
            "applayer_owed_recompute_faults_total",
            "applayer_owed_recomputes_total",
            "applayer_record_refused_total",
            "applayer_auth_refused_lane_bad_mac_total",
            "applayer_identity_batch_no_resolver_total",
            "txany_memo_write_retired_total",
        ] {
            assert!(names.iter().any(|n| n == family), "{family} is seeded");
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
