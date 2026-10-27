//! bsv-low #935: the coalescer's counters are seeded by the overlay and served on `/health/invariants.push`.
use low_app_layer::push_coalesce as pc;
use serde_json::json;

#[test]
fn fix935_g_the_counters_are_seeded_and_served_on_health() {
    let names = pc::counter_names();
    let overlay: Vec<String> = bsv_overlay_cloudflare::applayer_counters::PUSH
        .iter()
        .map(|n| n.to_string())
        .collect();
    assert_eq!(names, overlay);
    let seeded = bsv_overlay_cloudflare::applayer_counters::seeded_names();
    assert!(names.iter().all(|n| seeded.contains(n)));
    let view = bsv_overlay_cloudflare::applayer_counters::push_view(&json!({
        pc::COUNTER_COALESCED: 1704, pc::COUNTER_HELD: 3, pc::COUNTER_REFUSED_BUDGET: 2,
        pc::COUNTER_GIVEN_UP: 1, pc::COUNTER_WINDOWS: 300,
    }));
    assert_eq!(
        view,
        json!({"coalesced": 1704, "held": 3, "refusedBudget": 2, "givenUp": 1, "windows": 300})
    );
    let ops = include_str!("../../overlay-cloudflare/src/ops.rs");
    assert!(ops.contains("body[\"push\"] = crate::applayer_counters::push_view(&counters);"));
    let health = pc::health_json();
    for k in ["coalesced", "held", "refusedBudget", "givenUp", "windows"] {
        assert!(health[k].is_u64(), "{k}");
    }
}
