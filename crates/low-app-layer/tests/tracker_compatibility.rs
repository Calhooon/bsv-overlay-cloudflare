#[test]
fn ns3d_l3_compatibility_pins_cover_real_builders_and_runtime() {
    let pins = include_str!("tracker_served.rs");
    for builder in ["tx_any_body(", "tx_any_batch_body(", "spent_any_body(", "utxo_status_body(", "owed_body("] {
        assert!(pins.contains(builder), "production compatibility coverage missing {builder}");
    }
    assert!(pins.contains("runtime::decorate("));
    let routes = include_str!("../../../tools/lane-499/d1_budget_route_ci.mjs");
    assert!(routes.contains("an expired memo is re-asked even when its spender is tracked"));
}
