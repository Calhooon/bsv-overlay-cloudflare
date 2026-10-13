#[test]
fn ns3d_l3_compatibility_pins_cover_real_builders_and_runtime() {
    let pins = include_str!("tracker_served.rs");
    for builder in [
        "tx_any_body(",
        "tx_any_batch_body(",
        "spent_any_body(",
        "utxo_status_body(",
        "owed_body(",
    ] {
        assert!(
            pins.contains(builder),
            "production compatibility coverage missing {builder}"
        );
    }
    assert!(pins.contains("runtime::decorate("));
    let routes = include_str!("../../../tools/lane-499/d1_budget_route_ci.mjs");
    assert!(routes.contains("an expired memo is re-asked even when its spender is tracked"));
}

#[test]
fn ns3d2_note1_fault_cell_is_in_the_route_tier() {
    let make = include_str!("../../../Makefile");
    assert!(make.contains("node tools/lane-499/tracker_fault_route_ci.mjs http://127.0.0.1:$$PA \"$$state\""), "real-D1 fault cell is not wired into make");
}

#[test]
fn ns3d2_note5_register_names_hold_authority_and_transient_retry() {
    let notes = include_str!("../../../docs/audit/NS-3-APPLAYER-TRACKER-notes-2026-10-12.md");
    let register = &notes[notes.find("### Captain's register text").unwrap()..notes.find("Launch 5 measurement").unwrap()];
    assert!(register.contains("a callback or acceptance for a subject the overlay does not hold writes no row"), "register omits public write authority");
    assert!(register.contains("a proof that does not check against its header is skipped, counted by reason and not retried until the next `BACKFILL_VERSION`; a header fault ends the pass and the subject is retried"), "register conflates invalid proof and transient fault");
}
