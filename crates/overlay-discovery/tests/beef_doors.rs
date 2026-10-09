//! NL-6: whole-transaction discovery hooks read a carried BEEF of any size
//! and refuse invalid bytes only (the P0-5f witness, inverted).
use bsv_overlay_discovery::tx_facts::facts_from_atomic_beef;
use overlay_engine::beef_limits::DISCOVERY_BEEF_LIMITS;

#[path = "../../overlay-engine/tests/support/beef_doors.rs"]
mod shapes;

#[test]
fn discovery_reader_over_every_former_bound() {
    for over in [
        shapes::transactions(513),
        shapes::bumps(513),
        shapes::sized_body(DISCOVERY_BEEF_LIMITS.max_bytes + 1),
    ] {
        assert!(facts_from_atomic_beef(&over.0, &over.1).is_some());
    }
}

#[test]
fn discovery_reader_refuses_invalid_bytes() {
    let (_, id) = shapes::body(1);
    for (bytes, _, _) in shapes::invalid() {
        assert!(facts_from_atomic_beef(&bytes, &id).is_none());
    }
}
