//! P0-5f: whole-transaction discovery hooks recheck their carried BEEF.
use bsv_overlay_discovery::tx_facts::facts_from_atomic_beef;
use overlay_engine::beef_limits::DISCOVERY_BEEF_LIMITS;

#[path = "../../overlay-engine/tests/support/beef_doors.rs"]
mod shapes;

#[test]
fn discovery_reader_at_and_one_over_every_bound() {
    for (at, over) in [
        (shapes::transactions(512), shapes::transactions(513)),
        (shapes::bumps(512), shapes::bumps(513)),
        (
            shapes::sized_body(DISCOVERY_BEEF_LIMITS.max_bytes),
            shapes::sized_body(DISCOVERY_BEEF_LIMITS.max_bytes + 1),
        ),
    ] {
        assert!(facts_from_atomic_beef(&at.0, &at.1).is_some());
        assert!(facts_from_atomic_beef(&over.0, &over.1).is_none());
    }
}
