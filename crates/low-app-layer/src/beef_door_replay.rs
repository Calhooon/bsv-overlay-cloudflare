//! P0-5f: stored serving and fetched-parent readers use the actual app policy.
use crate::{beef_guard, credit_beef};
use bsv_rs::transaction::Beef;
use overlay_engine::beef_limits::APP_BEEF_LIMITS;

#[path = "../../overlay-engine/tests/support/beef_doors.rs"]
mod shapes;

#[test]
fn stored_serving_at_and_one_over_every_bound() {
    for (at, over) in [
        (shapes::transactions(512).0, shapes::transactions(513).0),
        (shapes::bumps(512).0, shapes::bumps(513).0),
        (
            shapes::sized_body(APP_BEEF_LIMITS.max_bytes).0,
            shapes::sized_body(APP_BEEF_LIMITS.max_bytes + 1).0,
        ),
    ] {
        assert!(matches!(beef_guard::parse_for_serving(&at), Ok(Some(_))));
        assert!(matches!(
            beef_guard::parse_for_serving(&over),
            Err(beef_guard::Guarded::OverLimit)
        ));
    }
    assert!(matches!(
        beef_guard::parse_for_serving(&[1, 2, 3]),
        Ok(None)
    ));
}

#[test]
fn parent_merge_at_and_one_over_every_bound() {
    for (at, over) in [
        (shapes::transactions(512).0, shapes::transactions(513).0),
        (shapes::bumps(512).0, shapes::bumps(513).0),
        (
            shapes::sized_body(APP_BEEF_LIMITS.max_bytes).0,
            shapes::sized_body(APP_BEEF_LIMITS.max_bytes + 1).0,
        ),
    ] {
        assert!(credit_beef::merge_parent(&mut Beef::new(), &at));
        let mut unchanged = Beef::new();
        assert!(!credit_beef::merge_parent(&mut unchanged, &over));
        assert!(unchanged.txs.is_empty() && unchanged.bumps.is_empty());
    }
}
