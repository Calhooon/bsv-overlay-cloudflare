//! NL-6: the P0-5f app witnesses, inverted. The shared module no longer
//! refuses a valid BEEF for its size or its counts, so the stored-serving and
//! parent-merge readers read one over every former bound. (The app's own
//! doors are bsv-low #585's; this file only follows the module.)
use crate::{beef_guard, credit_beef};
use bsv_rs::transaction::Beef;
use overlay_engine::beef_limits::APP_BEEF_LIMITS;

#[path = "../../overlay-engine/tests/support/beef_doors.rs"]
mod shapes;

fn over_every_former_bound() -> [Vec<u8>; 3] {
    [
        shapes::transactions(513).0,
        shapes::bumps(513).0,
        shapes::sized_body(APP_BEEF_LIMITS.max_bytes + 1).0,
    ]
}

#[test]
fn stored_serving_over_every_former_bound() {
    for over in over_every_former_bound() {
        assert!(matches!(beef_guard::parse_for_serving(&over), Ok(Some(_))));
    }
    assert!(matches!(
        beef_guard::parse_for_serving(&[1, 2, 3]),
        Ok(None)
    ));
}

#[test]
fn parent_merge_over_every_former_bound() {
    for over in over_every_former_bound() {
        assert!(credit_beef::merge_parent(&mut Beef::new(), &over));
    }
}
