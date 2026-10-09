//! P0-5f: real pure Worker doors, using small boundary shapes only.
use crate::{dead_letters, ef, queue};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bsv_rs::transaction::Beef;
use overlay_engine::beef_limits::*;

#[path = "../../overlay-engine/tests/support/beef_doors.rs"]
mod shapes;

fn message(bytes: &[u8]) -> queue::MutationMessage {
    queue::MutationMessage {
        beef_b64: STANDARD.encode(bytes), topics: vec!["tm_test".into()],
        mode: "historical-tx".into(), reason: "boundary witness".into(), redrive: None,
    }
}

#[test]
fn ef_conversion_at_and_one_over_every_bound() {
    for (at, over, field) in [
        (shapes::transactions(256).0, shapes::transactions(257).0, "max_txs"),
        (shapes::bumps(256).0, shapes::bumps(257).0, "max_bumps"),
        (shapes::sized_body(EF_BEEF_LIMITS.max_bytes).0, shapes::sized_body(EF_BEEF_LIMITS.max_bytes + 1).0, "max_bytes"),
    ] {
        assert!(ef::beef_to_ef_batch(&at).is_ok());
        let error = ef::beef_to_ef_batch(&over).err().expect("over-bound EF BEEF was admitted");
        assert!(error.to_string().contains(field), "{error}");
    }
}

#[test]
fn ef_auxiliary_readers_refuse_one_over() {
    for (bytes, id) in [shapes::transactions(257), shapes::bumps(257), shapes::sized_body(EF_BEEF_LIMITS.max_bytes + 1)] {
        assert!(ef::proven_subject_raw(&bytes).is_none());
        assert!(ef::strip_subject_bump(&bytes, &id).is_none());
        assert!(ef::missing_source_txids(&bytes).is_empty());
        let (raw_beef, raw_id) = shapes::body(2);
        let raw = Beef::from_binary(&raw_beef).unwrap().find_atomic_transaction(&raw_id).unwrap().to_hex();
        assert_eq!(ef::merge_raw_sources(&bytes, &[(raw_id, raw)]), bytes);
    }
}

#[test]
fn dead_letter_subject_at_and_one_over_every_bound() {
    for (at, over) in [
        (shapes::transactions(256).0, shapes::transactions(257).0),
        (shapes::bumps(256).0, shapes::bumps(257).0),
        (shapes::sized_body(DEAD_LETTER_BEEF_LIMITS.max_bytes).0, shapes::sized_body(DEAD_LETTER_BEEF_LIMITS.max_bytes + 1).0),
    ] {
        assert!(dead_letters::subject_of(&message(&at)).is_some());
        assert!(dead_letters::subject_of(&message(&over)).is_none());
    }
}
