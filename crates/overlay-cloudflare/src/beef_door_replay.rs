//! P0-5f: real pure Worker doors, using small boundary shapes only.
use crate::{dead_letters, ef, queue};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use beef_limits::*;
use bsv_rs::transaction::Beef;
use overlay_engine::beef_limits;

#[path = "../../overlay-engine/tests/support/beef_doors.rs"]
mod shapes;

fn message(bytes: &[u8]) -> queue::MutationMessage {
    queue::MutationMessage {
        beef_b64: STANDARD.encode(bytes),
        topics: vec!["tm_test".into()],
        mode: "historical-tx".into(),
        reason: "boundary witness".into(),
        redrive: None,
    }
}

#[test]
fn ef_conversion_at_and_one_over_every_bound() {
    for (at, over, field) in [
        (
            shapes::transactions(512).0,
            shapes::transactions(513).0,
            "max_txs",
        ),
        (shapes::bumps(512).0, shapes::bumps(513).0, "max_bumps"),
        (
            shapes::sized_body(EF_BEEF_LIMITS.max_bytes).0,
            shapes::sized_body(EF_BEEF_LIMITS.max_bytes + 1).0,
            "max_bytes",
        ),
    ] {
        assert!(ef::beef_to_ef_batch(&at).is_ok());
        let error = match ef::beef_to_ef_batch(&over) {
            Err(error) => error,
            Ok(_) => panic!("over-bound EF BEEF was admitted"),
        };
        assert!(error.to_string().contains(field), "{error}");
    }
}

#[test]
fn ef_auxiliary_readers_refuse_one_over() {
    for (bytes, id) in [
        shapes::transactions(513),
        shapes::bumps(513),
        shapes::sized_body(EF_BEEF_LIMITS.max_bytes + 1),
    ] {
        assert!(ef::proven_subject_raw(&bytes).is_none());
        assert!(ef::strip_subject_bump(&bytes, &id).is_none());
        assert!(ef::missing_source_txids(&bytes).is_empty());
        let (raw_beef, raw_id) = shapes::body(2);
        let raw = Beef::from_binary(&raw_beef)
            .unwrap()
            .find_atomic_transaction(&raw_id)
            .unwrap()
            .to_hex();
        assert_eq!(ef::merge_raw_sources(&bytes, &[(raw_id, raw)]), bytes);
    }
}

#[test]
fn dead_letter_subject_at_and_one_over_every_bound() {
    for (at, over) in [
        (shapes::transactions(512).0, shapes::transactions(513).0),
        (shapes::bumps(512).0, shapes::bumps(513).0),
        (
            shapes::sized_body(DEAD_LETTER_BEEF_LIMITS.max_bytes).0,
            shapes::sized_body(DEAD_LETTER_BEEF_LIMITS.max_bytes + 1).0,
        ),
    ] {
        assert!(dead_letters::subject_of(&message(&at)).is_some());
        assert!(dead_letters::subject_of(&message(&over)).is_none());
    }
}

#[test]
fn queue_replay_at_and_one_over_every_bound() {
    for (at, over) in [
        (shapes::transactions(512).0, shapes::transactions(513).0),
        (shapes::bumps(512).0, shapes::bumps(513).0),
        (
            shapes::sized_body(QUEUE_BEEF_LIMITS.max_bytes).0,
            shapes::sized_body(QUEUE_BEEF_LIMITS.max_bytes + 1).0,
        ),
    ] {
        assert_eq!(
            queue::decode_replay_beef(&STANDARD.encode(&at)).unwrap(),
            at
        );
        assert!(queue::decode_replay_beef(&STANDARD.encode(&over)).is_err());
    }
}

#[test]
fn arc_callback_body_at_and_one_over() {
    let mut body = format!(
        r#"{{"txid":"{}","txStatus":"SEEN_ON_NETWORK"}}"#,
        "aa".repeat(32)
    );
    body.extend(std::iter::repeat_n(
        ' ',
        crate::routes::ARC_INGEST_BODY_MAX_BYTES - body.len(),
    ));
    assert!(crate::routes::classify_arc_ingest_body(&body).is_ok());
    body.push(' ');
    let error = match crate::routes::classify_arc_ingest_body(&body) {
        Err(error) => error,
        Ok(_) => panic!("one extra body byte admitted"),
    };
    assert!(error.contains("max_bytes"), "{error}");
}
