//! NL-6: the P0-5f Worker door witnesses, inverted. A valid BEEF over each
//! former budget passes every pure Worker door; invalid bytes are refused
//! with the offset and the kind.
use crate::{dead_letters, ef, queue};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use beef_limits::*;
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

/// One shape at each former bound (read then, read now) and one over it.
fn at_and_over(door: &BeefLimits) -> [((Vec<u8>, String), (Vec<u8>, String)); 3] {
    [
        (shapes::transactions(512), shapes::transactions(513)),
        (shapes::bumps(512), shapes::bumps(513)),
        (
            shapes::sized_body(door.max_bytes),
            shapes::sized_body(door.max_bytes + 1),
        ),
    ]
}

#[test]
fn ef_conversion_over_every_former_bound() {
    for (_, (over, _)) in at_and_over(&EF_BEEF_LIMITS) {
        let converted = ef::beef_to_ef_batch(&over);
        assert!(converted.is_ok(), "{:?}", converted.err());
    }
}

#[test]
fn ef_auxiliary_readers_read_one_over_as_they_read_one_at() {
    for ((at, at_id), (over, over_id)) in at_and_over(&EF_BEEF_LIMITS) {
        assert!(ef::proven_subject_raw(&at).is_some());
        assert!(ef::proven_subject_raw(&over).is_some());
        assert!(ef::strip_subject_bump(&at, &at_id).is_some());
        assert!(ef::strip_subject_bump(&over, &over_id).is_some());
        assert_eq!(
            ef::missing_source_txids(&over),
            ef::missing_source_txids(&at)
        );
    }
}

#[test]
fn dead_letter_subject_over_every_former_bound() {
    for (_, (over, id)) in at_and_over(&DEAD_LETTER_BEEF_LIMITS) {
        assert_eq!(dead_letters::subject_of(&message(&over)), Some(id));
    }
}

#[test]
fn queue_replay_over_every_former_bound() {
    for (_, (over, _)) in at_and_over(&QUEUE_BEEF_LIMITS) {
        assert_eq!(
            queue::decode_replay_beef(&STANDARD.encode(&over)).unwrap(),
            over
        );
    }
}

#[test]
fn the_worker_doors_refuse_invalid_bytes_with_the_offset_and_the_kind() {
    for (bytes, offset, kind) in shapes::invalid() {
        let named = format!("invalid BEEF at byte {offset}: {kind}");
        let error = queue::decode_replay_beef(&STANDARD.encode(&bytes))
            .expect_err("the queue door read invalid bytes");
        assert!(error.contains(&named), "{named}: {error}");
        let error = match ef::beef_to_ef_batch(&bytes) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("the EF door read invalid bytes"),
        };
        assert!(error.contains(&named), "{named}: {error}");
        assert!(dead_letters::subject_of(&message(&bytes)).is_none());
        assert!(ef::proven_subject_raw(&bytes).is_none());
    }
}

/// The ARC callback's JSON body is not a BEEF: its 1 MiB bound stays.
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
