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
        r2: None,
        topics: vec!["tm_test".into()],
        mode: "historical-tx".into(),
        reason: "boundary witness".into(),
        redrive: None,
        ef_job: None,
    }
}

/// A BEEF and its subject's txid.
type Shape = (Vec<u8>, String);

/// One shape at each former bound (read then, read now) and one over it.
fn at_and_over(door: &BeefLimits) -> [(Shape, Shape); 3] {
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

/// The stored-BEEF readers that moved onto streaming folds (NL-6): the
/// subject's OWN bump of a stored body over each former bound is read as the
/// in-memory index reads it, and the funding body no longer has a byte bound.
#[test]
fn stored_readers_over_every_former_bound() {
    use bsv_rs::transaction::Beef;
    for (_, (over, id)) in at_and_over(&STORED_BEEF_LIMITS) {
        let beef = Beef::from_binary(&over).unwrap();
        let entry = beef.find_txid(&id).unwrap();
        let own = &beef.bumps[entry.bump_index().unwrap()];
        assert_eq!(
            crate::reorg_sweep::stored_bump_hex(&over, &id),
            Some(own.to_hex())
        );
        assert_eq!(
            crate::proof_fetcher::own_bump_hex(&over, &id),
            Some(own.to_hex())
        );
        let anchor = crate::reorg_sweep::stored_bump_anchor(&over, &id).unwrap();
        assert_eq!(anchor.height, 800_000);
        assert_eq!(anchor.root, own.compute_root(Some(&id)).unwrap());
        assert!(crate::d1_storage::D1Storage::beef_has_proof(&id, &over));
        assert!(crate::proof_fetcher::funding_output_script(&over, &id, 0).is_some());
    }
    for (bytes, _, _) in shapes::invalid() {
        let id = "11".repeat(32);
        assert!(crate::reorg_sweep::stored_bump_hex(&bytes, &id).is_none());
        assert!(crate::reorg_sweep::stored_bump_anchor(&bytes, &id).is_none());
        assert!(!crate::d1_storage::D1Storage::beef_has_proof(&id, &bytes));
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

/// bsv-low #585 (door 3): a VALID 500 KB BEEF past the queue's room is carried by key and named by its SUBJECT (the
/// letter's key in both consumers, no bytes read); the object's bytes pass the replay's check exactly when the
/// consumer's policy admits them (`QUEUE_BEEF_LIMITS`, NL-6's: no cap of the R2 path's own). A body that policy
/// admits today, sent by key under a lowered room, passes it now, and its subject read from the object is the
/// message's.
#[test]
fn e585_d3_a_valid_500kb_beef_is_keyed_by_its_subject_and_replays_under_the_consumers_policy() {
    use overlay_engine::types::SubmitMode;
    let topics = vec!["tm_test".to_string()];
    let (beef, id) = shapes::sized_body(500_000);
    let plan = queue::plan_replay(
        &beef,
        &topics,
        SubmitMode::HistoricalTx,
        queue::REPLAY_REASON_PHASE3_FAULT,
        queue::QUEUE_MESSAGE_ROOM,
        &SUBMIT_BEEF_LIMITS,
    )
    .unwrap();
    let queue::Carriage::R2(msg) = plan else {
        panic!("500 KB rides by key")
    };
    let r = msg.r2.clone().unwrap();
    assert_eq!(r.txid.as_deref(), Some(id.as_str()));
    assert_eq!(
        dead_letters::letter_key(&msg, None),
        (id.clone(), "tm_test".to_string())
    );
    queue::check_blob(&r, &beef).unwrap();
    let replay = queue::check_replay_blob(&r, &beef);
    if QUEUE_BEEF_LIMITS.max_bytes >= beef.len() {
        replay.unwrap();
    } else {
        assert!(replay.unwrap_err().contains("max_bytes"));
    }

    let (small, small_id) = shapes::body(8_000);
    let plan = queue::plan_replay(
        &small,
        &topics,
        SubmitMode::HistoricalTx,
        queue::REPLAY_REASON_PHASE3_FAULT,
        queue::QUEUE_MESSAGE_ROOM_MIN,
        &QUEUE_BEEF_LIMITS,
    )
    .unwrap();
    let queue::Carriage::R2(msg) = plan else {
        panic!("8 KB rides by key under a 1 KB room")
    };
    let r = msg.r2.clone().unwrap();
    queue::check_replay_blob(&r, &small).unwrap();
    let mut named = beef_limits::parse_beef(&small, &QUEUE_BEEF_LIMITS).unwrap();
    assert_eq!(ef::subject_txid_of(&mut named), Some(small_id.clone()));
    assert_eq!(r.txid, Some(small_id));
    // the same body under the default room is inline: the door changed nothing below it
    assert!(matches!(
        queue::plan_replay(
            &small,
            &topics,
            SubmitMode::HistoricalTx,
            queue::REPLAY_REASON_PHASE3_FAULT,
            queue::QUEUE_MESSAGE_ROOM,
            &QUEUE_BEEF_LIMITS
        ),
        Ok(queue::Carriage::Inline(_))
    ));
}
