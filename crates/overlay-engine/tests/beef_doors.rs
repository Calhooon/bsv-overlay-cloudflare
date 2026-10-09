//! NL-6: the P0-5f door witnesses, inverted (the charter "a BEEF of any
//! size"). A valid BEEF over each former budget is read at every door; invalid
//! bytes are refused with the offset and the kind.
use bsv_overlay_engine::beef_limits::*;
use bsv_overlay_engine::engine::{Engine, EngineConfig, EngineError};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::types::{SubmitMode, TaggedBEEF};
use bsv_rs::transaction::{Beef, MerklePath};
use std::collections::HashMap;

#[path = "support/beef_doors.rs"]
mod shapes;

/// The refusal a door gives: `invalid BEEF at byte <offset>: <kind> ...`.
fn names(error: &str, offset: usize, kind: &str) -> bool {
    error.contains(&format!("invalid BEEF at byte {offset}: {kind}"))
}

macro_rules! door {
    ($name:ident, $policy:ident) => {
        mod $name {
            use super::*;
            #[test]
            fn transactions_over_the_former_count_are_read() {
                for count in [$policy.max_txs + 1, 4 * $policy.max_txs] {
                    let (over, _) = shapes::transactions(count);
                    assert_eq!(parse_beef(&over, &$policy).unwrap().txs.len(), count);
                }
            }
            #[test]
            fn bumps_over_the_former_count_are_read() {
                for count in [$policy.max_bumps + 1, 4 * $policy.max_bumps] {
                    let (over, _) = shapes::bumps(count);
                    assert_eq!(parse_beef(&over, &$policy).unwrap().bumps.len(), count);
                }
            }
            #[test]
            fn bytes_over_the_former_budget_are_read() {
                let (over, id) = shapes::sized_body($policy.max_bytes + 1);
                assert!(parse_beef(&over, &$policy).is_ok());
                assert_eq!(
                    transaction_from_beef(&over, None, &$policy).unwrap().id(),
                    id
                );
            }
            #[test]
            fn invalid_bytes_are_refused_with_the_offset_and_the_kind() {
                for (bytes, offset, kind) in shapes::invalid() {
                    let error = parse_beef(&bytes, &$policy)
                        .err()
                        .expect("invalid bytes were read")
                        .to_string();
                    assert!(names(&error, offset, kind), "{offset} {kind}: {error}");
                    assert!(!error.contains("max_"), "{error}");
                }
            }
        }
    };
}
door!(submit, SUBMIT_BEEF_LIMITS);
door!(engine, ENGINE_BEEF_LIMITS);
door!(ef, EF_BEEF_LIMITS);
door!(stored, STORED_BEEF_LIMITS);
door!(peer, PEER_BEEF_LIMITS);
door!(discovery, DISCOVERY_BEEF_LIMITS);
door!(app, APP_BEEF_LIMITS);
door!(queue, QUEUE_BEEF_LIMITS);
door!(dead_letter, DEAD_LETTER_BEEF_LIMITS);
door!(census, CENSUS_BEEF_LIMITS);

/// The four former budgets by their numbers, whatever the constants say.
#[test]
fn a_valid_beef_over_each_former_budget_is_read() {
    for (bytes, door) in [
        (10_000_001, SUBMIT_BEEF_LIMITS),
        (12 * 1024 * 1024, SUBMIT_BEEF_LIMITS),
        (2 * 1024 * 1024 + 1, CENSUS_BEEF_LIMITS),
        (90_001, QUEUE_BEEF_LIMITS),
    ] {
        let (body, id) = shapes::sized_body(bytes);
        assert_eq!(
            transaction_from_beef(&body, None, &door).unwrap().id(),
            id,
            "{bytes} bytes"
        );
    }
    assert_eq!(
        parse_beef(&shapes::transactions(513).0, &SUBMIT_BEEF_LIMITS)
            .unwrap()
            .txs
            .len(),
        513
    );
    assert_eq!(
        parse_beef(&shapes::bumps(513).0, &SUBMIT_BEEF_LIMITS)
            .unwrap()
            .bumps
            .len(),
        513
    );
}

#[tokio::test]
async fn actual_core_submit_over_every_former_bound_without_spv() {
    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );
    for over in [
        shapes::transactions(513).0,
        shapes::bumps(513).0,
        shapes::sized_body(ENGINE_BEEF_LIMITS.max_bytes + 1).0,
    ] {
        let result = engine
            .submit(
                &TaggedBEEF::new(over, vec![]),
                SubmitMode::HistoricalTxNoSpv,
            )
            .await;
        assert!(result.is_ok(), "{result:?}");
    }
    for (bytes, offset, kind) in shapes::invalid() {
        let result = engine
            .submit(
                &TaggedBEEF::new(bytes, vec![]),
                SubmitMode::HistoricalTxNoSpv,
            )
            .await;
        assert!(
            matches!(result, Err(EngineError::BeefParseError(ref e)) if names(e, offset, kind)),
            "{offset} {kind}: {result:?}"
        );
    }
}

#[test]
fn actual_stored_stitch_over_every_former_bound() {
    for over in [
        shapes::transactions(513),
        shapes::bumps(513),
        shapes::sized_body(STORED_BEEF_LIMITS.max_bytes + 1),
    ] {
        let proof = MerklePath::from_coinbase_txid(&over.1, 800_000);
        assert!(Engine::stitch_proof_into_stored_beef(&over.0, &over.1, &proof).is_some());
    }
    for (bytes, _, _) in shapes::invalid() {
        let proof = MerklePath::from_coinbase_txid(&"11".repeat(32), 800_000);
        assert!(Engine::stitch_proof_into_stored_beef(&bytes, &"11".repeat(32), &proof).is_none());
    }
}

#[test]
fn target_selection_matches_the_sdk() {
    let (bytes, id) = shapes::body(1);
    let mut beef = Beef::from_binary(&bytes).unwrap();
    let atomic = beef.to_binary_atomic(&id).unwrap();
    assert_eq!(
        transaction_from_beef(&atomic, None, &ENGINE_BEEF_LIMITS)
            .unwrap()
            .id(),
        id
    );
    assert!(transaction_from_beef(&atomic, Some(&"ff".repeat(32)), &ENGINE_BEEF_LIMITS).is_err());
}

/// The courier's and the push's proof field is a wire bound (an 8 KiB JSON
/// hex field), not a BEEF refusal: it stays, checked before the hex decode.
#[test]
fn the_courier_wire_bound_stays_and_is_checked_before_hex_decode() {
    let (_, id) = shapes::body(1);
    let proof = MerklePath::from_coinbase_txid(&id, 800_000);
    let hex = proof.to_hex();
    let bytes = hex.len() / 2;
    assert_eq!(merkle_path_from_hex(&hex, bytes).unwrap().to_hex(), hex);
    assert!(merkle_path_from_hex(&hex, bytes - 1)
        .expect_err("one extra proof byte admitted")
        .to_string()
        .contains("max_bytes"));
    let (at, over) = shapes::proof_boundaries();
    for cap in [COURIER_PROOF_MAX_BYTES, PUSH_PROOF_MAX_BYTES] {
        assert_eq!(at.to_binary().len(), cap);
        assert_eq!(over.to_binary().len(), cap + 1);
        assert_eq!(
            merkle_path_from_hex(&at.to_hex(), cap).unwrap().to_hex(),
            at.to_hex()
        );
        assert!(merkle_path_from_hex(&over.to_hex(), cap)
            .err()
            .expect("one extra proof byte admitted")
            .to_string()
            .contains("max_bytes"));
        let invalid_hex = "z".repeat((cap + 1) * 2);
        assert!(merkle_path_from_hex(&invalid_hex, cap)
            .err()
            .unwrap()
            .to_string()
            .contains("max_bytes"));
    }
}

#[test]
fn the_atomic_form_of_a_body_at_the_former_cap_is_read_at_the_submit_door() {
    let (plain, id) = shapes::sized_body(SUBMIT_BODY_MAX_BYTES);
    let mut beef = Beef::from_binary(&plain).unwrap();
    let atomic = beef.to_binary_atomic(&id).unwrap();
    assert_eq!(atomic.len(), SUBMIT_BODY_MAX_BYTES + ATOMIC_HEADER_BYTES);
    assert!(transaction_from_beef(&atomic, None, &ENGINE_BEEF_LIMITS).is_ok());
    assert!(parse_beef(&atomic, &SUBMIT_BEEF_LIMITS).is_ok());
}

#[test]
fn linking_and_source_debug_preserve_the_subject() {
    use bsv_rs::script::{LockingScript, UnlockingScript};
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
    let (bytes, parent_id) = shapes::body(1);
    let parent = Transaction::from_beef(&bytes, Some(&parent_id)).unwrap();
    let mut child = Transaction::new();
    let mut input = TransactionInput::with_source_transaction(parent, 0);
    input.unlocking_script = Some(UnlockingScript::new());
    child.inputs.push(input);
    child.outputs.push(TransactionOutput::new(
        1,
        LockingScript::from_binary(&[0x51]).unwrap(),
    ));
    let id = child.id();
    let plain = child.to_beef(true).unwrap();
    let mut beef = Beef::from_binary(&plain).unwrap();
    let atomic = beef.to_binary_atomic(&id).unwrap();
    for bytes in [&plain, &atomic] {
        for target in [None, Some(id.as_str()), Some(parent_id.as_str())] {
            let expected = Transaction::from_beef(bytes, target).unwrap();
            let linked = transaction_from_beef(bytes, target, &ENGINE_BEEF_LIMITS).unwrap();
            assert_eq!(linked.to_hex(), expected.to_hex());
            assert_eq!(linked.id(), expected.id());
            if linked.id() == id {
                assert_eq!(
                    linked.inputs[0].source_transaction.as_ref().unwrap().id(),
                    parent_id
                );
                let debug = format!("{linked:?}");
                assert!(
                    debug.contains(&format!("source_transaction: Some(\"{parent_id}\")")),
                    "{debug}"
                );
            }
        }
    }
    assert!(transaction_from_beef(&Beef::new().to_binary(), None, &ENGINE_BEEF_LIMITS).is_err());
}
