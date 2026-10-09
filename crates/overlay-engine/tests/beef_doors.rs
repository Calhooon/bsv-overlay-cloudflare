//! P0-5f: honest-shaped witnesses, one at each bound and one over.
use bsv_overlay_engine::beef_limits::*;
use bsv_overlay_engine::engine::{Engine, EngineConfig, EngineError};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::types::{SubmitMode, TaggedBEEF};
use bsv_rs::transaction::{Beef, MerklePath};
use std::collections::HashMap;

#[path = "support/beef_doors.rs"]
mod shapes;

macro_rules! door {
    ($name:ident, $policy:ident) => {
        mod $name {
            use super::*;
            #[test]
            fn transactions_at_and_one_over() {
                let (at, _) = shapes::transactions($policy.max_txs);
                assert_eq!(
                    parse_beef(&at, &$policy).unwrap().txs.len(),
                    $policy.max_txs
                );
                let (over, _) = shapes::transactions($policy.max_txs + 1);
                assert!(parse_beef(&over, &$policy)
                    .err()
                    .expect("one extra transaction was admitted")
                    .to_string()
                    .contains("max_txs"));
            }
            #[test]
            fn bumps_at_and_one_over() {
                let (at, _) = shapes::bumps($policy.max_bumps);
                assert_eq!(
                    parse_beef(&at, &$policy).unwrap().bumps.len(),
                    $policy.max_bumps
                );
                let (over, _) = shapes::bumps($policy.max_bumps + 1);
                assert!(parse_beef(&over, &$policy)
                    .err()
                    .expect("one extra proof was admitted")
                    .to_string()
                    .contains("max_bumps"));
            }
            #[test]
            fn bytes_at_and_one_over() {
                let (at, _) = shapes::sized_body($policy.max_bytes);
                assert!(parse_beef(&at, &$policy).is_ok());
                let (over, _) = shapes::sized_body($policy.max_bytes + 1);
                assert!(parse_beef(&over, &$policy)
                    .err()
                    .expect("one extra byte was admitted")
                    .to_string()
                    .contains("max_bytes"));
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

#[tokio::test]
async fn actual_core_submit_refuses_counts_even_without_spv() {
    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );
    for (bytes, _) in [shapes::transactions(513), shapes::bumps(513)] {
        let result = engine
            .submit(
                &TaggedBEEF::new(bytes, vec![]),
                SubmitMode::HistoricalTxNoSpv,
            )
            .await;
        assert!(
            matches!(result, Err(EngineError::BeefParseError(ref e)) if e.contains("max_")),
            "{result:?}"
        );
    }
}

#[test]
fn actual_stored_stitch_refuses_counts() {
    for (bytes, id) in [shapes::transactions(513), shapes::bumps(513)] {
        let proof = MerklePath::from_coinbase_txid(&id, 800_000);
        assert!(Engine::stitch_proof_into_stored_beef(&bytes, &id, &proof).is_none());
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

#[test]
fn proof_reader_checks_size_before_hex_decode() {
    let (_, id) = shapes::body(1);
    let proof = MerklePath::from_coinbase_txid(&id, 800_000);
    let hex = proof.to_hex();
    let bytes = hex.len() / 2;
    assert_eq!(merkle_path_from_hex(&hex, bytes).unwrap().to_hex(), hex);
    assert!(merkle_path_from_hex(&hex, bytes - 1)
        .expect_err("one extra proof byte admitted")
        .to_string()
        .contains("max_bytes"));
    for cap in [
        COURIER_PROOF_MAX_BYTES,
        PUSH_PROOF_MAX_BYTES,
        STORED_PROOF_MAX_BYTES,
        PEER_PROOF_MAX_BYTES,
    ] {
        assert!(merkle_path_from_hex(&hex, cap).is_ok());
        assert!(check_size(cap * 2, cap * 2, "proof hex").is_ok());
        let invalid_hex = "z".repeat((cap + 1) * 2);
        assert!(merkle_path_from_hex(&invalid_hex, cap)
            .err()
            .unwrap()
            .to_string()
            .contains("max_bytes"));
    }
}

#[test]
fn production_proof_fields_at_and_one_over() {
    let (at, over) = shapes::proof_boundaries();
    for cap in [
        COURIER_PROOF_MAX_BYTES,
        PUSH_PROOF_MAX_BYTES,
        STORED_PROOF_MAX_BYTES,
        PEER_PROOF_MAX_BYTES,
    ] {
        assert_eq!(at.to_binary().len(), cap);
        assert_eq!(over.to_binary().len(), cap + 1);
        assert_eq!(
            merkle_path_from_hex(&at.to_hex(), cap).unwrap().to_hex(),
            at.to_hex()
        );
        let error = match merkle_path_from_hex(&over.to_hex(), cap) {
            Err(error) => error,
            Ok(_) => panic!("one extra proof byte admitted"),
        };
        assert!(error.to_string().contains("max_bytes"));
    }
}

#[test]
fn atomic_header_allowance_is_exact() {
    let (plain, id) = shapes::sized_body(SUBMIT_BODY_MAX_BYTES);
    let mut beef = Beef::from_binary(&plain).unwrap();
    let atomic = beef.to_binary_atomic(&id).unwrap();
    assert_eq!(atomic.len(), ENGINE_BEEF_LIMITS.max_bytes);
    assert!(transaction_from_beef(&atomic, None, &ENGINE_BEEF_LIMITS).is_ok());
    assert!(parse_beef(&atomic, &SUBMIT_BEEF_LIMITS).is_err());
}

#[test]
fn bounded_linking_and_source_debug_preserve_the_subject() {
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
            let bounded = transaction_from_beef(bytes, target, &ENGINE_BEEF_LIMITS).unwrap();
            assert_eq!(bounded.to_hex(), expected.to_hex());
            assert_eq!(bounded.id(), expected.id());
            if bounded.id() == id {
                assert_eq!(
                    bounded.inputs[0].source_transaction.as_ref().unwrap().id(),
                    parent_id
                );
                let debug = format!("{bounded:?}");
                assert!(
                    debug.contains(&format!("source_transaction: Some(\"{parent_id}\")")),
                    "{debug}"
                );
            }
        }
    }
    assert!(transaction_from_beef(&Beef::new().to_binary(), None, &ENGINE_BEEF_LIMITS).is_err());
}
