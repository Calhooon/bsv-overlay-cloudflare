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
                assert_eq!(parse_beef(&at, &$policy).unwrap().txs.len(), $policy.max_txs);
                let (over, _) = shapes::transactions($policy.max_txs + 1);
                assert!(parse_beef(&over, &$policy).err().expect("one extra transaction was admitted").to_string().contains("max_txs"));
            }
            #[test]
            fn bumps_at_and_one_over() {
                let (at, _) = shapes::bumps($policy.max_bumps);
                assert_eq!(parse_beef(&at, &$policy).unwrap().bumps.len(), $policy.max_bumps);
                let (over, _) = shapes::bumps($policy.max_bumps + 1);
                assert!(parse_beef(&over, &$policy).err().expect("one extra proof was admitted").to_string().contains("max_bumps"));
            }
            #[test]
            fn bytes_at_and_one_over() {
                let (at, _) = shapes::sized_body($policy.max_bytes);
                assert!(parse_beef(&at, &$policy).is_ok());
                let (over, _) = shapes::sized_body($policy.max_bytes + 1);
                assert!(parse_beef(&over, &$policy).err().expect("one extra byte was admitted").to_string().contains("max_bytes"));
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
    let engine = Engine::new(HashMap::new(), HashMap::new(), Box::new(MemoryStorage::new()), None, EngineConfig::default());
    for (bytes, _) in [shapes::transactions(257), shapes::bumps(257)] {
        let result = engine.submit(&TaggedBEEF::new(bytes, vec![]), SubmitMode::HistoricalTxNoSpv).await;
        assert!(matches!(result, Err(EngineError::BeefParseError(ref e)) if e.contains("max_")), "{result:?}");
    }
}

#[test]
fn actual_stored_stitch_refuses_counts() {
    for (bytes, id) in [shapes::transactions(257), shapes::bumps(257)] {
        let proof = MerklePath::from_coinbase_txid(&id, 800_000);
        assert!(Engine::stitch_proof_into_stored_beef(&bytes, &id, &proof).is_none());
    }
}

#[test]
fn target_selection_matches_the_sdk() {
    let (bytes, id) = shapes::body(1);
    let mut beef = Beef::from_binary(&bytes).unwrap();
    let atomic = beef.to_binary_atomic(&id).unwrap();
    assert_eq!(transaction_from_beef(&atomic, None, &ENGINE_BEEF_LIMITS).unwrap().id(), id);
    assert!(transaction_from_beef(&atomic, Some(&"ff".repeat(32)), &ENGINE_BEEF_LIMITS).is_err());
}
