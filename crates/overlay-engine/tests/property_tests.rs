//! Property-based tests using proptest.
//!
//! Verifies serde roundtrips, Outpoint invariants, and MemoryStorage invariants.

use overlay_engine::types::*;
use proptest::prelude::*;

// ============================================================================
// Strategies for generating arbitrary types
// ============================================================================

fn arb_outpoint() -> impl Strategy<Value = Outpoint> {
    ("[a-f0-9]{64}", 0u32..100).prop_map(|(txid, oi)| Outpoint::new(txid, oi))
}

fn arb_output() -> impl Strategy<Value = Output> {
    (
        "[a-f0-9]{64}",
        0u32..10,
        prop::collection::vec(any::<u8>(), 0..50),
        0u64..1_000_000,
        "tm_[a-z]{3,10}",
        any::<bool>(),
        prop::option::of(0u32..900_000),
        prop::option::of(0.0f64..1_000_000.0),
    )
        .prop_map(|(txid, oi, script, sats, topic, spent, bh, score)| Output {
            txid,
            output_index: oi,
            output_script: script,
            satoshis: sats,
            topic,
            spent,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: None,
            block_height: bh,
            score,
        })
}

fn arb_advertisement() -> impl Strategy<Value = Advertisement> {
    (
        prop::bool::ANY,
        "[a-f0-9]{66}",
        "https://[a-z]{3,10}\\.com",
        "(tm|ls)_[a-z]{3,10}",
    )
        .prop_map(|(is_ship, key, domain, tos)| Advertisement {
            protocol: if is_ship {
                Protocol::Ship
            } else {
                Protocol::Slap
            },
            identity_key: key,
            domain,
            topic_or_service: tos,
            beef: None,
            output_index: None,
        })
}

fn arb_gasp_initial_request() -> impl Strategy<Value = GASPInitialRequest> {
    (1u32..5, 0u64..1_000_000, prop::option::of(1u64..100_000)).prop_map(
        |(version, since, limit)| GASPInitialRequest {
            version,
            since,
            limit,
        },
    )
}

fn arb_gasp_node() -> impl Strategy<Value = GASPNode> {
    (
        "[a-f0-9]{64}\\.[0-9]{1,2}",
        "[a-f0-9]{20,100}",
        0u32..10,
        prop::option::of("[a-f0-9]{20,50}"),
    )
        .prop_map(|(gid, raw, oi, proof)| GASPNode {
            graph_id: gid,
            raw_tx: raw,
            output_index: oi,
            proof,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        })
}

fn arb_utxo_reference() -> impl Strategy<Value = UTXOReference> {
    ("[a-f0-9]{64}", 0u32..100).prop_map(|(txid, oi)| UTXOReference {
        txid,
        output_index: oi,
    })
}

// ============================================================================
// Serde roundtrip property tests
// ============================================================================

proptest! {
    #[test]
    fn outpoint_serde_roundtrip(op in arb_outpoint()) {
        let json = serde_json::to_string(&op).unwrap();
        let back: Outpoint = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back.txid, op.txid);
        prop_assert_eq!(back.output_index, op.output_index);
    }

    #[test]
    fn outpoint_graph_id_roundtrip(op in arb_outpoint()) {
        let graph_id = op.to_graph_id();
        let back = Outpoint::from_graph_id(&graph_id).unwrap();
        prop_assert_eq!(back, op);
    }

    #[test]
    fn output_serde_roundtrip(output in arb_output()) {
        let json = serde_json::to_string(&output).unwrap();
        let back: Output = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back.txid, output.txid);
        prop_assert_eq!(back.output_index, output.output_index);
        prop_assert_eq!(back.satoshis, output.satoshis);
        prop_assert_eq!(back.topic, output.topic);
        prop_assert_eq!(back.spent, output.spent);
        prop_assert_eq!(back.block_height, output.block_height);
    }

    #[test]
    fn advertisement_serde_roundtrip(ad in arb_advertisement()) {
        let json = serde_json::to_string(&ad).unwrap();
        let back: Advertisement = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back.protocol, ad.protocol);
        prop_assert_eq!(back.identity_key, ad.identity_key);
        prop_assert_eq!(back.domain, ad.domain);
        prop_assert_eq!(back.topic_or_service, ad.topic_or_service);
    }

    #[test]
    fn gasp_initial_request_serde_roundtrip(req in arb_gasp_initial_request()) {
        let json = serde_json::to_string(&req).unwrap();
        let back: GASPInitialRequest = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back.version, req.version);
        prop_assert_eq!(back.since, req.since);
        prop_assert_eq!(back.limit, req.limit);
    }

    #[test]
    fn gasp_node_serde_roundtrip(node in arb_gasp_node()) {
        let json = serde_json::to_string(&node).unwrap();
        let back: GASPNode = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back.graph_id, node.graph_id);
        prop_assert_eq!(back.raw_tx, node.raw_tx);
        prop_assert_eq!(back.output_index, node.output_index);
        prop_assert_eq!(back.proof, node.proof);
    }

    #[test]
    fn utxo_reference_serde_roundtrip(r in arb_utxo_reference()) {
        let json = serde_json::to_string(&r).unwrap();
        let back: UTXOReference = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back.txid, r.txid);
        prop_assert_eq!(back.output_index, r.output_index);
    }
}

// ============================================================================
// Enum serde roundtrips
// ============================================================================

proptest! {
    #[test]
    fn submit_mode_serde_roundtrip(mode in prop::sample::select(vec![
        SubmitMode::CurrentTx,
        SubmitMode::HistoricalTx,
        SubmitMode::HistoricalTxNoSpv,
    ])) {
        let json = serde_json::to_string(&mode).unwrap();
        let back: SubmitMode = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back, mode);
    }

    #[test]
    fn admission_mode_serde_roundtrip(mode in prop::sample::select(vec![
        AdmissionMode::LockingScript,
        AdmissionMode::WholeTx,
    ])) {
        let json = serde_json::to_string(&mode).unwrap();
        let back: AdmissionMode = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back, mode);
    }

    #[test]
    fn spend_notification_mode_serde_roundtrip(mode in prop::sample::select(vec![
        SpendNotificationMode::None,
        SpendNotificationMode::Txid,
        SpendNotificationMode::Script,
        SpendNotificationMode::WholeTx,
    ])) {
        let json = serde_json::to_string(&mode).unwrap();
        let back: SpendNotificationMode = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back, mode);
    }

    #[test]
    fn protocol_serde_roundtrip(proto in prop::sample::select(vec![
        Protocol::Ship,
        Protocol::Slap,
    ])) {
        let json = serde_json::to_string(&proto).unwrap();
        let back: Protocol = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(back, proto);
    }
}

// ============================================================================
// MemoryStorage invariant tests
// ============================================================================

use overlay_engine::storage::memory::MemoryStorage;
use overlay_engine::storage::Storage;

fn make_test_output(txid: &str, index: u32, topic: &str) -> Output {
    Output {
        txid: txid.to_string(),
        output_index: index,
        output_script: vec![0x76],
        satoshis: 1000,
        topic: topic.to_string(),
        spent: false,
        outputs_consumed: vec![],
        consumed_by: vec![],
        beef: Some(vec![0xBE, 0xEF]),
        block_height: None,
        score: Some(1.0),
    }
}

proptest! {
    #[test]
    fn storage_insert_n_outputs_count_matches(n in 1usize..20) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStorage::new();
            for i in 0..n {
                let output = make_test_output(&format!("tx{i:04}"), 0, "tm_test");
                store.insert_output(&output).await.unwrap();
            }
            prop_assert_eq!(store.output_count(), n);
            Ok(())
        })?;
    }

    #[test]
    fn storage_insert_then_delete_restores_count(n in 1usize..10) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStorage::new();
            for i in 0..n {
                let output = make_test_output(&format!("tx{i:04}"), 0, "tm_test");
                store.insert_output(&output).await.unwrap();
            }
            for i in 0..n {
                store.delete_output(&format!("tx{i:04}"), 0, "tm_test").await.unwrap();
            }
            prop_assert_eq!(store.output_count(), 0);
            Ok(())
        })?;
    }

    #[test]
    fn storage_mark_spent_then_find_with_spent_true(txid_suffix in 0u32..1000) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let txid = format!("tx{txid_suffix:08}");
            let store = MemoryStorage::new();
            store.insert_output(&make_test_output(&txid, 0, "tm_test")).await.unwrap();
            store.mark_utxo_as_spent(&txid, 0, "tm_test").await.unwrap();

            let found = store.find_output(&txid, 0, Some("tm_test"), Some(true), false).await.unwrap();
            prop_assert!(found.is_some());

            let not_found = store.find_output(&txid, 0, Some("tm_test"), Some(false), false).await.unwrap();
            prop_assert!(not_found.is_none());
            Ok(())
        })?;
    }

    #[test]
    fn storage_beef_roundtrip(beef_byte in any::<u8>()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemoryStorage::new();
            let mut output = make_test_output("txbeef", 0, "tm_test");
            output.beef = Some(vec![beef_byte, beef_byte, beef_byte]);
            store.insert_output(&output).await.unwrap();

            let found = store.find_output("txbeef", 0, Some("tm_test"), None, true).await.unwrap().unwrap();
            prop_assert_eq!(found.beef.unwrap(), vec![beef_byte, beef_byte, beef_byte]);
            Ok(())
        })?;
    }
}
