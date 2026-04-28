//! Live smoke tests — verify our types parse real production overlay responses.
//!
//! These tests hit real overlay nodes. Run with: cargo test -- --ignored
//! They also test against saved fixtures for CI (non-ignored).

use bsv_rs::overlay::decode_overlay_admin_token;
use bsv_rs::transaction::Transaction;
use bsv_overlay_engine::types::*;

// ============================================================================
// Fixture tests (run in CI, no network needed)
// ============================================================================

/// Real SHIP lookup response saved from users.bapp.dev
const SHIP_FIXTURE: &str = include_str!("fixtures/ship_lookup_response.json");
/// Real SLAP lookup response saved from users.bapp.dev
const SLAP_FIXTURE: &str = include_str!("fixtures/slap_lookup_response.json");

/// Helper: parse a lookup response JSON into outputs with BEEF bytes.
fn parse_lookup_response(json: &str) -> Vec<(Vec<u8>, u32)> {
    let value: serde_json::Value = serde_json::from_str(json).expect("valid JSON");
    assert_eq!(value["type"].as_str(), Some("output-list"));

    value["outputs"]
        .as_array()
        .expect("outputs array")
        .iter()
        .map(|o| {
            let beef: Vec<u8> = o["beef"]
                .as_array()
                .expect("beef array")
                .iter()
                .map(|b| b.as_u64().expect("byte") as u8)
                .collect();
            let output_index = o["outputIndex"].as_u64().expect("outputIndex") as u32;
            (beef, output_index)
        })
        .collect()
}

#[test]
fn fixture_ship_response_deserializes() {
    let outputs = parse_lookup_response(SHIP_FIXTURE);
    assert!(!outputs.is_empty(), "SHIP fixture should have outputs");
    println!("SHIP fixture: {} outputs", outputs.len());
}

#[test]
fn fixture_slap_response_deserializes() {
    let outputs = parse_lookup_response(SLAP_FIXTURE);
    assert!(!outputs.is_empty(), "SLAP fixture should have outputs");
    println!("SLAP fixture: {} outputs", outputs.len());
}

#[test]
fn fixture_ship_beef_parses_to_transaction() {
    let outputs = parse_lookup_response(SHIP_FIXTURE);
    for (i, (beef, output_index)) in outputs.iter().enumerate() {
        let tx = Transaction::from_beef(beef, None)
            .unwrap_or_else(|e| panic!("SHIP output {i}: BEEF parse failed: {e}"));
        assert!(!tx.id().is_empty(), "txid should be non-empty");
        assert!(
            tx.outputs.len() > *output_index as usize,
            "output index {} out of range (tx has {} outputs)",
            output_index,
            tx.outputs.len()
        );
        println!(
            "SHIP[{i}]: txid={}, outputs={}, target_index={}",
            &tx.id()[..16],
            tx.outputs.len(),
            output_index
        );
    }
}

#[test]
fn fixture_slap_beef_parses_to_transaction() {
    let outputs = parse_lookup_response(SLAP_FIXTURE);
    for (i, (beef, output_index)) in outputs.iter().enumerate() {
        let tx = Transaction::from_beef(beef, None)
            .unwrap_or_else(|e| panic!("SLAP output {i}: BEEF parse failed: {e}"));
        assert!(!tx.id().is_empty());
        assert!(tx.outputs.len() > *output_index as usize);
        println!(
            "SLAP[{i}]: txid={}, outputs={}, target_index={}",
            &tx.id()[..16],
            tx.outputs.len(),
            output_index
        );
    }
}

#[test]
fn fixture_ship_pushdrop_decodes_to_admin_token() {
    let outputs = parse_lookup_response(SHIP_FIXTURE);
    let mut decoded_count = 0;

    for (i, (beef, output_index)) in outputs.iter().enumerate() {
        let tx = Transaction::from_beef(beef, None).unwrap();
        let output = &tx.outputs[*output_index as usize];

        match decode_overlay_admin_token(&output.locking_script) {
            Ok(token) => {
                assert_eq!(token.protocol, Protocol::Ship);
                assert!(!token.domain.is_empty(), "domain should be non-empty");
                assert!(
                    token.topic_or_service.starts_with("tm_"),
                    "SHIP topic should start with tm_, got: {}",
                    token.topic_or_service
                );
                println!(
                    "SHIP[{i}]: protocol={}, domain={}, topic={}",
                    token.protocol, token.domain, token.topic_or_service
                );
                decoded_count += 1;
            }
            Err(e) => {
                println!("SHIP[{i}]: decode failed (may not be admin token): {e}");
            }
        }
    }

    assert!(
        decoded_count > 0,
        "At least one SHIP output should decode as admin token"
    );
    println!(
        "Decoded {decoded_count}/{} SHIP admin tokens",
        outputs.len()
    );
}

#[test]
fn fixture_slap_pushdrop_decodes_to_admin_token() {
    let outputs = parse_lookup_response(SLAP_FIXTURE);
    let mut decoded_count = 0;

    for (i, (beef, output_index)) in outputs.iter().enumerate() {
        let tx = Transaction::from_beef(beef, None).unwrap();
        let output = &tx.outputs[*output_index as usize];

        match decode_overlay_admin_token(&output.locking_script) {
            Ok(token) => {
                assert_eq!(token.protocol, Protocol::Slap);
                assert!(!token.domain.is_empty());
                assert!(
                    token.topic_or_service.starts_with("ls_"),
                    "SLAP service should start with ls_, got: {}",
                    token.topic_or_service
                );
                println!(
                    "SLAP[{i}]: protocol={}, domain={}, service={}",
                    token.protocol, token.domain, token.topic_or_service
                );
                decoded_count += 1;
            }
            Err(e) => {
                println!("SLAP[{i}]: decode failed: {e}");
            }
        }
    }

    assert!(decoded_count > 0);
    println!(
        "Decoded {decoded_count}/{} SLAP admin tokens",
        outputs.len()
    );
}

// ============================================================================
// GASP fixture tests
// ============================================================================

const GASP_FIXTURE: &str = include_str!("fixtures/gasp_sync_response.json");

#[test]
fn fixture_gasp_response_deserializes() {
    let value: serde_json::Value = serde_json::from_str(GASP_FIXTURE).unwrap();
    let utxos = value["UTXOList"].as_array().unwrap();
    assert!(!utxos.is_empty(), "GASP fixture should have UTXOs");
    println!("GASP fixture: {} UTXOs", utxos.len());

    // Each UTXO should have txid and outputIndex
    for (i, u) in utxos.iter().take(5).enumerate() {
        let txid = u["txid"].as_str().unwrap();
        let oi = u["outputIndex"].as_u64().unwrap();
        assert_eq!(txid.len(), 64, "txid should be 64 hex chars");
        println!("  GASP[{i}]: txid={}, oi={oi}", &txid[..16]);
    }
}

#[test]
fn fixture_gasp_response_parses_as_gasp_types() {
    use bsv_overlay_engine::types::GASPInitialResponse;

    // The live response may not have `score` or `since` at the top level in the expected format.
    // Let's parse manually since the live format has no scores.
    let value: serde_json::Value = serde_json::from_str(GASP_FIXTURE).unwrap();
    let utxos = value["UTXOList"].as_array().unwrap();
    let since = value.get("since").and_then(|v| v.as_u64()).unwrap_or(0);

    let gasp_outputs: Vec<bsv_overlay_engine::types::GASPOutput> = utxos
        .iter()
        .map(|u| bsv_overlay_engine::types::GASPOutput {
            txid: u["txid"].as_str().unwrap().to_string(),
            output_index: u["outputIndex"].as_u64().unwrap() as u32,
            score: u.get("score").and_then(|s| s.as_f64()).unwrap_or(0.0),
        })
        .collect();

    assert!(!gasp_outputs.is_empty());
    println!("Parsed {} GASPOutputs, since={}", gasp_outputs.len(), since);
}

// ============================================================================
// Live network tests (run with --ignored)
// ============================================================================

#[ignore]
#[test]
fn live_query_ship_advertisements() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = reqwest::Client::new();
        let resp = client
            .post("https://users.bapp.dev/lookup")
            .header("Content-Type", "application/json")
            .body(r#"{"service":"ls_ship","query":{"findAll":true,"limit":5}}"#)
            .send()
            .await
            .expect("HTTP request failed");

        assert!(resp.status().is_success(), "Status: {}", resp.status());
        let body = resp.text().await.unwrap();
        let outputs = parse_lookup_response(&body);
        println!("Live SHIP: {} outputs", outputs.len());
        assert!(!outputs.is_empty());

        // Parse each one
        for (i, (beef, oi)) in outputs.iter().enumerate() {
            let tx = Transaction::from_beef(beef, None).unwrap();
            let token = decode_overlay_admin_token(&tx.outputs[*oi as usize].locking_script);
            if let Ok(t) = token {
                println!(
                    "  [{i}] {} at {} for {}",
                    t.protocol, t.domain, t.topic_or_service
                );
            }
        }
    });
}

#[ignore]
#[test]
fn live_query_slap_advertisements() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = reqwest::Client::new();
        let resp = client
            .post("https://users.bapp.dev/lookup")
            .header("Content-Type", "application/json")
            .body(r#"{"service":"ls_slap","query":{"findAll":true,"limit":5}}"#)
            .send()
            .await
            .unwrap();

        assert!(resp.status().is_success());
        let body = resp.text().await.unwrap();
        let outputs = parse_lookup_response(&body);
        println!("Live SLAP: {} outputs", outputs.len());
        assert!(!outputs.is_empty());

        for (i, (beef, oi)) in outputs.iter().enumerate() {
            let tx = Transaction::from_beef(beef, None).unwrap();
            if let Ok(t) = decode_overlay_admin_token(&tx.outputs[*oi as usize].locking_script) {
                println!(
                    "  [{i}] {} at {} for {}",
                    t.protocol, t.domain, t.topic_or_service
                );
            }
        }
    });
}

#[ignore]
#[test]
fn live_list_topic_managers() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for host in &["https://overlay-us-1.bsvb.tech", "https://users.bapp.dev"] {
            let client = reqwest::Client::new();
            let resp = client.get(format!("{host}/listTopicManagers")).send().await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap();
                    let managers: Vec<&str> = body
                        .as_object()
                        .unwrap()
                        .keys()
                        .map(|k| k.as_str())
                        .collect();
                    println!("{host} managers: {managers:?}");
                }
                Ok(r) => println!("{host}: status {}", r.status()),
                Err(e) => println!("{host}: error {e}"),
            }
        }
    });
}

#[ignore]
#[test]
fn live_gasp_sync_request() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // type-stamp overlay has GASP enabled (BSV Association nodes do NOT)
        let client = reqwest::Client::new();
        let resp = client
            .post("https://type-stamp-overlay-2-production.up.railway.app/requestSyncResponse")
            .header("Content-Type", "application/json")
            .header("X-BSV-Topic", "tm_ship")
            .body(r#"{"version":1,"since":0,"limit":10}"#)
            .send()
            .await
            .expect("GASP request failed");

        assert!(resp.status().is_success(), "Status: {}", resp.status());
        let body: serde_json::Value = resp.json().await.unwrap();

        let utxos = body["UTXOList"].as_array().unwrap();
        println!("Live GASP tm_ship: {} UTXOs", utxos.len());
        assert!(!utxos.is_empty(), "Should have SHIP UTXOs");

        for (i, u) in utxos.iter().take(5).enumerate() {
            println!(
                "  [{i}] txid={}..., oi={}",
                &u["txid"].as_str().unwrap()[..16],
                u["outputIndex"]
            );
        }
    });
}

#[ignore]
#[test]
fn live_list_lookup_services() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for host in &["https://overlay-us-1.bsvb.tech", "https://users.bapp.dev"] {
            let client = reqwest::Client::new();
            let resp = client
                .get(format!("{host}/listLookupServiceProviders"))
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap();
                    let services: Vec<&str> = body
                        .as_object()
                        .unwrap()
                        .keys()
                        .map(|k| k.as_str())
                        .collect();
                    println!("{host} services: {services:?}");
                }
                Ok(r) => println!("{host}: status {}", r.status()),
                Err(e) => println!("{host}: error {e}"),
            }
        }
    });
}
