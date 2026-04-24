//! End-to-end tests for the Agent Registry (tm_agent / ls_agent).
//!
//! These tests create real AGENT PushDrop tokens using a live wallet,
//! submit them to the deployed overlay worker, verify the full cycle,
//! and print test vectors that the worm (rust-bsv-worm) can consume.
//!
//! # Prerequisites
//! - Wallet running on localhost:3322 in auto-approve mode
//!   (identity key: 034aa44668fbc73ca5d490f0fa54b98b398b790856d8c55d540759ccefa5e6d0ce)
//! - Overlay worker deployed at <your-overlay>.workers.dev
//!
//! # Running
//! ```bash
//! cargo test --test agent_e2e -- --ignored --nocapture
//! ```
//!
//! # Budget
//! Each AGENT token costs ~1 sat + ~200 sats fee. Budget: <20,000 sats total.
//! These tests create at most 8 transactions (~1,600 sats).

use bsv_rs::primitives::ec::PublicKey;
use bsv_rs::script::templates::PushDrop;

const WALLET_URL: &str = "http://localhost:3322";
const OVERLAY_URL: &str = "https://<your-overlay>.workers.dev";

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

/// Helper: convert bytes to hex string.
fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Helper: convert hex string to bytes.
fn from_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

/// Call the wallet REST API with retry logic for transient SQLite lock errors.
async fn wallet_call(method: &str, body: &serde_json::Value) -> serde_json::Value {
    let max_retries = 5;
    for attempt in 0..=max_retries {
        let resp = client()
            .post(format!("{WALLET_URL}/{method}"))
            .header("Content-Type", "application/json")
            .header("Origin", "http://localhost")
            .json(body)
            .send()
            .await
            .unwrap_or_else(|e| panic!("Wallet call {method} failed: {e}"));

        let status = resp.status();
        let text = resp.text().await.unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&text)
            .unwrap_or_else(|_| panic!("Wallet {method} returned non-JSON: {text}"));

        if status.is_success() {
            return parsed;
        }

        // Retry on transient "database is locked" errors
        let msg = parsed["message"].as_str().unwrap_or("");
        if msg.contains("database is locked") && attempt < max_retries {
            println!(
                "  Wallet {method} returned database-locked, retrying ({}/{})",
                attempt + 1,
                max_retries
            );
            tokio::time::sleep(std::time::Duration::from_millis(500 * (attempt as u64 + 1))).await;
            continue;
        }

        panic!("Wallet {method} returned {status}: {parsed}");
    }
    unreachable!()
}

/// Get the wallet's identity key as hex string.
async fn get_identity_key() -> String {
    let resp = wallet_call("getPublicKey", &serde_json::json!({"identityKey": true})).await;
    resp["publicKey"]
        .as_str()
        .expect("publicKey should be a string")
        .to_string()
}

/// Build a 6-field AGENT PushDrop locking script.
///
/// The PushDrop has 6 fields:
/// 1. Protocol tag ("AGENT")
/// 2. Subject identity key (33-byte compressed pubkey)
/// 3. Certifier identity key (33-byte compressed pubkey, same as subject for self-signed)
/// 4. Endpoint URL (UTF-8 string)
/// 5. Capabilities CSV (UTF-8 string, e.g. "tool-use,wallet,messaging")
/// 6. Signature (DER-encoded, from wallet createSignature)
///
/// The locking key is the BRC-42 derived child key (NOT the identity key).
async fn build_agent_locking_script(
    identity_key_hex: &str,
    endpoint: &str,
    capabilities: &[&str],
) -> (Vec<u8>, String) {
    let identity_key_bytes = from_hex(identity_key_hex);

    let capabilities_csv = capabilities.join(",");

    // Build the first 5 fields (protocol, subject, certifier, endpoint, capabilities)
    let unsigned_fields = [
        b"AGENT".to_vec(),
        identity_key_bytes.clone(), // subject
        identity_key_bytes.clone(), // certifier (self-signed)
        endpoint.as_bytes().to_vec(),
        capabilities_csv.as_bytes().to_vec(),
    ];

    // Concatenate all field bytes for signing
    let data_to_sign: Vec<u8> = unsigned_fields
        .iter()
        .flat_map(|f| f.iter().copied())
        .collect();

    // Sign via wallet
    let sig_resp = wallet_call(
        "createSignature",
        &serde_json::json!({
            "data": data_to_sign.iter().map(|b| *b as u64).collect::<Vec<_>>(),
            "protocolID": [2, "agent registry"],
            "keyID": "1",
            "counterparty": "anyone"
        }),
    )
    .await;

    let signature_bytes: Vec<u8> = sig_resp["signature"]
        .as_array()
        .expect("createSignature should return signature array")
        .iter()
        .map(|v| v.as_u64().unwrap() as u8)
        .collect();

    // Get BRC-42 DERIVED locking key (NOT the identity key!)
    let derived_resp = wallet_call(
        "getPublicKey",
        &serde_json::json!({
            "protocolID": [2, "agent registry"],
            "keyID": "1",
            "counterparty": "anyone",
            "forSelf": true
        }),
    )
    .await;

    let locking_key_hex = derived_resp["publicKey"]
        .as_str()
        .expect("getPublicKey should return publicKey");
    let locking_key = PublicKey::from_bytes(&from_hex(locking_key_hex))
        .expect("derived key should be valid pubkey");

    // Build final 6-field PushDrop
    let fields = vec![
        b"AGENT".to_vec(),
        identity_key_bytes.clone(),
        identity_key_bytes,
        endpoint.as_bytes().to_vec(),
        capabilities_csv.as_bytes().to_vec(),
        signature_bytes,
    ];

    let pushdrop = PushDrop::new(locking_key, fields);
    let script_bytes = pushdrop.lock().to_binary();
    let script_hex = to_hex(&script_bytes);

    (script_bytes, script_hex)
}

/// Create a transaction via the wallet with a custom locking script output.
/// Returns (txid, beef_bytes).
async fn create_agent_transaction(
    locking_script_hex: &str,
    description: &str,
) -> (String, Vec<u8>) {
    let body = serde_json::json!({
        "description": description,
        "outputs": [{
            "satoshis": 1,
            "lockingScript": locking_script_hex,
            "outputDescription": "AGENT registration output",
            "basket": "overlay_advertisements",
            "tags": ["overlay-agent-registration"]
        }],
    });

    let resp = wallet_call("createAction", &body).await;

    let txid = resp["txid"]
        .as_str()
        .expect("createAction should return txid")
        .to_string();

    // The tx field is an array of byte values (AtomicBEEF)
    let beef_bytes: Vec<u8> = resp["tx"]
        .as_array()
        .expect("createAction tx should be a byte array")
        .iter()
        .map(|b| {
            b.as_u64()
                .unwrap_or_else(|| panic!("tx byte should be u64, got: {b}")) as u8
        })
        .collect();

    assert!(!beef_bytes.is_empty(), "BEEF bytes should not be empty");
    println!(
        "  Created transaction: txid={txid} beef_len={}",
        beef_bytes.len()
    );

    (txid, beef_bytes)
}

/// Submit BEEF bytes to the overlay worker with the given topics.
async fn submit_to_overlay(beef: &[u8], topics: &[&str]) -> serde_json::Value {
    let topics_json = serde_json::to_string(topics).unwrap();

    let resp = client()
        .post(format!("{OVERLAY_URL}/submit"))
        .header("Content-Type", "application/octet-stream")
        .header("x-topics", &topics_json)
        .body(beef.to_vec())
        .send()
        .await
        .expect("Submit to overlay failed");

    let status = resp.status();
    let body: serde_json::Value = resp.json().await.expect("Submit response should be JSON");

    println!("  Submit response: status={status} body={body}");

    assert!(
        status.is_success(),
        "Submit should succeed (200), got: {status} body: {body}"
    );

    body
}

/// Lookup records from the overlay worker.
async fn lookup_overlay(service: &str, query: serde_json::Value) -> serde_json::Value {
    let body = serde_json::json!({
        "service": service,
        "query": query,
    });

    let resp = client()
        .post(format!("{OVERLAY_URL}/lookup"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .expect("Lookup request failed");

    let status = resp.status();
    let result: serde_json::Value = resp.json().await.expect("Lookup response should be JSON");

    assert!(
        status.is_success(),
        "Lookup should succeed, got: {status} body: {result}"
    );

    result
}

/// Helper: create an AGENT token, submit it, and return all artifacts.
/// Returns (txid, beef_bytes, script_hex, identity_key_hex, endpoint, capabilities_csv).
async fn create_and_submit_agent(
    endpoint: &str,
    capabilities: &[&str],
) -> (String, Vec<u8>, String, String, String) {
    let identity_key = get_identity_key().await;
    let capabilities_csv = capabilities.join(",");

    let (_script_bytes, script_hex) =
        build_agent_locking_script(&identity_key, endpoint, capabilities).await;

    let description = format!("AGENT registration: {endpoint}");
    let (txid, beef) = create_agent_transaction(&script_hex, &description).await;

    let steak = submit_to_overlay(&beef, &["tm_agent"]).await;
    println!("  Submit result: {steak}");

    (txid, beef, script_hex, identity_key, capabilities_csv)
}

/// Print test vectors for the worm (rust-bsv-worm) to consume.
fn print_test_vectors(
    identity_key_hex: &str,
    endpoint: &str,
    capabilities_csv: &str,
    script_hex: &str,
    beef: &[u8],
    txid: &str,
) {
    println!();
    println!("=== AGENT TEST VECTOR ===");
    println!("identity_key: {identity_key_hex}");
    println!("endpoint: {endpoint}");
    println!("capabilities: {capabilities_csv}");
    println!("locking_script_hex: {script_hex}");
    println!("beef_hex: {}", to_hex(beef));
    println!("txid: {txid}");
    println!("=== END AGENT TEST VECTOR ===");
    println!();
}

// =============================================================================
// 1. Create agent token and submit to overlay
// =============================================================================

/// Creates a real AGENT PushDrop token via the wallet, submits it to the overlay
/// with x-topics: ["tm_agent"], and verifies the 200 response with outputsToAdmit.
#[ignore]
#[test]
fn agent_create_and_submit() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        println!("Identity key: {identity_key}");

        let endpoint = "https://<your-overlay>.workers.dev";
        let capabilities = &["overlay-host"];

        // Build AGENT PushDrop locking script
        println!("Step 1: Building AGENT PushDrop locking script...");
        let (script_bytes, script_hex) =
            build_agent_locking_script(&identity_key, endpoint, capabilities).await;
        println!(
            "  Script: {} bytes, hex prefix: {}...",
            script_bytes.len(),
            &script_hex[..80.min(script_hex.len())]
        );

        // Create transaction via wallet
        println!("Step 2: Creating AGENT transaction via wallet...");
        let (txid, beef) =
            create_agent_transaction(&script_hex, "AGENT registration for overlay-host").await;
        println!("  txid: {txid}");

        // Submit to overlay
        println!("Step 3: Submitting to overlay with topic tm_agent...");
        let steak = submit_to_overlay(&beef, &["tm_agent"]).await;

        // Verify response contains tm_agent key
        let tm_agent = steak.get("tm_agent");
        assert!(
            tm_agent.is_some(),
            "Response should contain tm_agent key, got: {steak}"
        );

        let admitted = tm_agent.unwrap()["outputsToAdmit"]
            .as_array()
            .expect("outputsToAdmit should be an array");

        println!(
            "Step 4: Admission result: {} outputs admitted",
            admitted.len()
        );

        if admitted.is_empty() {
            println!("  NOTE: No outputs admitted (likely already exists from a prior run)");
        } else {
            println!("  Admitted output indices: {:?}", admitted);
        }

        // Print test vectors for the worm
        let capabilities_csv = capabilities.join(",");
        print_test_vectors(
            &identity_key,
            endpoint,
            &capabilities_csv,
            &script_hex,
            &beef,
            &txid,
        );

        println!("AGENT create-and-submit complete: txid={txid}");
    });
}

// =============================================================================
// 2. Submit agent then lookup by capability
// =============================================================================

/// Submits an AGENT token, then looks it up by capability via ls_agent.
#[ignore]
#[test]
fn agent_submit_then_lookup_by_capability() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let endpoint = "https://<your-overlay>.workers.dev";
        let capabilities = &["overlay-host"];

        println!("Step 1: Creating and submitting AGENT token...");
        let (txid, _beef, _script_hex, identity_key, _caps_csv) =
            create_and_submit_agent(endpoint, capabilities).await;
        println!("  txid: {txid}");

        // Lookup by capability
        println!("Step 2: Looking up AGENT records by capability...");
        let lookup_result = lookup_overlay(
            "ls_agent",
            serde_json::json!({"findByCapability": "overlay-host"}),
        )
        .await;

        assert_eq!(
            lookup_result["type"], "output-list",
            "Lookup should return output-list, got: {lookup_result}"
        );

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be an array");

        println!(
            "  Lookup returned {} AGENT outputs for capability 'overlay-host'",
            outputs.len()
        );

        assert!(
            !outputs.is_empty(),
            "Should find at least one AGENT output for capability 'overlay-host'"
        );

        // Verify each output has expected fields
        for (i, output) in outputs.iter().enumerate() {
            assert!(
                output.get("beef").is_some(),
                "Output {i} should have 'beef' field"
            );
            assert!(
                output.get("outputIndex").is_some(),
                "Output {i} should have 'outputIndex' field"
            );
        }

        println!(
            "AGENT lookup-by-capability verified: {} outputs, identity_key={identity_key}",
            outputs.len()
        );
    });
}

// =============================================================================
// 3. Submit agent then lookup by identity key
// =============================================================================

/// Submits an AGENT token, then looks it up by the wallet's identity key.
#[ignore]
#[test]
fn agent_submit_then_lookup_by_identity_key() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let endpoint = "https://<your-overlay>.workers.dev";
        let capabilities = &["overlay-host"];

        println!("Step 1: Creating and submitting AGENT token...");
        let (txid, _beef, _script_hex, identity_key, _caps_csv) =
            create_and_submit_agent(endpoint, capabilities).await;
        println!("  txid: {txid}");

        // Lookup by identity key
        println!("Step 2: Looking up AGENT records by identity key...");
        let lookup_result = lookup_overlay(
            "ls_agent",
            serde_json::json!({"findByIdentityKey": identity_key}),
        )
        .await;

        assert_eq!(
            lookup_result["type"], "output-list",
            "Lookup should return output-list, got: {lookup_result}"
        );

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be an array");

        println!(
            "  Lookup returned {} AGENT outputs for identity_key={}",
            outputs.len(),
            &identity_key[..16]
        );

        assert!(
            !outputs.is_empty(),
            "Should find at least one AGENT output for identity key {identity_key}"
        );

        println!(
            "AGENT lookup-by-identity-key verified: {} outputs",
            outputs.len()
        );
    });
}

// =============================================================================
// 4. Submit agent then lookup findAll
// =============================================================================

/// Submits an AGENT token, then uses findAll to list all registered agents.
#[ignore]
#[test]
fn agent_submit_then_lookup_find_all() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let endpoint = "https://<your-overlay>.workers.dev";
        let capabilities = &["overlay-host"];

        println!("Step 1: Creating and submitting AGENT token...");
        let (txid, _beef, _script_hex, _identity_key, _caps_csv) =
            create_and_submit_agent(endpoint, capabilities).await;
        println!("  txid: {txid}");

        // Lookup findAll
        println!("Step 2: Looking up all AGENT records...");
        let lookup_result = lookup_overlay("ls_agent", serde_json::json!({"findAll": true})).await;

        assert_eq!(
            lookup_result["type"], "output-list",
            "Should return output-list, got: {lookup_result}"
        );

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be an array");

        println!("  findAll returned {} AGENT outputs", outputs.len());

        assert!(
            !outputs.is_empty(),
            "findAll should return at least one AGENT output"
        );

        // Verify structure of each output
        for (i, output) in outputs.iter().enumerate() {
            assert!(
                output.get("beef").is_some(),
                "Output {i} should have 'beef'"
            );
            assert!(
                output.get("outputIndex").is_some(),
                "Output {i} should have 'outputIndex'"
            );

            let beef = output["beef"].as_array().expect("beef should be array");
            assert!(!beef.is_empty(), "Output {i} beef should not be empty");
        }

        println!("AGENT findAll verified: {} outputs", outputs.len());
    });
}

// =============================================================================
// 5. Deduplication: submit same agent BEEF twice
// =============================================================================

/// Submits the same AGENT BEEF twice and verifies the second submission
/// is deduplicated (outputsToAdmit is empty).
#[ignore]
#[test]
fn agent_deduplication() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        let endpoint = "https://<your-overlay>.workers.dev";
        let capabilities = &["overlay-host"];

        let (_script_bytes, script_hex) =
            build_agent_locking_script(&identity_key, endpoint, capabilities).await;

        println!("Step 1: Creating AGENT transaction...");
        let (_txid, beef) = create_agent_transaction(&script_hex, "AGENT dedup test").await;

        // First submit
        println!("Step 2: First submit...");
        let steak1 = submit_to_overlay(&beef, &["tm_agent"]).await;
        println!("  First: {steak1}");

        // Wait for the overlay worker to complete async mutations (CF Queue / wait_until).
        // The onSteakReady pattern returns the Steak immediately, but INSERTs into
        // applied_transactions happen asynchronously. Without this delay the second
        // submit may not see the first and will admit the output again.
        // Use 10s to account for load when all E2E tests run concurrently.
        println!("  Waiting 10s for async mutations to complete...");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        // Second submit (same BEEF)
        println!("Step 3: Second submit (same BEEF)...");
        let steak2 = submit_to_overlay(&beef, &["tm_agent"]).await;
        println!("  Second: {steak2}");

        let admitted2 = steak2["tm_agent"]["outputsToAdmit"]
            .as_array()
            .expect("outputsToAdmit should be array");

        assert!(
            admitted2.is_empty(),
            "Second submit should be deduplicated (empty outputsToAdmit), got: {steak2}"
        );

        println!("AGENT deduplication verified: second submit returned empty outputsToAdmit");
    });
}

// =============================================================================
// 6. Agent with multiple capabilities
// =============================================================================

/// Submits an agent with multiple capabilities ("tool-use,wallet,messaging"),
/// then verifies lookup by each capability individually returns the agent.
#[ignore]
#[test]
fn agent_multiple_capabilities() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Use a unique endpoint hostname so the has_duplicate_record check
        // (identity_key + endpoint) does not skip this registration. Prior tests
        // already register the same identity key with the default overlay endpoint
        // and a different capabilities set; reusing that endpoint would cause the
        // lookup service to silently skip the new record as a duplicate.
        // URI validation requires path == "/", so we vary the hostname instead.
        let endpoint = "https://multi-cap-test.dev-a3e.workers.dev";
        let capabilities = &["tool-use", "wallet", "messaging"];

        println!("Step 1: Creating and submitting multi-capability AGENT...");
        let (txid, _beef, _script_hex, identity_key, caps_csv) =
            create_and_submit_agent(endpoint, capabilities).await;
        println!("  txid: {txid}, capabilities: {caps_csv}");

        // Wait for the overlay worker to complete async mutations (CF Queue / wait_until).
        // The onSteakReady pattern returns the Steak immediately, but INSERTs into
        // the lookup tables happen asynchronously. Without this delay the lookup
        // may return 0 results because the data hasn't been written yet.
        println!("  Waiting 5s for async mutations to complete...");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Lookup by each capability individually
        for cap in capabilities {
            println!("Step 2: Looking up AGENT by capability '{cap}'...");
            let lookup_result =
                lookup_overlay("ls_agent", serde_json::json!({"findByCapability": cap})).await;

            assert_eq!(
                lookup_result["type"], "output-list",
                "Lookup for capability '{cap}' should return output-list, got: {lookup_result}"
            );

            let outputs = lookup_result["outputs"]
                .as_array()
                .expect("outputs should be an array");

            println!("  Capability '{cap}': {} outputs found", outputs.len());

            assert!(
                !outputs.is_empty(),
                "Should find at least one AGENT output for capability '{cap}' \
                 (identity_key={identity_key})"
            );
        }

        println!(
            "AGENT multi-capability verified: all {} capabilities individually queryable",
            capabilities.len()
        );
    });
}

// =============================================================================
// 7. Verify PushDrop roundtrip from lookup response
// =============================================================================

/// After submitting an AGENT token, fetches it back via lookup and verifies
/// the PushDrop fields in the BEEF match the 6-field AGENT structure.
#[ignore]
#[test]
fn agent_verify_pushdrop_roundtrip() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        // Use a unique endpoint hostname so the has_duplicate_record check
        // (identity_key + endpoint) does not skip this registration as a duplicate
        // of a prior test's record. URI validation requires path == "/", so we
        // vary the hostname instead.
        let endpoint = "https://roundtrip-test.dev-a3e.workers.dev";
        let capabilities = &["roundtrip-test"];

        // Submit an agent
        println!("Step 1: Creating and submitting AGENT token...");
        let (_script_bytes, script_hex) =
            build_agent_locking_script(&identity_key, endpoint, capabilities).await;

        let (txid, beef) =
            create_agent_transaction(&script_hex, "AGENT for PushDrop roundtrip test").await;
        submit_to_overlay(&beef, &["tm_agent"]).await;
        println!("  txid: {txid}");

        // Wait for the overlay worker to complete async mutations (CF Queue / wait_until).
        // The onSteakReady pattern returns the Steak immediately, but INSERTs into
        // the lookup tables happen asynchronously. Without this delay the lookup
        // may not include the just-submitted output.
        println!("  Waiting 5s for async mutations to complete...");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Lookup to get back the BEEF
        println!("Step 2: Looking up AGENT records...");
        let lookup_result = lookup_overlay(
            "ls_agent",
            serde_json::json!({"findByIdentityKey": identity_key}),
        )
        .await;

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be array");
        assert!(!outputs.is_empty(), "Should find at least one output");

        // Find the output whose PushDrop capabilities match what we submitted.
        // Multiple outputs may exist for this identity key from prior test runs,
        // so we cannot simply take .last() — we must search for ours.
        let expected_caps = capabilities.join(",");
        let mut found_pushdrop: Option<PushDrop> = None;

        for candidate in outputs.iter() {
            let beef_bytes: Vec<u8> = candidate["beef"]
                .as_array()
                .expect("beef should be array")
                .iter()
                .map(|b| b.as_u64().unwrap() as u8)
                .collect();
            let oi = candidate["outputIndex"]
                .as_u64()
                .expect("outputIndex should be u64") as usize;

            let tx = match bsv_rs::transaction::Transaction::from_beef(&beef_bytes, None) {
                Ok(t) => t,
                Err(_) => continue,
            };
            if oi >= tx.outputs.len() {
                continue;
            }

            let pd = match PushDrop::decode(&tx.outputs[oi].locking_script) {
                Ok(p) => p,
                Err(_) => continue,
            };

            if pd.fields.len() >= 5 {
                let caps = String::from_utf8_lossy(&pd.fields[4]);
                if caps == expected_caps {
                    found_pushdrop = Some(pd);
                    break;
                }
            }
        }

        let pushdrop = found_pushdrop.expect(&format!(
            "Should find an output with capabilities '{expected_caps}' among {} outputs",
            outputs.len()
        ));

        // Verify 6 fields
        assert_eq!(
            pushdrop.fields.len(),
            6,
            "AGENT PushDrop should have 6 fields, got: {}",
            pushdrop.fields.len()
        );

        // Field 0: protocol tag
        let protocol = String::from_utf8_lossy(&pushdrop.fields[0]);
        assert_eq!(protocol, "AGENT", "Field 0 should be 'AGENT'");

        // Field 1: subject identity key (33 bytes)
        assert_eq!(
            pushdrop.fields[1].len(),
            33,
            "Field 1 (subject) should be 33 bytes, got: {}",
            pushdrop.fields[1].len()
        );
        let subject_hex = to_hex(&pushdrop.fields[1]);
        assert_eq!(
            subject_hex, identity_key,
            "Field 1 (subject) should match identity key"
        );

        // Field 2: certifier identity key (33 bytes, self-signed)
        assert_eq!(
            pushdrop.fields[2].len(),
            33,
            "Field 2 (certifier) should be 33 bytes, got: {}",
            pushdrop.fields[2].len()
        );
        let certifier_hex = to_hex(&pushdrop.fields[2]);
        assert_eq!(
            certifier_hex, identity_key,
            "Field 2 (certifier) should match identity key (self-signed)"
        );

        // Field 3: endpoint URL
        let decoded_endpoint = String::from_utf8_lossy(&pushdrop.fields[3]);
        assert_eq!(
            decoded_endpoint, endpoint,
            "Field 3 (endpoint) should match"
        );

        // Field 4: capabilities CSV
        let decoded_caps = String::from_utf8_lossy(&pushdrop.fields[4]);
        assert_eq!(
            decoded_caps, expected_caps,
            "Field 4 (capabilities) should match"
        );

        // Field 5: signature (should be non-empty)
        assert!(
            !pushdrop.fields[5].is_empty(),
            "Field 5 (signature) should be non-empty"
        );

        println!("AGENT PushDrop roundtrip verified:");
        println!("  Protocol: {protocol}");
        println!("  Subject: {subject_hex}");
        println!("  Certifier: {certifier_hex}");
        println!("  Endpoint: {decoded_endpoint}");
        println!("  Capabilities: {decoded_caps}");
        println!("  Signature: {} bytes", pushdrop.fields[5].len());

        // Print test vectors
        print_test_vectors(
            &identity_key,
            endpoint,
            &capabilities.join(","),
            &script_hex,
            &beef,
            &txid,
        );
    });
}

// =============================================================================
// 8. Generate comprehensive test vectors for the worm
// =============================================================================

/// Creates multiple AGENT tokens with different capability profiles and prints
/// comprehensive test vectors that rust-bsv-worm can use for contract testing.
#[ignore]
#[test]
fn agent_generate_worm_test_vectors() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;

        println!("=== WORM TEST VECTORS: AGENT REGISTRY ===");
        println!("identity_key: {identity_key}");
        println!();

        // Vector 1: Single capability
        {
            let endpoint = "https://<your-overlay>.workers.dev";
            let capabilities = &["overlay-host"];

            println!("--- Vector 1: Single capability ---");
            let (script_bytes, script_hex) =
                build_agent_locking_script(&identity_key, endpoint, capabilities).await;
            let (txid, beef) =
                create_agent_transaction(&script_hex, "AGENT vector: single capability").await;

            let steak = submit_to_overlay(&beef, &["tm_agent"]).await;
            println!("  submit_result: {steak}");

            print_test_vectors(
                &identity_key,
                endpoint,
                &capabilities.join(","),
                &script_hex,
                &beef,
                &txid,
            );

            // Also print the raw PushDrop field breakdown
            let script = bsv_rs::script::Script::from_binary(&script_bytes)
                .expect("Script bytes should parse");
            let locking: bsv_rs::script::LockingScript = script.into();
            let pushdrop = PushDrop::decode(&locking).expect("Should decode as PushDrop");
            println!("  field_count: {}", pushdrop.fields.len());
            for (i, field) in pushdrop.fields.iter().enumerate() {
                println!("  field_{i}_hex: {}", to_hex(field));
                if i == 0 || i == 3 || i == 4 {
                    println!("  field_{i}_utf8: {}", String::from_utf8_lossy(field));
                }
            }
            println!();
        }

        // Vector 2: Multiple capabilities
        {
            let endpoint = "https://agent.example.com/api";
            let capabilities = &["tool-use", "wallet", "messaging"];

            println!("--- Vector 2: Multiple capabilities ---");
            let (script_bytes, script_hex) =
                build_agent_locking_script(&identity_key, endpoint, capabilities).await;
            let (txid, beef) =
                create_agent_transaction(&script_hex, "AGENT vector: multi capability").await;

            let steak = submit_to_overlay(&beef, &["tm_agent"]).await;
            println!("  submit_result: {steak}");

            print_test_vectors(
                &identity_key,
                endpoint,
                &capabilities.join(","),
                &script_hex,
                &beef,
                &txid,
            );

            let script = bsv_rs::script::Script::from_binary(&script_bytes)
                .expect("Script bytes should parse");
            let locking: bsv_rs::script::LockingScript = script.into();
            let pushdrop = PushDrop::decode(&locking).expect("Should decode as PushDrop");
            println!("  field_count: {}", pushdrop.fields.len());
            for (i, field) in pushdrop.fields.iter().enumerate() {
                println!("  field_{i}_hex: {}", to_hex(field));
                if i == 0 || i == 3 || i == 4 {
                    println!("  field_{i}_utf8: {}", String::from_utf8_lossy(field));
                }
            }
            println!();
        }

        println!("=== END WORM TEST VECTORS ===");
    });
}
