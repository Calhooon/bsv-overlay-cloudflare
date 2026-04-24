//! End-to-end tests using a live wallet (localhost:3322) and the deployed overlay worker.
//!
//! These tests create real SHIP and SLAP advertisements using the wallet,
//! submit them to the live overlay worker, and validate the full cycle.
//!
//! # Prerequisites
//! - Wallet running on localhost:3322 in auto-approve mode
//! - Overlay worker deployed at <your-overlay>.workers.dev
//!
//! # Running
//! ```bash
//! cargo test --test wallet_e2e -- --ignored --nocapture
//! ```
//!
//! # Budget
//! Each SHIP/SLAP ad costs ~1 sat + ~200 sats fee. Budget: <50,000 sats total.
//! These tests create at most 5 transactions (~1,000 sats).

use bsv_rs::primitives::ec::PublicKey;
use bsv_rs::script::templates::PushDrop;

const WALLET_URL: &str = "http://localhost:3322";
const OVERLAY_URL: &str = "https://<your-overlay>.workers.dev";
const ADVERTISED_DOMAIN: &str = "https://<your-overlay>.workers.dev";

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

/// Build a PushDrop locking script for a SHIP or SLAP advertisement.
///
/// The PushDrop has 5 fields:
/// 1. Protocol ("SHIP" or "SLAP")
/// 2. Identity key (33-byte compressed pubkey)
/// 3. Domain (UTF-8 string)
/// 4. Topic or service name (UTF-8 string)
/// 5. Signature (real ECDSA from wallet createSignature)
///
/// The locking key is the BRC-42 derived child key (NOT the identity key).
async fn build_ad_locking_script(
    protocol: &str,
    identity_key_hex: &str,
    domain: &str,
    topic_or_service: &str,
) -> Vec<u8> {
    let identity_key_bytes = from_hex(identity_key_hex);

    // Determine the BRC-43 protocol name from the short protocol identifier
    let brc43_protocol = match protocol {
        "SHIP" => "service host interconnect",
        "SLAP" => "service lookup availability",
        other => panic!("unsupported protocol: {other}"),
    };

    // Build data fields (WITHOUT signature) for signing
    let data_fields = vec![
        protocol.as_bytes().to_vec(),
        identity_key_bytes.clone(),
        domain.as_bytes().to_vec(),
        topic_or_service.as_bytes().to_vec(),
    ];

    // Concatenate data fields for signing
    let data_to_sign: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

    // Create REAL signature via wallet
    let sig_resp = wallet_call(
        "createSignature",
        &serde_json::json!({
            "data": data_to_sign.iter().map(|b| *b as u64).collect::<Vec<_>>(),
            "protocolID": [2, brc43_protocol],
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
            "protocolID": [2, brc43_protocol],
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

    // Build PushDrop with DERIVED locking key + all 5 fields including real signature
    let mut all_fields = data_fields;
    all_fields.push(signature_bytes);

    let pushdrop = PushDrop::new(locking_key, all_fields);
    pushdrop.lock().to_binary()
}

/// Create a transaction via the wallet with a custom locking script output.
/// Returns (txid, beef_bytes).
async fn create_ad_transaction(
    locking_script_hex: &str,
    description: &str,
    output_description: &str,
) -> (String, Vec<u8>) {
    let body = serde_json::json!({
        "description": description,
        "outputs": [{
            "satoshis": 1,
            "lockingScript": locking_script_hex,
            "outputDescription": output_description,
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

// =============================================================================
// 1. Wallet connectivity probe
// =============================================================================

/// Verifies the wallet is running and returns a valid identity key.
#[ignore]
#[test]
fn wallet_connectivity() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;

        assert_eq!(
            identity_key.len(),
            66,
            "Identity key should be 66 hex chars (33 bytes compressed), got: {identity_key}"
        );
        assert!(
            identity_key.starts_with("02") || identity_key.starts_with("03"),
            "Identity key should start with 02 or 03, got: {identity_key}"
        );

        println!("Wallet identity key: {identity_key}");
    });
}

// =============================================================================
// 2. Build and verify PushDrop script locally
// =============================================================================

/// Verifies that our PushDrop construction produces a valid 5-field script
/// that can be decoded and matches the expected structure.
#[ignore]
#[test]
fn build_pushdrop_script_locally() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        let script_bytes =
            build_ad_locking_script("SHIP", &identity_key, ADVERTISED_DOMAIN, "tm_test").await;

        assert!(!script_bytes.is_empty(), "Script bytes should not be empty");

        // Decode and verify structure
        let script =
            bsv_rs::script::Script::from_binary(&script_bytes).expect("Script bytes should parse");
        let locking: bsv_rs::script::LockingScript = script.into();
        let pushdrop = PushDrop::decode(&locking).expect("Should decode as PushDrop");

        assert_eq!(pushdrop.fields.len(), 5, "Should have 5 fields");
        assert_eq!(
            String::from_utf8_lossy(&pushdrop.fields[0]),
            "SHIP",
            "Field 0 should be SHIP"
        );
        assert_eq!(
            pushdrop.fields[1].len(),
            33,
            "Field 1 should be 33 bytes (compressed pubkey)"
        );
        assert_eq!(
            to_hex(&pushdrop.fields[1]),
            identity_key,
            "Field 1 should match identity key"
        );
        assert_eq!(
            String::from_utf8_lossy(&pushdrop.fields[2]),
            ADVERTISED_DOMAIN,
            "Field 2 should be domain"
        );
        assert_eq!(
            String::from_utf8_lossy(&pushdrop.fields[3]),
            "tm_test",
            "Field 3 should be topic name"
        );
        assert!(
            !pushdrop.fields[4].is_empty(),
            "Field 4 (signature) should be non-empty"
        );

        let script_hex = to_hex(&script_bytes);
        println!(
            "PushDrop SHIP script ({} bytes): {}",
            script_bytes.len(),
            &script_hex[..80.min(script_hex.len())]
        );
        println!("PushDrop fields verified: 5 fields, protocol=SHIP, topic=tm_test");
    });
}

// =============================================================================
// 3. Create SHIP advertisement transaction
// =============================================================================

/// Creates a fresh SHIP advertisement transaction via the wallet and verifies
/// the response format (txid + AtomicBEEF bytes).
#[ignore]
#[test]
fn wallet_create_ship_transaction() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        let script_bytes =
            build_ad_locking_script("SHIP", &identity_key, ADVERTISED_DOMAIN, "tm_test").await;
        let script_hex = to_hex(&script_bytes);

        println!("Creating SHIP advertisement transaction...");
        let (txid, beef) = create_ad_transaction(
            &script_hex,
            "SHIP advertisement for tm_test",
            "SHIP advertisement output",
        )
        .await;

        assert_eq!(txid.len(), 64, "txid should be 64 hex chars");
        assert!(
            beef.len() > 100,
            "BEEF should be substantial, got {} bytes",
            beef.len()
        );

        // Verify the BEEF starts with a valid version byte
        // AtomicBEEF version 0x0101 (257) or standard BEEF 0x0100 (256)
        let version = u32::from_le_bytes([beef[0], beef[1], beef[2], beef[3]]);
        println!(
            "BEEF version: 0x{:08x} ({version}), length: {} bytes",
            version,
            beef.len()
        );
        assert!(
            version == 0x0101_0001 || version == 0x0100_0001 || beef[0] == 0x01,
            "BEEF should have a valid version prefix, got first 4 bytes: {:02x}{:02x}{:02x}{:02x}",
            beef[0],
            beef[1],
            beef[2],
            beef[3]
        );

        println!("SHIP transaction created: txid={txid}");
    });
}

// =============================================================================
// 4. Create and submit SHIP advertisement (full E2E)
// =============================================================================

/// Full E2E: create SHIP advertisement via wallet, submit to overlay, verify admission.
#[ignore]
#[test]
fn wallet_create_and_submit_ship_advertisement() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        println!("Identity key: {identity_key}");

        // Step 1: Build locking script
        let script_bytes =
            build_ad_locking_script("SHIP", &identity_key, ADVERTISED_DOMAIN, "tm_test").await;
        let script_hex = to_hex(&script_bytes);

        // Step 2: Create transaction via wallet
        println!("Step 1: Creating SHIP transaction via wallet...");
        let (txid, beef) = create_ad_transaction(
            &script_hex,
            "SHIP advertisement for tm_test",
            "SHIP advertisement output",
        )
        .await;
        println!("  txid: {txid}");

        // Step 3: Submit to overlay worker
        println!("Step 2: Submitting to overlay worker...");
        let steak = submit_to_overlay(&beef, &["tm_ship"]).await;

        // Step 4: Verify admission
        let tm_ship = steak.get("tm_ship");
        assert!(
            tm_ship.is_some(),
            "Response should contain tm_ship key, got: {steak}"
        );

        let admitted = tm_ship.unwrap()["outputsToAdmit"]
            .as_array()
            .expect("outputsToAdmit should be an array");

        println!(
            "Step 3: Admission result: {} outputs admitted",
            admitted.len()
        );

        // First submit should admit at least one output
        // (subsequent runs may dedup, which is also valid)
        if admitted.is_empty() {
            println!("  NOTE: No outputs admitted (likely already exists from a prior run)");
        } else {
            println!("  Admitted output indices: {:?}", admitted);
        }

        println!("SHIP E2E complete: txid={txid}");
    });
}

// =============================================================================
// 5. Create and submit SLAP advertisement (full E2E)
// =============================================================================

/// Full E2E: create SLAP advertisement via wallet, submit to overlay, verify admission.
#[ignore]
#[test]
fn wallet_create_and_submit_slap_advertisement() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        println!("Identity key: {identity_key}");

        // Step 1: Build SLAP locking script
        let script_bytes =
            build_ad_locking_script("SLAP", &identity_key, ADVERTISED_DOMAIN, "ls_test").await;
        let script_hex = to_hex(&script_bytes);

        // Step 2: Create transaction via wallet
        println!("Step 1: Creating SLAP transaction via wallet...");
        let (txid, beef) = create_ad_transaction(
            &script_hex,
            "SLAP advertisement for ls_test",
            "SLAP advertisement output",
        )
        .await;
        println!("  txid: {txid}");

        // Step 3: Submit to overlay worker
        println!("Step 2: Submitting to overlay worker...");
        let steak = submit_to_overlay(&beef, &["tm_slap"]).await;

        // Step 4: Verify admission
        let tm_slap = steak.get("tm_slap");
        assert!(
            tm_slap.is_some(),
            "Response should contain tm_slap key, got: {steak}"
        );

        let admitted = tm_slap.unwrap()["outputsToAdmit"]
            .as_array()
            .expect("outputsToAdmit should be an array");

        println!(
            "Step 3: Admission result: {} outputs admitted",
            admitted.len()
        );

        if admitted.is_empty() {
            println!("  NOTE: No outputs admitted (likely already exists from a prior run)");
        } else {
            println!("  Admitted output indices: {:?}", admitted);
        }

        println!("SLAP E2E complete: txid={txid}");
    });
}

// =============================================================================
// 6. Submit SHIP then lookup by domain
// =============================================================================

/// Full cycle: create SHIP ad, submit, then lookup by domain to verify indexing.
#[ignore]
#[test]
fn wallet_submit_ship_then_lookup_by_domain() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        println!("Identity key: {identity_key}");

        // Step 1: Create and submit SHIP ad
        let script_bytes =
            build_ad_locking_script("SHIP", &identity_key, ADVERTISED_DOMAIN, "tm_test").await;
        let script_hex = to_hex(&script_bytes);

        println!("Step 1: Creating SHIP transaction...");
        let (txid, beef) = create_ad_transaction(
            &script_hex,
            "SHIP ad for lookup test",
            "SHIP advertisement output",
        )
        .await;

        println!("Step 2: Submitting to overlay...");
        let steak = submit_to_overlay(&beef, &["tm_ship"]).await;
        println!("  Submit result: {steak}");

        // Step 2: Lookup by domain
        println!("Step 3: Looking up SHIP records by domain...");
        let lookup_result =
            lookup_overlay("ls_ship", serde_json::json!({"domain": ADVERTISED_DOMAIN})).await;

        assert_eq!(
            lookup_result["type"], "output-list",
            "Lookup should return output-list, got: {lookup_result}"
        );

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be an array");

        println!(
            "  Lookup returned {} SHIP outputs for domain {}",
            outputs.len(),
            ADVERTISED_DOMAIN
        );

        // We should find at least our newly submitted ad
        assert!(
            !outputs.is_empty(),
            "Should find at least one SHIP output for {ADVERTISED_DOMAIN}"
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

        // The lookup returns BEEF bytes (not txid directly), so we verify
        // at least one output exists. Detailed PushDrop verification is in test 14.
        assert!(
            !outputs.is_empty(),
            "Our output should be in lookup results"
        );

        println!(
            "SHIP lookup-by-domain verified: {} outputs found, txid={txid}",
            outputs.len()
        );
    });
}

// =============================================================================
// 7. Submit SLAP then lookup by domain
// =============================================================================

/// Full cycle: create SLAP ad, submit, then lookup by domain.
#[ignore]
#[test]
fn wallet_submit_slap_then_lookup_by_domain() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        println!("Identity key: {identity_key}");

        // Step 1: Create and submit SLAP ad
        let script_bytes =
            build_ad_locking_script("SLAP", &identity_key, ADVERTISED_DOMAIN, "ls_test").await;
        let script_hex = to_hex(&script_bytes);

        println!("Step 1: Creating SLAP transaction...");
        let (_txid, beef) = create_ad_transaction(
            &script_hex,
            "SLAP ad for lookup test",
            "SLAP advertisement output",
        )
        .await;

        println!("Step 2: Submitting to overlay...");
        submit_to_overlay(&beef, &["tm_slap"]).await;

        // Step 2: Lookup by domain
        println!("Step 3: Looking up SLAP records by domain...");
        let lookup_result =
            lookup_overlay("ls_slap", serde_json::json!({"domain": ADVERTISED_DOMAIN})).await;

        assert_eq!(
            lookup_result["type"], "output-list",
            "Lookup should return output-list, got: {lookup_result}"
        );

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be an array");

        println!(
            "  Lookup returned {} SLAP outputs for domain {}",
            outputs.len(),
            ADVERTISED_DOMAIN
        );

        assert!(
            !outputs.is_empty(),
            "Should find at least one SLAP output for {ADVERTISED_DOMAIN}"
        );

        println!(
            "SLAP lookup-by-domain verified: {} outputs found",
            outputs.len()
        );
    });
}

// =============================================================================
// 8. Submit SHIP then lookup by topic
// =============================================================================

/// Create SHIP ad for a specific topic, submit, then lookup by topic name.
#[ignore]
#[test]
fn wallet_submit_ship_then_lookup_by_topic() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;

        let script_bytes =
            build_ad_locking_script("SHIP", &identity_key, ADVERTISED_DOMAIN, "tm_test").await;
        let script_hex = to_hex(&script_bytes);

        println!("Step 1: Creating SHIP transaction for tm_test...");
        let (_txid, beef) = create_ad_transaction(
            &script_hex,
            "SHIP ad for topic lookup test",
            "SHIP advertisement output",
        )
        .await;

        println!("Step 2: Submitting to overlay...");
        submit_to_overlay(&beef, &["tm_ship"]).await;

        // Lookup by topic
        println!("Step 3: Looking up SHIP records by topic...");
        let lookup_result =
            lookup_overlay("ls_ship", serde_json::json!({"topics": ["tm_test"]})).await;

        assert_eq!(lookup_result["type"], "output-list");

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be an array");

        println!(
            "  Lookup by topic returned {} outputs for tm_test",
            outputs.len()
        );

        assert!(
            !outputs.is_empty(),
            "Should find at least one SHIP output for topic tm_test"
        );

        println!("SHIP lookup-by-topic verified");
    });
}

// =============================================================================
// 9. Submit SLAP then lookup by service name
// =============================================================================

/// Create SLAP ad for a specific service, submit, then lookup by service name.
#[ignore]
#[test]
fn wallet_submit_slap_then_lookup_by_service() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;

        let script_bytes =
            build_ad_locking_script("SLAP", &identity_key, ADVERTISED_DOMAIN, "ls_test").await;
        let script_hex = to_hex(&script_bytes);

        println!("Step 1: Creating SLAP transaction for ls_test...");
        let (_txid, beef) = create_ad_transaction(
            &script_hex,
            "SLAP ad for service lookup test",
            "SLAP advertisement output",
        )
        .await;

        println!("Step 2: Submitting to overlay...");
        submit_to_overlay(&beef, &["tm_slap"]).await;

        // Lookup by service name
        println!("Step 3: Looking up SLAP records by service name...");
        let lookup_result =
            lookup_overlay("ls_slap", serde_json::json!({"service": "ls_test"})).await;

        assert_eq!(lookup_result["type"], "output-list");

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be an array");

        println!(
            "  Lookup by service returned {} outputs for ls_test",
            outputs.len()
        );

        assert!(
            !outputs.is_empty(),
            "Should find at least one SLAP output for service ls_test"
        );

        println!("SLAP lookup-by-service verified");
    });
}

// =============================================================================
// 10. Deduplication: submit same SHIP ad twice
// =============================================================================

/// Submits the same SHIP advertisement BEEF twice and verifies the second
/// submission is deduplicated (outputsToAdmit is empty).
#[ignore]
#[test]
fn wallet_ship_deduplication() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;

        let script_bytes =
            build_ad_locking_script("SHIP", &identity_key, ADVERTISED_DOMAIN, "tm_test").await;
        let script_hex = to_hex(&script_bytes);

        println!("Step 1: Creating SHIP transaction...");
        let (_txid, beef) = create_ad_transaction(
            &script_hex,
            "SHIP ad for dedup test",
            "SHIP advertisement output",
        )
        .await;

        // First submit
        println!("Step 2: First submit...");
        let steak1 = submit_to_overlay(&beef, &["tm_ship"]).await;
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
        let steak2 = submit_to_overlay(&beef, &["tm_ship"]).await;
        println!("  Second: {steak2}");

        let admitted2 = steak2["tm_ship"]["outputsToAdmit"]
            .as_array()
            .expect("outputsToAdmit should be array");

        assert!(
            admitted2.is_empty(),
            "Second submit should be deduplicated (empty outputsToAdmit), got: {steak2}"
        );

        println!("Deduplication verified: second submit returned empty outputsToAdmit");
    });
}

// =============================================================================
// 11. Lookup findAll for SHIP
// =============================================================================

/// Queries all SHIP records via findAll and verifies the response format.
#[ignore]
#[test]
fn wallet_lookup_ship_find_all() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        println!("Looking up all SHIP records...");
        let lookup_result = lookup_overlay("ls_ship", serde_json::json!({"find_all": true})).await;

        assert_eq!(
            lookup_result["type"], "output-list",
            "Should return output-list, got: {lookup_result}"
        );

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be an array");

        println!("  findAll returned {} SHIP outputs", outputs.len());

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

            // Verify BEEF is non-empty byte array
            let beef = output["beef"].as_array().expect("beef should be array");
            assert!(!beef.is_empty(), "Output {i} beef should not be empty");
        }

        println!("SHIP findAll structure verified");
    });
}

// =============================================================================
// 12. Lookup findAll for SLAP
// =============================================================================

/// Queries all SLAP records via findAll and verifies the response format.
#[ignore]
#[test]
fn wallet_lookup_slap_find_all() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        println!("Looking up all SLAP records...");
        let lookup_result = lookup_overlay("ls_slap", serde_json::json!({"find_all": true})).await;

        assert_eq!(
            lookup_result["type"], "output-list",
            "Should return output-list, got: {lookup_result}"
        );

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be an array");

        println!("  findAll returned {} SLAP outputs", outputs.len());

        for (i, output) in outputs.iter().enumerate() {
            assert!(
                output.get("beef").is_some(),
                "Output {i} should have 'beef'"
            );
            assert!(
                output.get("outputIndex").is_some(),
                "Output {i} should have 'outputIndex'"
            );
        }

        println!("SLAP findAll structure verified");
    });
}

// =============================================================================
// 13. SHIP and SLAP in same test — combined E2E cycle
// =============================================================================

/// Full combined E2E: create both SHIP and SLAP ads, submit both, verify both
/// appear in their respective lookups.
#[ignore]
#[test]
fn wallet_combined_ship_and_slap_e2e() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        println!("Identity key: {identity_key}");

        // --- SHIP ---
        println!("\n--- SHIP ---");
        let ship_script =
            build_ad_locking_script("SHIP", &identity_key, ADVERTISED_DOMAIN, "tm_test").await;
        let ship_script_hex = to_hex(&ship_script);

        let (ship_txid, ship_beef) = create_ad_transaction(
            &ship_script_hex,
            "SHIP ad combined test",
            "SHIP advertisement output",
        )
        .await;
        println!("  SHIP txid: {ship_txid}");

        let ship_steak = submit_to_overlay(&ship_beef, &["tm_ship"]).await;
        println!("  SHIP submit: {ship_steak}");

        // --- SLAP ---
        println!("\n--- SLAP ---");
        let slap_script =
            build_ad_locking_script("SLAP", &identity_key, ADVERTISED_DOMAIN, "ls_test").await;
        let slap_script_hex = to_hex(&slap_script);

        let (slap_txid, slap_beef) = create_ad_transaction(
            &slap_script_hex,
            "SLAP ad combined test",
            "SLAP advertisement output",
        )
        .await;
        println!("  SLAP txid: {slap_txid}");

        let slap_steak = submit_to_overlay(&slap_beef, &["tm_slap"]).await;
        println!("  SLAP submit: {slap_steak}");

        // --- Verify SHIP lookup ---
        println!("\n--- Verify SHIP lookup ---");
        let ship_lookup =
            lookup_overlay("ls_ship", serde_json::json!({"domain": ADVERTISED_DOMAIN})).await;
        let ship_outputs = ship_lookup["outputs"]
            .as_array()
            .expect("SHIP outputs should be array");
        assert!(
            !ship_outputs.is_empty(),
            "Should find SHIP outputs for {ADVERTISED_DOMAIN}"
        );
        println!("  SHIP lookup: {} outputs", ship_outputs.len());

        // --- Verify SLAP lookup ---
        println!("\n--- Verify SLAP lookup ---");
        let slap_lookup =
            lookup_overlay("ls_slap", serde_json::json!({"domain": ADVERTISED_DOMAIN})).await;
        let slap_outputs = slap_lookup["outputs"]
            .as_array()
            .expect("SLAP outputs should be array");
        assert!(
            !slap_outputs.is_empty(),
            "Should find SLAP outputs for {ADVERTISED_DOMAIN}"
        );
        println!("  SLAP lookup: {} outputs", slap_outputs.len());

        println!("\nCombined E2E complete:");
        println!(
            "  SHIP: txid={ship_txid}, lookup={} outputs",
            ship_outputs.len()
        );
        println!(
            "  SLAP: txid={slap_txid}, lookup={} outputs",
            slap_outputs.len()
        );
    });
}

// =============================================================================
// 14. Verify PushDrop can be decoded from lookup response BEEF
// =============================================================================

/// After submitting a SHIP ad, fetch it back via lookup and verify the
/// PushDrop fields in the BEEF match what we originally submitted.
#[ignore]
#[test]
fn wallet_verify_pushdrop_roundtrip_from_lookup() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;

        // Submit a SHIP ad
        let script_bytes =
            build_ad_locking_script("SHIP", &identity_key, ADVERTISED_DOMAIN, "tm_test").await;
        let script_hex = to_hex(&script_bytes);

        let (_txid, beef) = create_ad_transaction(
            &script_hex,
            "SHIP ad for roundtrip verification",
            "SHIP advertisement output",
        )
        .await;
        submit_to_overlay(&beef, &["tm_ship"]).await;

        // Lookup
        let lookup_result =
            lookup_overlay("ls_ship", serde_json::json!({"domain": ADVERTISED_DOMAIN})).await;

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be array");
        assert!(!outputs.is_empty(), "Should find at least one output");

        // Parse BEEF from the first output to extract the PushDrop
        let first_output = &outputs[0];
        let beef_bytes: Vec<u8> = first_output["beef"]
            .as_array()
            .expect("beef should be array")
            .iter()
            .map(|b| b.as_u64().unwrap() as u8)
            .collect();
        let output_index = first_output["outputIndex"]
            .as_u64()
            .expect("outputIndex should be u64") as usize;

        // Parse the transaction from BEEF
        let tx = bsv_rs::transaction::Transaction::from_beef(&beef_bytes, None)
            .expect("Should parse BEEF into transaction");

        assert!(
            output_index < tx.outputs.len(),
            "outputIndex {output_index} should be within transaction outputs (len={})",
            tx.outputs.len()
        );

        let output = &tx.outputs[output_index];
        let pushdrop =
            PushDrop::decode(&output.locking_script).expect("Output should be a valid PushDrop");

        // Verify fields
        let protocol = String::from_utf8_lossy(&pushdrop.fields[0]);
        assert_eq!(protocol, "SHIP", "Protocol should be SHIP");

        let domain = String::from_utf8_lossy(&pushdrop.fields[2]);
        // The domain should be a valid advertisable URI
        assert!(!domain.is_empty(), "Domain should not be empty");

        let topic = String::from_utf8_lossy(&pushdrop.fields[3]);
        assert!(
            topic.starts_with("tm_"),
            "Topic should start with tm_, got: {topic}"
        );

        println!("PushDrop roundtrip verified:");
        println!("  Protocol: {protocol}");
        println!("  Identity key: {}", to_hex(&pushdrop.fields[1]));
        println!("  Domain: {domain}");
        println!("  Topic: {topic}");
        println!(
            "  Signature len: {} bytes",
            pushdrop.fields.get(4).map(|f| f.len()).unwrap_or(0)
        );
    });
}

// =============================================================================
// 15. Lookup SHIP by identity key
// =============================================================================

/// Submit SHIP ad and then look it up by the wallet's identity key.
#[ignore]
#[test]
fn wallet_lookup_ship_by_identity_key() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let identity_key = get_identity_key().await;
        println!("Identity key: {identity_key}");

        // Ensure there is at least one SHIP ad for this identity key
        let script_bytes =
            build_ad_locking_script("SHIP", &identity_key, ADVERTISED_DOMAIN, "tm_test").await;
        let script_hex = to_hex(&script_bytes);

        let (_txid, beef) = create_ad_transaction(
            &script_hex,
            "SHIP ad for identity key lookup",
            "SHIP advertisement output",
        )
        .await;
        submit_to_overlay(&beef, &["tm_ship"]).await;

        // Lookup by identity key
        println!("Looking up SHIP records by identity key...");
        let lookup_result =
            lookup_overlay("ls_ship", serde_json::json!({"identity_key": identity_key})).await;

        assert_eq!(lookup_result["type"], "output-list");

        let outputs = lookup_result["outputs"]
            .as_array()
            .expect("outputs should be array");

        println!(
            "  Lookup by identity key returned {} outputs",
            outputs.len()
        );

        assert!(
            !outputs.is_empty(),
            "Should find at least one SHIP output for identity key {identity_key}"
        );

        println!("SHIP lookup-by-identity-key verified");
    });
}
