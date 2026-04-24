//! Live end-to-end tests for the deployed overlay worker.
//!
//! These tests hit the real Cloudflare Worker at <your-overlay>.workers.dev.
//! Run with: cargo test --test live_worker -- --ignored

const BASE_URL: &str = "https://<your-overlay>.workers.dev";

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

// =============================================================================
// 1. Health endpoint
// =============================================================================

#[ignore]
#[test]
fn health_endpoint() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .get(format!("{BASE_URL}/health"))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(resp.status(), 200, "Expected 200 OK");

        let body: serde_json::Value = resp.json().await.expect("response should be JSON");
        assert_eq!(body["status"], "ok", "Expected status: ok, got: {body}");
    });
}

// =============================================================================
// 2. List topic managers
// =============================================================================

#[ignore]
#[test]
fn list_topic_managers() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .get(format!("{BASE_URL}/listTopicManagers"))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.expect("response should be JSON");
        let obj = body.as_object().expect("response should be a JSON object");

        assert!(obj.contains_key("tm_ship"), "Missing tm_ship in: {body}");
        assert!(obj.contains_key("tm_slap"), "Missing tm_slap in: {body}");

        // Each entry is either a string (simple) or an object with name/description
        for key in &["tm_ship", "tm_slap"] {
            let val = &obj[*key];
            if val.is_string() {
                assert!(
                    !val.as_str().unwrap().is_empty(),
                    "{key} should have non-empty metadata"
                );
            } else if val.is_object() {
                // Engine returns { name, description } objects
                let inner = val.as_object().unwrap();
                assert!(
                    inner.contains_key("name") || inner.contains_key("description"),
                    "{key} object should have name or description, got: {val}"
                );
            } else {
                panic!("{key} value should be a string or object, got: {val}");
            }
        }

        println!("Topic managers: {:?}", obj.keys().collect::<Vec<_>>());
    });
}

// =============================================================================
// 3. List lookup service providers
// =============================================================================

#[ignore]
#[test]
fn list_lookup_service_providers() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .get(format!("{BASE_URL}/listLookupServiceProviders"))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.expect("response should be JSON");
        let obj = body.as_object().expect("response should be a JSON object");

        assert!(obj.contains_key("ls_ship"), "Missing ls_ship in: {body}");
        assert!(obj.contains_key("ls_slap"), "Missing ls_slap in: {body}");

        for key in &["ls_ship", "ls_slap"] {
            let val = &obj[*key];
            if val.is_string() {
                assert!(
                    !val.as_str().unwrap().is_empty(),
                    "{key} should have non-empty metadata"
                );
            } else if val.is_object() {
                let inner = val.as_object().unwrap();
                assert!(
                    inner.contains_key("name") || inner.contains_key("description"),
                    "{key} object should have name or description, got: {val}"
                );
            } else {
                panic!("{key} value should be a string or object, got: {val}");
            }
        }

        println!("Lookup services: {:?}", obj.keys().collect::<Vec<_>>());
    });
}

// =============================================================================
// 4. CORS preflight
// =============================================================================

#[ignore]
#[test]
fn cors_preflight() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .request(reqwest::Method::OPTIONS, format!("{BASE_URL}/health"))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            204,
            "OPTIONS should return 204, got: {}",
            resp.status()
        );

        let headers = resp.headers();

        let acao = headers
            .get("access-control-allow-origin")
            .expect("Missing Access-Control-Allow-Origin");
        assert_eq!(acao, "*", "Allow-Origin should be *");

        assert!(
            headers.get("access-control-allow-headers").is_some(),
            "Missing Access-Control-Allow-Headers"
        );
        assert!(
            headers.get("access-control-allow-methods").is_some(),
            "Missing Access-Control-Allow-Methods"
        );

        println!("CORS preflight headers verified");
    });
}

// =============================================================================
// 5. CORS headers on regular response
// =============================================================================

#[ignore]
#[test]
fn cors_headers_on_response() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .get(format!("{BASE_URL}/health"))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(resp.status(), 200);

        let headers = resp.headers();

        let acao = headers
            .get("access-control-allow-origin")
            .expect("Missing Access-Control-Allow-Origin on GET response");
        assert_eq!(acao, "*");

        assert!(
            headers.get("access-control-allow-methods").is_some(),
            "Missing Access-Control-Allow-Methods on GET response"
        );
        assert!(
            headers.get("access-control-expose-headers").is_some(),
            "Missing Access-Control-Expose-Headers on GET response"
        );

        println!("CORS headers on regular GET verified");
    });
}

// =============================================================================
// 6. Not found
// =============================================================================

#[ignore]
#[test]
fn not_found() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .get(format!("{BASE_URL}/nonexistent"))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(resp.status(), 404, "Expected 404 for unknown route");

        let body: serde_json::Value = resp.json().await.expect("404 should return JSON");
        assert_eq!(body["status"], "error");
        assert_eq!(
            body["code"], "ERR_ROUTE_NOT_FOUND",
            "Expected ERR_ROUTE_NOT_FOUND, got: {body}"
        );

        println!("404 response: {body}");
    });
}

// =============================================================================
// 7. Submit with missing topics header
// =============================================================================

#[ignore]
#[test]
fn submit_with_missing_topics_header() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .post(format!("{BASE_URL}/submit"))
            .body(vec![0u8; 10]) // dummy BEEF bytes
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            400,
            "Missing x-topics should be 400, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("error should be JSON");
        assert_eq!(body["status"], "error");
        let msg = body["message"].as_str().unwrap_or("");
        assert!(
            msg.to_lowercase().contains("x-topics"),
            "Error should mention x-topics, got: {msg}"
        );

        println!("Submit missing topics: {body}");
    });
}

// =============================================================================
// 8. Submit with invalid topics
// =============================================================================

#[ignore]
#[test]
fn submit_with_invalid_topics() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .post(format!("{BASE_URL}/submit"))
            .header("x-topics", "not json")
            .body(vec![0u8; 10])
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            400,
            "Invalid x-topics should be 400, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("error should be JSON");
        assert_eq!(body["status"], "error");
        let msg = body["message"].as_str().unwrap_or("");
        assert!(
            msg.to_lowercase().contains("x-topics"),
            "Error should mention x-topics, got: {msg}"
        );

        println!("Submit invalid topics: {body}");
    });
}

// =============================================================================
// 9. Lookup with empty body
// =============================================================================

#[ignore]
#[test]
fn lookup_with_empty_body() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .post(format!("{BASE_URL}/lookup"))
            .header("Content-Type", "application/json")
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            400,
            "Empty lookup body should be 400, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("error should be JSON");
        assert_eq!(body["status"], "error");

        println!("Lookup empty body: {body}");
    });
}

// =============================================================================
// 10. Lookup SHIP
// =============================================================================

#[ignore]
#[test]
fn lookup_ship() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .post(format!("{BASE_URL}/lookup"))
            .header("Content-Type", "application/json")
            .body(r#"{"service":"ls_ship","query":{"domain":"test"}}"#)
            .send()
            .await
            .expect("HTTP request failed");

        assert!(
            resp.status().is_success(),
            "SHIP lookup should succeed, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("response should be JSON");

        // The response should be a valid lookup answer — either output-list or freeform
        // For a domain that likely has no results, we expect a well-formed response
        assert!(
            body.is_object() || body.is_array(),
            "Lookup response should be JSON object or array, got: {body}"
        );

        println!("SHIP lookup response: {body}");
    });
}

// =============================================================================
// 11. Get documentation for topic manager
// =============================================================================

#[ignore]
#[test]
fn get_documentation_for_topic_manager() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .get(format!(
                "{BASE_URL}/getDocumentationForTopicManager?manager=tm_ship"
            ))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(resp.status(), 200);

        let content_type = resp
            .headers()
            .get("content-type")
            .expect("Missing Content-Type header")
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            content_type.contains("text/markdown"),
            "Expected text/markdown, got: {content_type}"
        );

        let body = resp.text().await.unwrap();
        assert!(!body.is_empty(), "Documentation should not be empty");

        println!(
            "tm_ship documentation ({} bytes): {}...",
            body.len(),
            &body[..body.len().min(200)]
        );
    });
}

// =============================================================================
// 12. Admin without auth
// =============================================================================

#[ignore]
#[test]
fn admin_without_auth() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .post(format!("{BASE_URL}/admin/syncAdvertisements"))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            401,
            "Admin without auth should be 401, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("error should be JSON");
        assert_eq!(body["status"], "error");
        let msg = body["message"].as_str().unwrap_or("");
        assert!(
            msg.contains("Unauthorized") || msg.contains("Missing"),
            "Error should indicate missing token, got: {msg}"
        );

        println!("Admin without auth: {body}");
    });
}

// =============================================================================
// 13. Submit SHIP BEEF and verify admission (D1 pipeline)
// =============================================================================

const SHIP_FIXTURE: &str = include_str!("fixtures/ship_lookup_response.json");

fn parse_first_beef(json: &str) -> Vec<u8> {
    let value: serde_json::Value = serde_json::from_str(json).unwrap();
    value["outputs"][0]["beef"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_u64().unwrap() as u8)
        .collect()
}

#[ignore]
#[test]
fn submit_ship_beef_admits_outputs() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let beef = parse_first_beef(SHIP_FIXTURE);

        let resp = client()
            .post(format!("{BASE_URL}/submit"))
            .header("Content-Type", "application/octet-stream")
            .header("x-topics", r#"["tm_ship"]"#)
            .body(beef)
            .send()
            .await
            .expect("HTTP request failed");

        let status = resp.status();
        let body: serde_json::Value = resp.json().await.expect("response should be JSON");

        println!("Submit SHIP BEEF: status={status} body={body}");

        assert!(
            status.is_success(),
            "Submit should succeed (200), got: {status} body: {body}"
        );

        // Response is a Steak — map of topic -> AdmittanceInstructions
        let tm_ship = body
            .get("tm_ship")
            .expect("Response should contain tm_ship");
        let admitted = tm_ship["outputsToAdmit"]
            .as_array()
            .expect("outputsToAdmit should be an array");

        // First submit should admit outputs; subsequent runs may dedup (empty array)
        println!("Admitted {} outputs: {:?}", admitted.len(), admitted);
    });
}

// =============================================================================
// 14. Submit then lookup SHIP (full D1 round-trip)
// =============================================================================

#[ignore]
#[test]
fn submit_then_lookup_ship_e2e() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Step 1: Submit BEEF
        let beef = parse_first_beef(SHIP_FIXTURE);

        let submit_resp = client()
            .post(format!("{BASE_URL}/submit"))
            .header("Content-Type", "application/octet-stream")
            .header("x-topics", r#"["tm_ship"]"#)
            .body(beef)
            .send()
            .await
            .expect("Submit HTTP request failed");

        assert!(
            submit_resp.status().is_success(),
            "Submit should succeed, got: {}",
            submit_resp.status()
        );

        let submit_body: serde_json::Value = submit_resp.json().await.unwrap();
        println!("Submit response: {submit_body}");

        // Step 2: Lookup
        let lookup_resp = client()
            .post(format!("{BASE_URL}/lookup"))
            .header("Content-Type", "application/json")
            .body(r#"{"service":"ls_ship","query":{}}"#)
            .send()
            .await
            .expect("Lookup HTTP request failed");

        assert!(
            lookup_resp.status().is_success(),
            "Lookup should succeed, got: {}",
            lookup_resp.status()
        );

        let lookup_body: serde_json::Value = lookup_resp.json().await.unwrap();

        assert_eq!(
            lookup_body["type"], "output-list",
            "Lookup should return output-list, got: {lookup_body}"
        );

        let outputs = lookup_body["outputs"]
            .as_array()
            .expect("outputs should be array");

        assert!(
            !outputs.is_empty(),
            "Lookup should return at least one output after submit"
        );

        // Verify each output has BEEF and outputIndex
        for (i, output) in outputs.iter().enumerate() {
            assert!(
                output.get("beef").is_some(),
                "Output {i} should have 'beef' field"
            );
            assert!(
                output.get("outputIndex").is_some(),
                "Output {i} should have 'outputIndex' field"
            );
            let beef_arr = output["beef"]
                .as_array()
                .expect("beef should be byte array");
            assert!(!beef_arr.is_empty(), "Output {i} BEEF should not be empty");
        }

        println!("Lookup returned {} SHIP outputs", outputs.len());
    });
}

// =============================================================================
// 15. Submit SLAP BEEF and lookup (full D1 round-trip)
// =============================================================================

const SLAP_FIXTURE: &str = include_str!("fixtures/slap_lookup_response.json");

#[ignore]
#[test]
fn submit_then_lookup_slap_e2e() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Step 1: Submit SLAP BEEF
        let value: serde_json::Value = serde_json::from_str(SLAP_FIXTURE).unwrap();
        let beef: Vec<u8> = value["outputs"][0]["beef"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b.as_u64().unwrap() as u8)
            .collect();

        let submit_resp = client()
            .post(format!("{BASE_URL}/submit"))
            .header("Content-Type", "application/octet-stream")
            .header("x-topics", r#"["tm_slap"]"#)
            .body(beef)
            .send()
            .await
            .expect("Submit HTTP request failed");

        assert!(
            submit_resp.status().is_success(),
            "Submit should succeed, got: {}",
            submit_resp.status()
        );

        let submit_body: serde_json::Value = submit_resp.json().await.unwrap();
        println!("SLAP submit response: {submit_body}");

        // Step 2: Lookup
        let lookup_resp = client()
            .post(format!("{BASE_URL}/lookup"))
            .header("Content-Type", "application/json")
            .body(r#"{"service":"ls_slap","query":{}}"#)
            .send()
            .await
            .expect("Lookup HTTP request failed");

        assert!(
            lookup_resp.status().is_success(),
            "Lookup should succeed, got: {}",
            lookup_resp.status()
        );

        let lookup_body: serde_json::Value = lookup_resp.json().await.unwrap();

        assert_eq!(lookup_body["type"], "output-list");

        let outputs = lookup_body["outputs"]
            .as_array()
            .expect("outputs should be array");

        assert!(
            !outputs.is_empty(),
            "SLAP lookup should return outputs after submit"
        );

        println!("SLAP lookup returned {} outputs", outputs.len());
    });
}

// =============================================================================
// 16. Dedup: submitting same BEEF twice only admits on first call
// =============================================================================

#[ignore]
#[test]
fn dedup_on_live_worker() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let beef = parse_first_beef(SHIP_FIXTURE);

        // First submit
        let resp1 = client()
            .post(format!("{BASE_URL}/submit"))
            .header("Content-Type", "application/octet-stream")
            .header("x-topics", r#"["tm_ship"]"#)
            .body(beef.clone())
            .send()
            .await
            .expect("First submit failed");
        assert!(resp1.status().is_success());
        let body1: serde_json::Value = resp1.json().await.unwrap();

        // Second submit (same BEEF)
        let resp2 = client()
            .post(format!("{BASE_URL}/submit"))
            .header("Content-Type", "application/octet-stream")
            .header("x-topics", r#"["tm_ship"]"#)
            .body(beef)
            .send()
            .await
            .expect("Second submit failed");
        assert!(resp2.status().is_success());
        let body2: serde_json::Value = resp2.json().await.unwrap();

        let admitted2 = body2["tm_ship"]["outputsToAdmit"]
            .as_array()
            .expect("outputsToAdmit should be array");

        assert!(
            admitted2.is_empty(),
            "Second submit should be deduped (empty outputsToAdmit), got: {body2}"
        );

        println!("First submit: {body1}");
        println!("Second submit (deduped): {body2}");
    });
}

// =============================================================================
// Admin evictOutpoint (with auth)
// =============================================================================

#[ignore]
#[test]
fn admin_evict_outpoint_invalid_body() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let token =
            std::env::var("ADMIN_TOKEN").unwrap_or_else(|_| "test-token-not-set".to_string());

        let resp = client()
            .post(format!("{BASE_URL}/admin/evictOutpoint"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .body("{}")
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            400,
            "Missing required fields should be 400, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("error should be JSON");
        assert_eq!(body["status"], "error");

        println!("Admin evictOutpoint invalid body: {body}");
    });
}

#[ignore]
#[test]
fn admin_evict_outpoint_nonexistent() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let token = std::env::var("ADMIN_TOKEN")
            .unwrap_or_else(|_| "test-token-not-set".to_string());

        let resp = client()
            .post(format!("{BASE_URL}/admin/evictOutpoint"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .body(r#"{"txid":"0000000000000000000000000000000000000000000000000000000000000000","outputIndex":0,"topic":"tm_ship"}"#)
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            200,
            "Evicting nonexistent outpoint should succeed (no-op), got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("response should be JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["message"], "Outpoint evicted");

        println!("Admin evictOutpoint nonexistent: {body}");
    });
}

// =============================================================================
// 13. Admin with wrong token
// =============================================================================

#[ignore]
#[test]
fn admin_with_wrong_token() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .post(format!("{BASE_URL}/admin/syncAdvertisements"))
            .header("Authorization", "Bearer wrong-token-value")
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            403,
            "Admin with wrong token should be 403, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("error should be JSON");
        assert_eq!(body["status"], "error");
        let msg = body["message"].as_str().unwrap_or("");
        assert!(
            msg.contains("Forbidden") || msg.contains("Invalid"),
            "Error should indicate invalid token, got: {msg}"
        );

        println!("Admin wrong token: {body}");
    });
}

// =============================================================================
// 14. Admin sync advertisements with correct token
// =============================================================================

#[ignore]
#[test]
fn admin_sync_advertisements() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Use ADMIN_TOKEN env var if set, otherwise use the dev default
        let token =
            std::env::var("ADMIN_TOKEN").unwrap_or_else(|_| "test-token-not-set".to_string());

        let resp = client()
            .post(format!("{BASE_URL}/admin/syncAdvertisements"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            200,
            "Admin sync with correct token should be 200, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("response should be JSON");
        assert_eq!(body["status"], "success");

        println!("Admin sync advertisements: {body}");
    });
}

// =============================================================================
// 15. Lookup with x-aggregation: yes (binary response)
// =============================================================================

/// Bitcoin-style VarInt decoder. Returns (value, bytes_consumed).
fn read_varint(data: &[u8]) -> (u64, usize) {
    assert!(!data.is_empty(), "VarInt: empty data");
    let first = data[0];
    if first < 0xfd {
        (first as u64, 1)
    } else if first == 0xfd {
        let val = u16::from_le_bytes([data[1], data[2]]);
        (val as u64, 3)
    } else if first == 0xfe {
        let val = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
        (val as u64, 5)
    } else {
        let val = u64::from_le_bytes([
            data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
        ]);
        (val, 9)
    }
}

#[ignore]
#[test]
fn lookup_ship_aggregated_binary() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // First, ensure there's data by submitting a SHIP BEEF
        let beef = parse_first_beef(SHIP_FIXTURE);
        let _ = client()
            .post(format!("{BASE_URL}/submit"))
            .header("Content-Type", "application/octet-stream")
            .header("x-topics", r#"["tm_ship"]"#)
            .body(beef)
            .send()
            .await
            .expect("Submit failed");

        // Now lookup with x-aggregation: yes
        let resp = client()
            .post(format!("{BASE_URL}/lookup"))
            .header("Content-Type", "application/json")
            .header("x-aggregation", "yes")
            .body(r#"{"service":"ls_ship","query":{}}"#)
            .send()
            .await
            .expect("Lookup request failed");

        assert!(
            resp.status().is_success(),
            "Aggregated lookup should succeed, got: {}",
            resp.status()
        );

        let content_type = resp
            .headers()
            .get("content-type")
            .expect("Missing Content-Type header")
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            content_type.contains("application/octet-stream"),
            "Expected application/octet-stream, got: {content_type}"
        );

        let bytes = resp.bytes().await.expect("Failed to read response bytes");
        assert!(!bytes.is_empty(), "Aggregated response should not be empty");

        println!("Aggregated binary response: {} bytes", bytes.len());

        // Parse the binary format
        let mut pos = 0;

        // Read number of outputs
        let (num_outputs, consumed) = read_varint(&bytes[pos..]);
        pos += consumed;
        println!("Number of outputs: {num_outputs}");

        assert!(
            num_outputs > 0,
            "Should have at least one output after submit"
        );

        // Read each output's metadata
        for i in 0..num_outputs {
            assert!(
                pos + 32 <= bytes.len(),
                "Output {i}: not enough bytes for txid"
            );
            let txid_bytes = &bytes[pos..pos + 32];
            pos += 32;

            let txid_hex: String = txid_bytes.iter().map(|b| format!("{b:02x}")).collect();
            println!("Output {i}: txid={txid_hex}");

            let (output_index, consumed) = read_varint(&bytes[pos..]);
            pos += consumed;
            println!("Output {i}: outputIndex={output_index}");

            let (ctx_len, consumed) = read_varint(&bytes[pos..]);
            pos += consumed;

            if ctx_len > 0 {
                assert!(
                    pos + ctx_len as usize <= bytes.len(),
                    "Output {i}: not enough bytes for context"
                );
                pos += ctx_len as usize;
            }
            println!("Output {i}: contextLen={ctx_len}");
        }

        // Remaining bytes should be BEEF data
        let beef_data = &bytes[pos..];
        assert!(
            !beef_data.is_empty(),
            "BEEF data should not be empty after output metadata"
        );
        println!("BEEF data: {} bytes", beef_data.len());
    });
}

// =============================================================================
// 16. ARC ingest with invalid body
// =============================================================================

#[ignore]
#[test]
fn arc_ingest_invalid_body() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .post(format!("{BASE_URL}/arc-ingest"))
            .header("Content-Type", "application/json")
            .body("not valid json at all")
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(
            resp.status(),
            400,
            "Invalid arc-ingest body should be 400, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("error should be JSON");
        assert_eq!(body["status"], "error");

        println!("Arc ingest invalid body: {body}");
    });
}

// =============================================================================
// 17. Web UI dashboard returns HTML
// =============================================================================

#[ignore]
#[test]
fn web_ui_returns_html() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .get(format!("{BASE_URL}/"))
            .send()
            .await
            .expect("HTTP request failed");

        assert_eq!(resp.status(), 200, "GET / should return 200");

        let content_type = resp
            .headers()
            .get("content-type")
            .expect("Missing Content-Type header")
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            content_type.contains("text/html"),
            "Expected text/html, got: {content_type}"
        );

        let body = resp.text().await.unwrap();
        assert!(
            body.contains("rust-overlay"),
            "HTML should contain 'rust-overlay', got {} bytes",
            body.len()
        );
        assert!(
            body.contains("Overlay Services"),
            "HTML should contain 'Overlay Services'"
        );
        assert!(
            body.contains("tm_ship"),
            "HTML should list tm_ship topic manager"
        );
        assert!(
            body.contains("ls_ship"),
            "HTML should list ls_ship lookup service"
        );

        println!("Web UI: {} bytes, content-type: {content_type}", body.len());
    });
}

// =============================================================================
// 18. Admin janitor (live)
// =============================================================================

#[ignore]
#[test]
fn admin_janitor() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let token =
            std::env::var("ADMIN_TOKEN").unwrap_or_else(|_| "test-token-not-set".to_string());

        let resp = client()
            .post(format!("{BASE_URL}/admin/janitor"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .expect("HTTP request failed");

        assert!(
            resp.status().is_success(),
            "Janitor should succeed, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.expect("response should be JSON");

        // The janitor returns a JanitorResult with these fields
        assert!(
            body.get("ship_records_checked").is_some() || body.get("shipRecordsChecked").is_some(),
            "Response should contain ship_records_checked, got: {body}"
        );

        println!("Admin janitor: {body}");
    });
}

// =============================================================================
// 19. Submit returns fast (onSteakReady pattern) — sub-2s response
// =============================================================================

#[ignore]
#[test]
fn submit_returns_fast_with_steak() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let beef = parse_first_beef(SHIP_FIXTURE);

        let start = std::time::Instant::now();

        let resp = client()
            .post(format!("{BASE_URL}/submit"))
            .header("Content-Type", "application/octet-stream")
            .header("x-topics", r#"["tm_ship"]"#)
            .body(beef)
            .send()
            .await
            .expect("HTTP request failed");

        let elapsed = start.elapsed();

        assert!(
            resp.status().is_success(),
            "Submit should succeed, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.unwrap();

        // onSteakReady: response should come back BEFORE mutations complete
        // The worker returns the Steak (validation result) immediately and
        // enqueues mutations via wait_until / queue.
        assert!(
            elapsed.as_millis() < 2000,
            "Submit should return in under 2s (onSteakReady), took: {}ms",
            elapsed.as_millis()
        );

        // Response should contain topic keys with admittance instructions
        assert!(
            body.get("tm_ship").is_some(),
            "Response should contain tm_ship steak, got: {body}"
        );

        println!(
            "Submit response time: {}ms, body: {}",
            elapsed.as_millis(),
            body
        );
    });
}

// =============================================================================
// 20. Submit then delayed lookup — verify mutations processed
// =============================================================================

#[ignore]
#[test]
fn submit_then_delayed_lookup() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Step 1: Submit BEEF
        let beef = parse_first_beef(SHIP_FIXTURE);

        let resp = client()
            .post(format!("{BASE_URL}/submit"))
            .header("Content-Type", "application/octet-stream")
            .header("x-topics", r#"["tm_ship"]"#)
            .body(beef)
            .send()
            .await
            .expect("Submit failed");

        assert!(resp.status().is_success());
        let submit_body: serde_json::Value = resp.json().await.unwrap();
        println!("Submit response: {submit_body}");

        // Step 2: Wait 3 seconds for mutations to be processed
        // (via wait_until or queue consumer)
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Step 3: Lookup should find outputs
        let lookup_resp = client()
            .post(format!("{BASE_URL}/lookup"))
            .header("Content-Type", "application/json")
            .body(r#"{"service":"ls_ship","query":{}}"#)
            .send()
            .await
            .expect("Lookup failed");

        assert!(lookup_resp.status().is_success());

        let lookup_body: serde_json::Value = lookup_resp.json().await.unwrap();

        assert_eq!(
            lookup_body["type"], "output-list",
            "Expected output-list, got: {lookup_body}"
        );

        let outputs = lookup_body["outputs"]
            .as_array()
            .expect("outputs should be array");

        assert!(
            !outputs.is_empty(),
            "Lookup should return outputs after submit + 3s delay (mutations should be processed)"
        );

        println!(
            "Delayed lookup: {} outputs found after submit + 3s wait",
            outputs.len()
        );
    });
}

// =============================================================================
// 21. Cross-overlay: list topic managers from BSV Association node
// =============================================================================

#[ignore]
#[test]
fn cross_overlay_list_topic_managers() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let peers = &[
            ("bsvb.tech US", "https://overlay-us-1.bsvb.tech"),
            ("bapp.dev", "https://users.bapp.dev"),
            (
                "type-stamp",
                "https://type-stamp-overlay-2-production.up.railway.app",
            ),
        ];

        for (name, url) in peers {
            let resp = client()
                .get(format!("{url}/listTopicManagers"))
                .timeout(std::time::Duration::from_secs(15))
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap();
                    let obj = body.as_object().expect("should be object");

                    // All BSV overlay nodes should have tm_ship and tm_slap
                    assert!(
                        obj.contains_key("tm_ship"),
                        "{name}: Missing tm_ship in: {:?}",
                        obj.keys().collect::<Vec<_>>()
                    );
                    assert!(
                        obj.contains_key("tm_slap"),
                        "{name}: Missing tm_slap in: {:?}",
                        obj.keys().collect::<Vec<_>>()
                    );

                    println!(
                        "PASS: {name} has {} topic managers: {:?}",
                        obj.len(),
                        obj.keys().collect::<Vec<_>>()
                    );
                }
                Ok(r) => {
                    panic!("{name}: unexpected status {}", r.status());
                }
                Err(e) => {
                    panic!("{name}: request failed: {e}");
                }
            }
        }
    });
}

// =============================================================================
// 22. Cross-overlay: lookup SHIP records from peer overlay
// =============================================================================

#[ignore]
#[test]
fn cross_overlay_lookup_ship() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Query the BSV Association overlay for all SHIP records
        let resp = client()
            .post("https://overlay-us-1.bsvb.tech/lookup")
            .header("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(15))
            .body(r#"{"service":"ls_ship","query":{"findAll":true}}"#)
            .send()
            .await
            .expect("SHIP lookup on peer failed");

        assert!(
            resp.status().is_success(),
            "Peer SHIP lookup should succeed, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.unwrap();

        assert_eq!(
            body["type"],
            "output-list",
            "Expected output-list, got type: {:?}",
            body.get("type")
        );

        let outputs = body["outputs"].as_array().expect("outputs should be array");

        assert!(
            !outputs.is_empty(),
            "BSV Association overlay should have SHIP records"
        );

        // Verify each output has BEEF and outputIndex
        for (i, output) in outputs.iter().take(5).enumerate() {
            assert!(
                output.get("beef").is_some(),
                "Output {i} should have 'beef'"
            );
            assert!(
                output.get("outputIndex").is_some(),
                "Output {i} should have 'outputIndex'"
            );
        }

        println!("PASS: Peer overlay has {} SHIP records", outputs.len());

        // Also query bapp.dev
        let resp2 = client()
            .post("https://users.bapp.dev/lookup")
            .header("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(15))
            .body(r#"{"service":"ls_ship","query":{"findAll":true}}"#)
            .send()
            .await
            .expect("bapp.dev SHIP lookup failed");

        assert!(resp2.status().is_success());
        let body2: serde_json::Value = resp2.json().await.unwrap();
        let outputs2 = body2["outputs"].as_array().expect("outputs array");
        assert!(!outputs2.is_empty(), "bapp.dev should have SHIP records");
        println!("PASS: bapp.dev has {} SHIP records", outputs2.len());
    });
}

// =============================================================================
// 23. Cross-overlay: GASP sync — verify /admin/startGASPSync
// =============================================================================

#[ignore]
#[test]
fn cross_overlay_gasp_sync() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let token =
            std::env::var("ADMIN_TOKEN").unwrap_or_else(|_| "<your-admin-token>".to_string());

        let resp = client()
            .post(format!("{BASE_URL}/admin/startGASPSync"))
            .header("Authorization", format!("Bearer {token}"))
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .expect("GASP sync request failed");

        assert!(
            resp.status().is_success(),
            "GASP sync should succeed, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.unwrap();

        // Response should have topics_synced map
        assert!(
            body.get("topics_synced").is_some(),
            "Response should contain topics_synced, got: {body}"
        );

        let topics_synced = body["topics_synced"]
            .as_object()
            .expect("topics_synced should be object");

        println!(
            "PASS: GASP sync completed — {} topics synced: {:?}",
            topics_synced.len(),
            topics_synced.keys().collect::<Vec<_>>()
        );

        // Report per-topic details if available
        for (topic, result) in topics_synced {
            let peers = result
                .get("peers")
                .and_then(|p| p.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            println!("  {topic}: {peers} peers synced");
        }
    });
}

// =============================================================================
// 24. GASP requestSyncResponse on our node — returns UTXOList
// =============================================================================

#[ignore]
#[test]
fn gasp_request_sync_response_our_node() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for topic in &["tm_ship", "tm_slap"] {
            let resp = client()
                .post(format!("{BASE_URL}/requestSyncResponse"))
                .header("Content-Type", "application/json")
                .header("X-BSV-Topic", *topic)
                .timeout(std::time::Duration::from_secs(15))
                .body(r#"{"version":1,"since":0}"#)
                .send()
                .await
                .expect("requestSyncResponse failed");

            assert!(
                resp.status().is_success(),
                "requestSyncResponse for {topic} should succeed, got: {}",
                resp.status()
            );

            let body: serde_json::Value = resp.json().await.unwrap();

            let utxos = body["UTXOList"]
                .as_array()
                .expect("UTXOList should be array");

            // Each UTXO should have txid and outputIndex
            for (i, utxo) in utxos.iter().take(3).enumerate() {
                let txid = utxo["txid"].as_str().expect("txid should be string");
                assert_eq!(txid.len(), 64, "txid should be 64 hex chars");
                assert!(
                    utxo.get("outputIndex").is_some(),
                    "UTXO {i} should have outputIndex"
                );
            }

            println!(
                "PASS: requestSyncResponse({topic}) returned {} UTXOs",
                utxos.len()
            );
        }
    });
}

// =============================================================================
// 25. Cross-overlay: GASP requestSyncResponse from type-stamp overlay
// =============================================================================

#[ignore]
#[test]
fn cross_overlay_gasp_request_sync_response() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = client()
            .post("https://type-stamp-overlay-2-production.up.railway.app/requestSyncResponse")
            .header("Content-Type", "application/json")
            .header("X-BSV-Topic", "tm_ship")
            .timeout(std::time::Duration::from_secs(15))
            .body(r#"{"version":1,"since":0}"#)
            .send()
            .await
            .expect("Peer GASP request failed");

        assert!(
            resp.status().is_success(),
            "Peer GASP should succeed, got: {}",
            resp.status()
        );

        let body: serde_json::Value = resp.json().await.unwrap();

        let utxos = body["UTXOList"]
            .as_array()
            .expect("UTXOList should be array");

        assert!(
            !utxos.is_empty(),
            "type-stamp overlay should have SHIP UTXOs"
        );

        // Verify format
        let first = &utxos[0];
        assert!(
            first["txid"].as_str().is_some(),
            "UTXO should have string txid"
        );
        assert!(
            first.get("outputIndex").is_some(),
            "UTXO should have outputIndex"
        );

        println!(
            "PASS: type-stamp overlay returned {} SHIP UTXOs via GASP",
            utxos.len()
        );
    });
}

// =============================================================================
// 26. Cross-overlay: list lookup service providers from peers
// =============================================================================

#[ignore]
#[test]
fn cross_overlay_list_lookup_services() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let peers = &[
            ("bsvb.tech US", "https://overlay-us-1.bsvb.tech"),
            ("bapp.dev", "https://users.bapp.dev"),
        ];

        for (name, url) in peers {
            let resp = client()
                .get(format!("{url}/listLookupServiceProviders"))
                .timeout(std::time::Duration::from_secs(15))
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap();
                    let obj = body.as_object().expect("should be object");

                    assert!(obj.contains_key("ls_ship"), "{name}: Missing ls_ship");
                    assert!(obj.contains_key("ls_slap"), "{name}: Missing ls_slap");

                    println!(
                        "PASS: {name} has {} lookup services: {:?}",
                        obj.len(),
                        obj.keys().collect::<Vec<_>>()
                    );
                }
                Ok(r) => panic!("{name}: unexpected status {}", r.status()),
                Err(e) => panic!("{name}: request failed: {e}"),
            }
        }
    });
}

// =============================================================================
// 27. Wire compatibility: our GASP format matches peer overlay format
// =============================================================================

#[ignore]
#[test]
fn gasp_wire_format_matches_peers() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Get our GASP response
        let our_resp = client()
            .post(format!("{BASE_URL}/requestSyncResponse"))
            .header("Content-Type", "application/json")
            .header("X-BSV-Topic", "tm_ship")
            .timeout(std::time::Duration::from_secs(15))
            .body(r#"{"version":1,"since":0}"#)
            .send()
            .await
            .expect("Our GASP request failed");

        assert!(our_resp.status().is_success());
        let our_body: serde_json::Value = our_resp.json().await.unwrap();

        // Get peer GASP response
        let peer_resp = client()
            .post("https://type-stamp-overlay-2-production.up.railway.app/requestSyncResponse")
            .header("Content-Type", "application/json")
            .header("X-BSV-Topic", "tm_ship")
            .timeout(std::time::Duration::from_secs(15))
            .body(r#"{"version":1,"since":0}"#)
            .send()
            .await
            .expect("Peer GASP request failed");

        assert!(peer_resp.status().is_success());
        let peer_body: serde_json::Value = peer_resp.json().await.unwrap();

        // Both should have UTXOList at top level
        assert!(
            our_body.get("UTXOList").is_some(),
            "Our response should have UTXOList"
        );
        assert!(
            peer_body.get("UTXOList").is_some(),
            "Peer response should have UTXOList"
        );

        // Both should have since field
        assert!(
            our_body.get("since").is_some(),
            "Our response should have 'since' field"
        );

        // Verify UTXO format is the same shape
        let our_utxos = our_body["UTXOList"].as_array().unwrap();
        let peer_utxos = peer_body["UTXOList"].as_array().unwrap();

        if let Some(our_first) = our_utxos.first() {
            assert!(our_first.get("txid").is_some(), "Our UTXO should have txid");
            assert!(
                our_first.get("outputIndex").is_some(),
                "Our UTXO should have outputIndex"
            );
        }

        if let Some(peer_first) = peer_utxos.first() {
            assert!(
                peer_first.get("txid").is_some(),
                "Peer UTXO should have txid"
            );
            assert!(
                peer_first.get("outputIndex").is_some(),
                "Peer UTXO should have outputIndex"
            );
        }

        println!(
            "PASS: GASP wire format matches — our UTXOs: {}, peer UTXOs: {}",
            our_utxos.len(),
            peer_utxos.len()
        );
    });
}

// =============================================================================
// 28. Lookup compatibility: our output-list format matches peer format
// =============================================================================

#[ignore]
#[test]
fn lookup_wire_format_matches_peers() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Our node SHIP lookup
        let our_resp = client()
            .post(format!("{BASE_URL}/lookup"))
            .header("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(15))
            .body(r#"{"service":"ls_ship","query":{}}"#)
            .send()
            .await
            .expect("Our lookup failed");

        assert!(our_resp.status().is_success());
        let our_body: serde_json::Value = our_resp.json().await.unwrap();

        // Peer SHIP lookup
        let peer_resp = client()
            .post("https://users.bapp.dev/lookup")
            .header("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(15))
            .body(r#"{"service":"ls_ship","query":{"findAll":true}}"#)
            .send()
            .await
            .expect("Peer lookup failed");

        assert!(peer_resp.status().is_success());
        let peer_body: serde_json::Value = peer_resp.json().await.unwrap();

        // Both should be output-list type
        assert_eq!(our_body["type"], "output-list");
        assert_eq!(peer_body["type"], "output-list");

        // Both should have outputs array
        let our_outputs = our_body["outputs"].as_array().expect("our outputs");
        let peer_outputs = peer_body["outputs"].as_array().expect("peer outputs");

        // Verify format of individual outputs matches
        if let Some(our_first) = our_outputs.first() {
            assert!(our_first.get("beef").is_some(), "Our output has 'beef'");
            assert!(
                our_first.get("outputIndex").is_some(),
                "Our output has 'outputIndex'"
            );
            // BEEF should be a byte array
            assert!(our_first["beef"].is_array(), "Our BEEF should be array");
        }

        if let Some(peer_first) = peer_outputs.first() {
            assert!(peer_first.get("beef").is_some(), "Peer output has 'beef'");
            assert!(
                peer_first.get("outputIndex").is_some(),
                "Peer output has 'outputIndex'"
            );
            assert!(peer_first["beef"].is_array(), "Peer BEEF should be array");
        }

        println!(
            "PASS: Lookup wire format matches — our outputs: {}, peer outputs: {}",
            our_outputs.len(),
            peer_outputs.len()
        );
    });
}
