//! Worker-side HealthChecker — uses Cloudflare Workers Fetch API to check
//! if overlay hosts respond to health-check requests.

use async_trait::async_trait;
use overlay_engine::health_checker::HealthChecker;
use serde::Deserialize;

/// HealthChecker implementation using Cloudflare Workers `Fetch` API.
///
/// Sends a GET request to `{url}/health` and checks for a JSON response
/// with `{ "status": "ok" }`.
pub struct WorkerHealthChecker;

#[derive(Deserialize)]
struct HealthResponse {
    status: String,
}

#[async_trait(?Send)]
impl HealthChecker for WorkerHealthChecker {
    async fn check_health(&self, url: &str) -> Result<bool, String> {
        // Ensure the URL has a protocol
        let base_url = if url.starts_with("http") {
            url.to_string()
        } else {
            format!("https://{url}")
        };

        let health_url = format!("{}/health", base_url.trim_end_matches('/'));

        // Build the GET request
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);

        let headers = worker::Headers::new();
        let _ = headers.set("Accept", "application/json");
        init.with_headers(headers);

        let request = match worker::Request::new_with_init(&health_url, &init) {
            Ok(r) => r,
            Err(e) => return Err(format!("Failed to create request for {health_url}: {e}")),
        };

        let mut response = match worker::Fetch::Request(request).send().await {
            Ok(r) => r,
            Err(_) => return Ok(false), // Connection error = unhealthy
        };

        let status = response.status_code();
        if !(200..300).contains(&status) {
            return Ok(false);
        }

        // Try to parse the response body as JSON
        let body: HealthResponse = match response.json().await {
            Ok(b) => b,
            Err(_) => return Ok(false), // Can't parse = unhealthy
        };

        Ok(body.status == "ok")
    }
}
