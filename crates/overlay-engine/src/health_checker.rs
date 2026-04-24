//! HealthChecker trait — checks if an overlay host is reachable.
//!
//! Used by the Janitor service to verify that SHIP/SLAP-advertised hosts
//! are still operational. The trait abstracts the HTTP transport so the engine
//! crate stays platform-agnostic (no HTTP dependencies).
//!
//! Implementations (Cloudflare Workers `Fetch`, reqwest, etc.) live in
//! deployment crates.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Checks whether an overlay host's health endpoint responds successfully.
///
/// The Janitor calls `check_health()` for each unique domain discovered
/// from SHIP/SLAP records. Implementations should GET `{url}/health` and
/// verify a 200 response with `{ "status": "ok" }`.
#[async_trait(?Send)]
pub trait HealthChecker {
    /// Check if the given host URL is healthy.
    ///
    /// The implementation should:
    /// 1. Construct the health URL: `{url}/health`
    /// 2. Send a GET request with a timeout
    /// 3. Verify the response is HTTP 200 and contains `{ "status": "ok" }`
    ///
    /// Returns `Ok(true)` if healthy, `Ok(false)` if unhealthy (timeout, bad status,
    /// connection error, wrong response), or `Err` for unexpected failures.
    async fn check_health(&self, url: &str) -> Result<bool, String>;
}

/// Configuration for the Janitor health-check service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JanitorConfig {
    /// Timeout in milliseconds for each health-check request.
    /// Default: 10000 (10 seconds).
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// Number of consecutive failures before revoking/deleting a record.
    /// Default: 3.
    #[serde(default = "default_host_down_revoke_score")]
    pub host_down_revoke_score: u32,
}

fn default_request_timeout_ms() -> u64 {
    10_000
}

fn default_host_down_revoke_score() -> u32 {
    3
}

impl Default for JanitorConfig {
    fn default() -> Self {
        Self {
            request_timeout_ms: default_request_timeout_ms(),
            host_down_revoke_score: default_host_down_revoke_score(),
        }
    }
}

/// Summary of a single janitor run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JanitorResult {
    /// Number of SHIP records checked.
    pub ship_records_checked: u32,
    /// Number of SLAP records checked.
    pub slap_records_checked: u32,
    /// Number of records evicted (exceeded down threshold).
    pub records_evicted: u32,
    /// Number of unique domains that were healthy.
    pub domains_healthy: u32,
    /// Number of unique domains that were unhealthy.
    pub domains_unhealthy: u32,
    /// Per-domain details (domain -> healthy bool).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub domain_results: Vec<DomainHealthResult>,
}

/// Health-check result for a single domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainHealthResult {
    pub domain: String,
    pub healthy: bool,
    /// Number of records associated with this domain.
    pub record_count: u32,
    /// Number of records evicted for this domain.
    pub records_evicted: u32,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn janitor_config_defaults() {
        let config = JanitorConfig::default();
        assert_eq!(config.request_timeout_ms, 10_000);
        assert_eq!(config.host_down_revoke_score, 3);
    }

    #[test]
    fn janitor_config_serde_roundtrip() {
        let config = JanitorConfig {
            request_timeout_ms: 5000,
            host_down_revoke_score: 5,
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: JanitorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.request_timeout_ms, 5000);
        assert_eq!(back.host_down_revoke_score, 5);
    }

    #[test]
    fn janitor_result_defaults() {
        let result = JanitorResult::default();
        assert_eq!(result.ship_records_checked, 0);
        assert_eq!(result.slap_records_checked, 0);
        assert_eq!(result.records_evicted, 0);
        assert_eq!(result.domains_healthy, 0);
        assert_eq!(result.domains_unhealthy, 0);
        assert!(result.domain_results.is_empty());
    }

    #[test]
    fn janitor_result_serde_roundtrip() {
        let result = JanitorResult {
            ship_records_checked: 10,
            slap_records_checked: 5,
            records_evicted: 2,
            domains_healthy: 3,
            domains_unhealthy: 1,
            domain_results: vec![
                DomainHealthResult {
                    domain: "https://example.com".to_string(),
                    healthy: true,
                    record_count: 5,
                    records_evicted: 0,
                },
                DomainHealthResult {
                    domain: "https://down.example.com".to_string(),
                    healthy: false,
                    record_count: 2,
                    records_evicted: 2,
                },
            ],
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: JanitorResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.ship_records_checked, 10);
        assert_eq!(back.domain_results.len(), 2);
        assert!(back.domain_results[0].healthy);
        assert!(!back.domain_results[1].healthy);
    }

    #[test]
    fn domain_health_result_serde() {
        let r = DomainHealthResult {
            domain: "https://test.com".to_string(),
            healthy: true,
            record_count: 3,
            records_evicted: 0,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("test.com"));
        let back: DomainHealthResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.domain, "https://test.com");
        assert!(back.healthy);
    }

    /// Verify HealthChecker is object-safe.
    #[allow(dead_code)]
    fn assert_object_safe(_: &dyn HealthChecker) {}
}
