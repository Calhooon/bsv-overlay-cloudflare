//! Janitor health-check integration tests.
//!
//! Tests the janitor logic using mock HealthChecker and in-memory SHIP/SLAP storage.

use async_trait::async_trait;
use overlay_discovery::ship::storage::MemorySHIPStorage;
use overlay_discovery::slap::storage::MemorySLAPStorage;
use bsv_overlay_engine::health_checker::{HealthChecker, JanitorConfig, JanitorResult};

use std::collections::HashSet;
use std::sync::Mutex;

// ============================================================================
// Mock HealthChecker
// ============================================================================

/// A mock HealthChecker that returns healthy/unhealthy based on configuration.
struct MockHealthChecker {
    /// Domains that should be reported as healthy.
    healthy_domains: HashSet<String>,
    /// Track which domains were checked.
    checked_domains: Mutex<Vec<String>>,
}

impl MockHealthChecker {
    fn all_healthy() -> Self {
        Self {
            healthy_domains: HashSet::new(), // empty means "all healthy"
            checked_domains: Mutex::new(Vec::new()),
        }
    }

    fn with_healthy_domains(domains: &[&str]) -> Self {
        Self {
            healthy_domains: domains.iter().map(|s| s.to_string()).collect(),
            checked_domains: Mutex::new(Vec::new()),
        }
    }

    fn all_down() -> Self {
        // Use a sentinel: healthy_domains is non-empty but contains no real domains
        Self {
            healthy_domains: ["__none__"].iter().map(|s| s.to_string()).collect(),
            checked_domains: Mutex::new(Vec::new()),
        }
    }

    fn checked_count(&self) -> usize {
        self.checked_domains.lock().unwrap().len()
    }

    fn was_checked(&self, domain: &str) -> bool {
        self.checked_domains
            .lock()
            .unwrap()
            .iter()
            .any(|d| d == domain)
    }
}

#[async_trait(?Send)]
impl HealthChecker for MockHealthChecker {
    async fn check_health(&self, url: &str) -> Result<bool, String> {
        self.checked_domains.lock().unwrap().push(url.to_string());

        if self.healthy_domains.is_empty() {
            // Default: all healthy
            return Ok(true);
        }

        Ok(self.healthy_domains.contains(url))
    }
}

// ============================================================================
// Inline janitor logic (mirrors overlay-cloudflare::janitor::run_janitor)
// ============================================================================
// We duplicate the core janitor logic here since the overlay-engine tests can't
// depend on overlay-cloudflare (which is a cdylib). This keeps the test
// self-contained while verifying the same algorithm.

use overlay_discovery::ship::storage::SHIPStorage;
use overlay_discovery::slap::storage::SLAPStorage;
use bsv_overlay_engine::health_checker::DomainHealthResult;
use std::collections::HashMap;

/// Normalize a URL for domain comparison: lowercase, strip trailing slash.
fn normalize_url(url: &str) -> String {
    url.trim_end_matches('/').to_lowercase()
}

async fn run_janitor_test(
    ship_storage: &dyn SHIPStorage,
    slap_storage: &dyn SLAPStorage,
    health_checker: &dyn HealthChecker,
    config: &JanitorConfig,
) -> Result<JanitorResult, String> {
    run_janitor_test_with_hosting(ship_storage, slap_storage, health_checker, config, None).await
}

async fn run_janitor_test_with_hosting(
    ship_storage: &dyn SHIPStorage,
    slap_storage: &dyn SLAPStorage,
    health_checker: &dyn HealthChecker,
    config: &JanitorConfig,
    hosting_url: Option<&str>,
) -> Result<JanitorResult, String> {
    let mut result = JanitorResult::default();

    let ship_records = ship_storage
        .find_all_records()
        .await
        .map_err(|e| format!("SHIP: {e}"))?;
    result.ship_records_checked = ship_records.len() as u32;

    let slap_records = slap_storage
        .find_all_records()
        .await
        .map_err(|e| format!("SLAP: {e}"))?;
    result.slap_records_checked = slap_records.len() as u32;

    struct RecordRef {
        txid: String,
        output_index: u32,
        is_ship: bool,
    }

    let mut domain_records: HashMap<String, Vec<RecordRef>> = HashMap::new();

    for rec in &ship_records {
        domain_records
            .entry(rec.domain.clone())
            .or_default()
            .push(RecordRef {
                txid: rec.txid.clone(),
                output_index: rec.output_index,
                is_ship: true,
            });
    }

    for rec in &slap_records {
        domain_records
            .entry(rec.domain.clone())
            .or_default()
            .push(RecordRef {
                txid: rec.txid.clone(),
                output_index: rec.output_index,
                is_ship: false,
            });
    }

    let normalized_hosting = hosting_url.map(normalize_url);

    for (domain, records) in &domain_records {
        // Skip health-checking our own hosting URL — a CF Worker fetching
        // itself can timeout or be blocked by the runtime (issue #14).
        let is_self = normalized_hosting
            .as_ref()
            .is_some_and(|h| normalize_url(domain) == *h);

        let is_healthy = if is_self {
            true
        } else {
            health_checker
                .check_health(domain)
                .await
                .unwrap_or_default()
        };

        let record_count = records.len() as u32;
        let mut evicted = 0u32;

        if is_healthy {
            result.domains_healthy += 1;
        } else {
            result.domains_unhealthy += 1;

            if config.host_down_revoke_score <= 1 {
                for rec in records {
                    let evict_result = if rec.is_ship {
                        ship_storage
                            .delete_record(&rec.txid, rec.output_index)
                            .await
                            .map_err(|e| e.to_string())
                    } else {
                        slap_storage
                            .delete_record(&rec.txid, rec.output_index)
                            .await
                            .map_err(|e| e.to_string())
                    };

                    if evict_result.is_ok() {
                        evicted += 1;
                        result.records_evicted += 1;
                    }
                }
            }
        }

        result.domain_results.push(DomainHealthResult {
            domain: domain.clone(),
            healthy: is_healthy,
            record_count,
            records_evicted: evicted,
        });
    }

    Ok(result)
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn janitor_all_hosts_up_no_evictions() {
    let ship = MemorySHIPStorage::new();
    ship.store_record("tx1", 0, "k1", "https://healthy.com", "tm_test")
        .await
        .unwrap();
    ship.store_record("tx2", 0, "k2", "https://also-healthy.com", "tm_test")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();
    slap.store_record("tx3", 0, "k3", "https://healthy.com", "ls_test")
        .await
        .unwrap();

    let checker = MockHealthChecker::all_healthy();
    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    let result = run_janitor_test(&ship, &slap, &checker, &config)
        .await
        .unwrap();

    assert_eq!(result.ship_records_checked, 2);
    assert_eq!(result.slap_records_checked, 1);
    assert_eq!(result.records_evicted, 0);
    assert_eq!(result.domains_healthy, 2); // two unique domains
    assert_eq!(result.domains_unhealthy, 0);

    // Records should still exist
    assert_eq!(ship.record_count(), 2);
    assert_eq!(slap.record_count(), 1);
}

#[tokio::test]
async fn janitor_one_host_down_evicts_its_records() {
    let ship = MemorySHIPStorage::new();
    ship.store_record("tx1", 0, "k1", "https://healthy.com", "tm_a")
        .await
        .unwrap();
    ship.store_record("tx2", 0, "k2", "https://dead.com", "tm_b")
        .await
        .unwrap();
    ship.store_record("tx3", 0, "k3", "https://dead.com", "tm_c")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();

    let checker = MockHealthChecker::with_healthy_domains(&["https://healthy.com"]);
    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    let result = run_janitor_test(&ship, &slap, &checker, &config)
        .await
        .unwrap();

    assert_eq!(result.ship_records_checked, 3);
    assert_eq!(result.records_evicted, 2); // both dead.com records evicted
    assert_eq!(result.domains_healthy, 1);
    assert_eq!(result.domains_unhealthy, 1);

    // Only the healthy record should remain
    assert_eq!(ship.record_count(), 1);
    let remaining = ship.find_all_records().await.unwrap();
    assert_eq!(remaining[0].domain, "https://healthy.com");
}

#[tokio::test]
async fn janitor_all_hosts_down_evicts_everything() {
    let ship = MemorySHIPStorage::new();
    ship.store_record("tx1", 0, "k1", "https://down1.com", "tm_a")
        .await
        .unwrap();
    ship.store_record("tx2", 0, "k2", "https://down2.com", "tm_b")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();
    slap.store_record("tx3", 0, "k3", "https://down1.com", "ls_a")
        .await
        .unwrap();

    let checker = MockHealthChecker::all_down();
    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    let result = run_janitor_test(&ship, &slap, &checker, &config)
        .await
        .unwrap();

    assert_eq!(result.records_evicted, 3);
    assert_eq!(result.domains_unhealthy, 2);
    assert_eq!(ship.record_count(), 0);
    assert_eq!(slap.record_count(), 0);
}

#[tokio::test]
async fn janitor_empty_records_is_noop() {
    let ship = MemorySHIPStorage::new();
    let slap = MemorySLAPStorage::new();

    let checker = MockHealthChecker::all_healthy();
    let config = JanitorConfig::default();

    let result = run_janitor_test(&ship, &slap, &checker, &config)
        .await
        .unwrap();

    assert_eq!(result.ship_records_checked, 0);
    assert_eq!(result.slap_records_checked, 0);
    assert_eq!(result.records_evicted, 0);
    assert_eq!(result.domains_healthy, 0);
    assert_eq!(result.domains_unhealthy, 0);
    assert!(result.domain_results.is_empty());
}

#[tokio::test]
async fn janitor_checks_each_unique_domain_once() {
    let ship = MemorySHIPStorage::new();
    // Three records, same domain
    ship.store_record("tx1", 0, "k1", "https://example.com", "tm_a")
        .await
        .unwrap();
    ship.store_record("tx2", 0, "k2", "https://example.com", "tm_b")
        .await
        .unwrap();
    ship.store_record("tx3", 0, "k3", "https://example.com", "tm_c")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();

    let checker = MockHealthChecker::all_healthy();
    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    let result = run_janitor_test(&ship, &slap, &checker, &config)
        .await
        .unwrap();

    // Only one unique domain, so only one health check
    assert_eq!(checker.checked_count(), 1);
    assert!(checker.was_checked("https://example.com"));
    assert_eq!(result.domains_healthy, 1);
    assert_eq!(result.domain_results.len(), 1);
    assert_eq!(result.domain_results[0].record_count, 3);
}

#[tokio::test]
async fn janitor_high_threshold_skips_eviction() {
    let ship = MemorySHIPStorage::new();
    ship.store_record("tx1", 0, "k1", "https://flaky.com", "tm_a")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();

    let checker = MockHealthChecker::all_down();
    let config = JanitorConfig {
        host_down_revoke_score: 3, // needs 3 consecutive failures
        ..Default::default()
    };

    let result = run_janitor_test(&ship, &slap, &checker, &config)
        .await
        .unwrap();

    // Domain is unhealthy but not evicted (threshold not reached)
    assert_eq!(result.domains_unhealthy, 1);
    assert_eq!(result.records_evicted, 0);
    assert_eq!(ship.record_count(), 1); // record still exists
}

#[tokio::test]
async fn janitor_mixed_ship_and_slap_same_domain() {
    let ship = MemorySHIPStorage::new();
    ship.store_record("tx1", 0, "k1", "https://dead.com", "tm_foo")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();
    slap.store_record("tx2", 0, "k2", "https://dead.com", "ls_bar")
        .await
        .unwrap();

    let checker = MockHealthChecker::all_down();
    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    let result = run_janitor_test(&ship, &slap, &checker, &config)
        .await
        .unwrap();

    assert_eq!(result.ship_records_checked, 1);
    assert_eq!(result.slap_records_checked, 1);
    assert_eq!(result.records_evicted, 2); // both SHIP and SLAP evicted
    assert_eq!(ship.record_count(), 0);
    assert_eq!(slap.record_count(), 0);
}

#[tokio::test]
async fn janitor_result_domain_details() {
    let ship = MemorySHIPStorage::new();
    ship.store_record("tx1", 0, "k1", "https://up.com", "tm_a")
        .await
        .unwrap();
    ship.store_record("tx2", 0, "k2", "https://down.com", "tm_b")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();

    let checker = MockHealthChecker::with_healthy_domains(&["https://up.com"]);
    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    let result = run_janitor_test(&ship, &slap, &checker, &config)
        .await
        .unwrap();

    assert_eq!(result.domain_results.len(), 2);

    let up_result = result
        .domain_results
        .iter()
        .find(|d| d.domain == "https://up.com")
        .unwrap();
    assert!(up_result.healthy);
    assert_eq!(up_result.record_count, 1);
    assert_eq!(up_result.records_evicted, 0);

    let down_result = result
        .domain_results
        .iter()
        .find(|d| d.domain == "https://down.com")
        .unwrap();
    assert!(!down_result.healthy);
    assert_eq!(down_result.record_count, 1);
    assert_eq!(down_result.records_evicted, 1);
}

#[tokio::test]
async fn janitor_health_check_error_treated_as_unhealthy() {
    let ship = MemorySHIPStorage::new();
    ship.store_record("tx1", 0, "k1", "https://error.com", "tm_a")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();

    // Custom checker that returns Err
    struct ErrorChecker;
    #[async_trait(?Send)]
    impl HealthChecker for ErrorChecker {
        async fn check_health(&self, _url: &str) -> Result<bool, String> {
            Err("network error".to_string())
        }
    }

    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    let result = run_janitor_test(&ship, &slap, &ErrorChecker, &config)
        .await
        .unwrap();

    assert_eq!(result.domains_unhealthy, 1);
    assert_eq!(result.records_evicted, 1);
    assert_eq!(ship.record_count(), 0);
}

#[tokio::test]
async fn janitor_skips_own_hosting_url() {
    // Our own domain should be marked healthy without a health check,
    // even if the checker would report it as down (issue #14).
    let ship = MemorySHIPStorage::new();
    ship.store_record(
        "tx1",
        0,
        "k1",
        "https://<your-overlay>.workers.dev",
        "tm_a",
    )
    .await
    .unwrap();
    ship.store_record("tx2", 0, "k2", "https://other-host.com", "tm_b")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();

    // All domains report as down, but our own should be skipped
    let checker = MockHealthChecker::all_down();
    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    let result = run_janitor_test_with_hosting(
        &ship,
        &slap,
        &checker,
        &config,
        Some("https://<your-overlay>.workers.dev"),
    )
    .await
    .unwrap();

    // Our own domain is healthy (skipped), the other is unhealthy
    assert_eq!(result.domains_healthy, 1);
    assert_eq!(result.domains_unhealthy, 1);
    assert_eq!(result.records_evicted, 1); // only the other-host record

    // Our own record should still exist
    assert_eq!(ship.record_count(), 1);
    let remaining = ship.find_all_records().await.unwrap();
    assert_eq!(
        remaining[0].domain,
        "https://<your-overlay>.workers.dev"
    );

    // The health checker should NOT have been called for our own domain
    assert_eq!(checker.checked_count(), 1);
    assert!(!checker.was_checked("https://<your-overlay>.workers.dev"));
    assert!(checker.was_checked("https://other-host.com"));
}

#[tokio::test]
async fn janitor_skips_own_hosting_url_with_trailing_slash() {
    // Verify trailing slash normalization works for self-skip
    let ship = MemorySHIPStorage::new();
    ship.store_record("tx1", 0, "k1", "https://example.workers.dev/", "tm_a")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();

    let checker = MockHealthChecker::all_down();
    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    // hosting_url without trailing slash, record has trailing slash
    let result = run_janitor_test_with_hosting(
        &ship,
        &slap,
        &checker,
        &config,
        Some("https://example.workers.dev"),
    )
    .await
    .unwrap();

    assert_eq!(result.domains_healthy, 1);
    assert_eq!(result.domains_unhealthy, 0);
    assert_eq!(result.records_evicted, 0);
    assert_eq!(checker.checked_count(), 0); // no health checks made
}

#[tokio::test]
async fn janitor_no_hosting_url_checks_all_domains() {
    // When hosting_url is None, all domains should be checked normally
    let ship = MemorySHIPStorage::new();
    ship.store_record("tx1", 0, "k1", "https://example.com", "tm_a")
        .await
        .unwrap();

    let slap = MemorySLAPStorage::new();

    let checker = MockHealthChecker::all_down();
    let config = JanitorConfig {
        host_down_revoke_score: 1,
        ..Default::default()
    };

    let result = run_janitor_test_with_hosting(&ship, &slap, &checker, &config, None)
        .await
        .unwrap();

    assert_eq!(result.domains_unhealthy, 1);
    assert_eq!(result.records_evicted, 1);
    assert_eq!(checker.checked_count(), 1);
}
