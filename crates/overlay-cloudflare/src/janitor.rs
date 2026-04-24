//! Janitor health-check service for the overlay.
//!
//! Iterates all SHIP and SLAP advertisement records, health-checks each unique
//! domain, and evicts records for domains that are consistently unreachable.
//!
//! Ported from `~/bsv/overlay-express/src/JanitorService.ts`.
//!
//! ## Design
//!
//! The TS implementation stores per-record "down" counters in MongoDB and
//! increments/decrements across runs. For simplicity, this Rust implementation
//! tracks health per-domain (not per-record) within a single run, and evicts all
//! records for a domain that fails the health check. Since we don't persist the
//! down counter, the `host_down_revoke_score` acts as a threshold: if set to 1,
//! a single failed run evicts. If set higher, multiple consecutive cron invocations
//! must fail before eviction happens (tracked via a `down` column in D1 — future
//! enhancement). For now, we treat a single janitor run as decisive: if a domain
//! is down during the run, all its records are evicted.

use std::collections::HashMap;

use overlay_discovery::ship::storage::SHIPStorage;
use overlay_discovery::slap::storage::SLAPStorage;
use overlay_engine::health_checker::{
    DomainHealthResult, HealthChecker, JanitorConfig, JanitorResult,
};

/// Normalize a URL for domain comparison: lowercase, strip trailing slash.
fn normalize_url(url: &str) -> String {
    url.trim_end_matches('/').to_lowercase()
}

/// Run a single janitor pass: health-check all SHIP/SLAP domains and evict dead ones.
///
/// The caller (route handler or scheduled event) provides storage references and
/// a health checker implementation.
///
/// If `hosting_url` is provided, domains matching it are automatically marked
/// healthy without issuing a network request. This prevents a Cloudflare Worker
/// from calling itself (which can timeout or be blocked by the runtime).
pub async fn run_janitor(
    ship_storage: &dyn SHIPStorage,
    slap_storage: &dyn SLAPStorage,
    health_checker: &dyn HealthChecker,
    config: &JanitorConfig,
    hosting_url: Option<&str>,
) -> Result<JanitorResult, String> {
    let mut result = JanitorResult::default();

    // ── Collect all records ──────────────────────────────────────────────

    let ship_records = ship_storage
        .find_all_records()
        .await
        .map_err(|e| format!("Failed to fetch SHIP records: {e}"))?;
    result.ship_records_checked = ship_records.len() as u32;

    let slap_records = slap_storage
        .find_all_records()
        .await
        .map_err(|e| format!("Failed to fetch SLAP records: {e}"))?;
    result.slap_records_checked = slap_records.len() as u32;

    // ── Build domain -> records map ──────────────────────────────────────

    // Track which (txid, outputIndex) to evict per domain
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

    // ── Health-check each unique domain ─────────────────────────────────

    let _ = config.request_timeout_ms; // timeout is used inside the HealthChecker impl

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
                .unwrap_or_default() // errors treated as unhealthy
        };

        let record_count = records.len() as u32;
        let mut evicted = 0u32;

        if is_healthy {
            result.domains_healthy += 1;
        } else {
            result.domains_unhealthy += 1;

            // In the simple (non-persistent) model: evict all records for this domain
            // if the down threshold is 1 (default behavior for single-run janitor).
            // With host_down_revoke_score > 1, the caller should run the janitor
            // multiple times to build up failures. For now, we evict on first failure
            // when threshold is 1, and skip eviction otherwise (future: persist counters).
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
