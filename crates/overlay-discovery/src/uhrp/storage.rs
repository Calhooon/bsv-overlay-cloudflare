//! UHRP Storage trait — backend-agnostic storage for UHRP advertisement records.
//!
//! Parallels `super::super::ship::storage::SHIPStorage`. The concrete
//! implementation (D1, SQLite, in-memory) is provided by the deployment crate.
//!
//! There is no TypeScript reference for this storage trait — UHRP discovery
//! has never had a published lookup service. The shape here mirrors SHIP's
//! trait surface so the D1 adapter in `overlay-cloudflare` reuses the same
//! patterns.

use async_trait::async_trait;
use overlay_engine::types::UTXOReference;
use serde::{Deserialize, Serialize};

/// Sort order for query results. Mirrors SHIP's `SortOrder` so the JSON
/// wire shape is identical across discovery services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UHRPSortOrder {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

/// A UHRP discovery record carrying the fields the lookup service indexes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UHRPDiscoveryRecord {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    pub uhrp_url: String,
    pub identity_key: String,
    pub download_url: String,
    pub expiry_time: i64,
    pub content_length: i64,
}

/// Query parameters for finding UHRP records.
///
/// Wire-format field names match the TS reference
/// (`UHRPLookupServiceFactory.ts` — `uhrpUrl`, `hostIdentityKey`). Without
/// this serde rename, client queries from `@bsv/sdk` `LookupResolver` sent
/// `uhrpUrl`/`hostIdentityKey` which silently deserialized as `None`, so
/// every filtered query fell through to an unfiltered find_all.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UHRPQuery {
    /// If true, return all records with pagination.
    pub find_all: Option<bool>,
    /// Filter by canonical `uhrp://<base58check>` URL (matches TS `uhrpUrl`).
    pub uhrp_url: Option<String>,
    /// Filter by hex-encoded host identity key. TS reference wire name is
    /// `hostIdentityKey`; we keep the Rust field name `identity_key` but
    /// expose it on the wire as `hostIdentityKey` for parity.
    #[serde(rename = "hostIdentityKey")]
    pub identity_key: Option<String>,
    /// Pagination limit.
    pub limit: Option<u32>,
    /// Pagination offset.
    pub skip: Option<u32>,
    /// Sort order by creation time.
    pub sort_order: Option<UHRPSortOrder>,

    /// Opt-in: include past-expiry records in results. Default behavior
    /// (unset / `Some(false)`) hides adverts whose `expiry_time` is in
    /// the past vs [`UHRPQuery::now_unix_seconds`].
    ///
    /// Why opt-in rather than opt-out: `tm_uhrp` admits past-expiry
    /// advertisements for TS-parity (TS only rejects `expiry_time < 1`),
    /// so the hide-from-clients policy moves to the lookup layer.
    /// Callers that explicitly want to see expired records — e.g.
    /// historians, indexers doing retention audits — pass `Some(true)`.
    ///
    /// A record with `expiry_time == 0` is treated as "never expires"
    /// (canonical UHRP convention) and is always included.
    #[serde(rename = "includeExpired", skip_serializing_if = "Option::is_none")]
    pub include_expired: Option<bool>,

    /// "As-of" reference time in unix seconds for the expiry filter. When
    /// `None`, the storage impl uses its own clock (see
    /// [`current_unix_seconds_i64`]).
    ///
    /// Wire field — two legitimate uses:
    ///
    /// 1. **Tests** want determinism. Fixing `now` makes the
    ///    hide-past-expired behavior reproducible across wall-clock
    ///    advances. Alias `now_unix_seconds` on the wire.
    /// 2. **Audit / historian queries** — "what would a client have seen
    ///    at time T?" — set this to T and set `include_expired=false`.
    ///
    /// Not a security primitive: a client that lies about `now` only
    /// shifts their own view. The server still stores and indexes the
    /// truth. `include_expired=true` trivially reveals everything,
    /// which makes any "hidden via as-of" inference pointless.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub now_unix_seconds: Option<i64>,
}

/// Backend-agnostic storage for UHRP advertisement records.
#[async_trait(?Send)]
pub trait UHRPStorage {
    /// Check if a duplicate record exists for this uhrp_url + identity_key.
    ///
    /// UHRP adverts can legitimately repeat across expiry renewals, so
    /// dedupe here is tighter than SHIP: we compare `(txid, output_index)`
    /// uniqueness at the SQL level, not logical identity.
    async fn has_duplicate_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<bool, UHRPStorageError>;

    /// Store a new UHRP record.
    #[allow(clippy::too_many_arguments)]
    async fn store_record(
        &self,
        txid: &str,
        output_index: u32,
        uhrp_url: &str,
        identity_key: &str,
        download_url: &str,
        expiry_time: i64,
        content_length: i64,
    ) -> Result<(), UHRPStorageError>;

    /// Delete a UHRP record by UTXO reference.
    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), UHRPStorageError>;

    /// Find records matching a query.
    async fn find_record(&self, query: &UHRPQuery) -> Result<Vec<UTXOReference>, UHRPStorageError>;

    /// Find all records with optional pagination.
    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
        sort_order: Option<UHRPSortOrder>,
    ) -> Result<Vec<UTXOReference>, UHRPStorageError>;

    /// Find all discovery records with full metadata (for clients that
    /// want more than just a UTXO pointer).
    async fn find_all_records(&self) -> Result<Vec<UHRPDiscoveryRecord>, UHRPStorageError>;
}

/// UHRP storage errors.
#[derive(Debug, thiserror::Error)]
pub enum UHRPStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

/// Unix seconds as `i64`. Used when [`UHRPQuery::now_unix_seconds`] is
/// `None`, i.e. the caller didn't inject a time. Matches the cfg-gated
/// pattern in `topic_manager.rs::current_unix_seconds` — wasm32 routes
/// through `js_sys::Date` because `SystemTime` panics on Cloudflare
/// Workers; native uses `SystemTime`. A pre-epoch clock yields 0, which
/// makes every non-zero-expiry advert look unexpired — strictly safer
/// than panicking.
pub fn current_unix_seconds_i64() -> i64 {
    #[cfg(target_arch = "wasm32")]
    {
        (js_sys::Date::now() / 1000.0) as i64
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

#[derive(Debug, Clone)]
struct UHRPRecord {
    txid: String,
    output_index: u32,
    uhrp_url: String,
    identity_key: String,
    download_url: String,
    expiry_time: i64,
    content_length: i64,
    created_at: std::time::SystemTime,
}

/// In-memory UHRP storage for testing.
#[derive(Debug, Default)]
pub struct MemoryUHRPStorage {
    records: std::sync::Mutex<Vec<UHRPRecord>>,
}

impl MemoryUHRPStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        // Lock poisoning only happens if a thread panicked inside the
        // guard; in tests we treat that as a hard failure via `.expect`
        // rather than silently returning a stale count.
        self.records
            .lock()
            .expect("MemoryUHRPStorage mutex poisoned")
            .len()
    }
}

#[async_trait(?Send)]
impl UHRPStorage for MemoryUHRPStorage {
    async fn has_duplicate_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<bool, UHRPStorageError> {
        Ok(self
            .records
            .lock()
            .map_err(|e| UHRPStorageError::Other(e.to_string()))?
            .iter()
            .any(|r| r.txid == txid && r.output_index == output_index))
    }

    #[allow(clippy::too_many_arguments)]
    async fn store_record(
        &self,
        txid: &str,
        output_index: u32,
        uhrp_url: &str,
        identity_key: &str,
        download_url: &str,
        expiry_time: i64,
        content_length: i64,
    ) -> Result<(), UHRPStorageError> {
        self.records
            .lock()
            .map_err(|e| UHRPStorageError::Other(e.to_string()))?
            .push(UHRPRecord {
                txid: txid.into(),
                output_index,
                uhrp_url: uhrp_url.into(),
                identity_key: identity_key.into(),
                download_url: download_url.into(),
                expiry_time,
                content_length,
                created_at: std::time::SystemTime::now(),
            });
        Ok(())
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), UHRPStorageError> {
        self.records
            .lock()
            .map_err(|e| UHRPStorageError::Other(e.to_string()))?
            .retain(|r| !(r.txid == txid && r.output_index == output_index));
        Ok(())
    }

    async fn find_record(&self, query: &UHRPQuery) -> Result<Vec<UTXOReference>, UHRPStorageError> {
        let records = self
            .records
            .lock()
            .map_err(|e| UHRPStorageError::Other(e.to_string()))?;
        // Expiry filter: default behavior hides records whose expiry has
        // passed. Callers that explicitly want them (historians, retention
        // audits) set `include_expired = Some(true)`. A record with
        // `expiry_time == 0` is "never expires" — always visible. See
        // the UHRPQuery doc comment for the full rationale.
        let include_expired = query.include_expired.unwrap_or(false);
        let now = query
            .now_unix_seconds
            .unwrap_or_else(current_unix_seconds_i64);

        // Legacy-storage fallback: pre-2026-04-22 admissions indexed
        // `uhrp_url` as hex-of-hash. Post-fix admissions store canonical
        // `uhrp://<base58check>`. A client query in canonical form must
        // still match legacy records — so derive the hex-of-hash form
        // once and accept a hit on either stored representation.
        let hex_form_of_query = query.uhrp_url.as_deref().and_then(|u| {
            u.strip_prefix("uhrp://").and_then(|b58| {
                bsv_rs::primitives::encoding::from_base58_check(b58)
                    .ok()
                    .and_then(|(version, payload)| {
                        if version.len() == 1 && version[0] == 0x01 && payload.len() == 32 {
                            Some(hex::encode(&payload))
                        } else {
                            None
                        }
                    })
            })
        });

        let mut results: Vec<&UHRPRecord> = records
            .iter()
            .filter(|r| {
                if let Some(ref u) = query.uhrp_url {
                    let matches = r.uhrp_url == *u
                        || hex_form_of_query
                            .as_ref()
                            .is_some_and(|h| &r.uhrp_url == h);
                    if !matches {
                        return false;
                    }
                }
                if let Some(ref ik) = query.identity_key {
                    if r.identity_key != *ik {
                        return false;
                    }
                }
                if !include_expired && r.expiry_time != 0 && r.expiry_time < now {
                    return false;
                }
                true
            })
            .collect();

        match query.sort_order {
            Some(UHRPSortOrder::Asc) => results.sort_by_key(|r| r.created_at),
            _ => {
                results.sort_by_key(|r| r.created_at);
                results.reverse();
            }
        }

        let skip = query.skip.unwrap_or(0) as usize;
        let results: Vec<UTXOReference> = results
            .into_iter()
            .skip(skip)
            .take(query.limit.map_or(usize::MAX, |l| l as usize))
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect();

        Ok(results)
    }

    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
        sort_order: Option<UHRPSortOrder>,
    ) -> Result<Vec<UTXOReference>, UHRPStorageError> {
        self.find_record(&UHRPQuery {
            find_all: Some(true),
            limit,
            skip,
            sort_order,
            ..Default::default()
        })
        .await
    }

    async fn find_all_records(&self) -> Result<Vec<UHRPDiscoveryRecord>, UHRPStorageError> {
        let records = self
            .records
            .lock()
            .map_err(|e| UHRPStorageError::Other(e.to_string()))?;
        Ok(records
            .iter()
            .map(|r| UHRPDiscoveryRecord {
                txid: r.txid.clone(),
                output_index: r.output_index,
                uhrp_url: r.uhrp_url.clone(),
                identity_key: r.identity_key.clone(),
                download_url: r.download_url.clone(),
                expiry_time: r.expiry_time,
                content_length: r.content_length,
            })
            .collect())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[tokio::test]
    async fn store_and_find_record() {
        let store = MemoryUHRPStorage::new();
        store
            .store_record(
                "tx1",
                0,
                "uhrp://abc",
                "keyA",
                "https://a.example/cdn/1",
                1_900_000_000,
                1024,
            )
            .await
            .unwrap();
        store
            .store_record(
                "tx2",
                0,
                "uhrp://def",
                "keyB",
                "https://b.example/cdn/2",
                1_900_000_000,
                2048,
            )
            .await
            .unwrap();

        let results = store
            .find_record(&UHRPQuery {
                uhrp_url: Some("uhrp://abc".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn has_duplicate_checks_utxo_pair() {
        let store = MemoryUHRPStorage::new();
        assert!(!store.has_duplicate_record("tx1", 0).await.unwrap());
        store
            .store_record("tx1", 0, "u", "k", "d", 0, 1)
            .await
            .unwrap();
        assert!(store.has_duplicate_record("tx1", 0).await.unwrap());
        assert!(!store.has_duplicate_record("tx1", 1).await.unwrap());
    }

    #[tokio::test]
    async fn delete_record_removes_entry() {
        let store = MemoryUHRPStorage::new();
        store
            .store_record("tx1", 0, "u", "k", "d", 0, 1)
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);
        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn find_by_identity_key() {
        let store = MemoryUHRPStorage::new();
        store
            .store_record("tx1", 0, "u1", "keyA", "d", 0, 1)
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "u2", "keyB", "d", 0, 1)
            .await
            .unwrap();

        let results = store
            .find_record(&UHRPQuery {
                identity_key: Some("keyB".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx2");
    }

    #[tokio::test]
    async fn find_all_with_pagination() {
        let store = MemoryUHRPStorage::new();
        for i in 0..10 {
            store
                .store_record(&format!("tx{i}"), 0, "u", "k", "d", 0, 1)
                .await
                .unwrap();
        }
        let results = store.find_all(Some(3), Some(2), None).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn find_all_records_returns_full_metadata() {
        let store = MemoryUHRPStorage::new();
        store
            .store_record(
                "tx1",
                0,
                "uhrp://abc",
                "keyA",
                "https://a.example/cdn/1",
                1_900_000_000,
                1024,
            )
            .await
            .unwrap();
        let records = store.find_all_records().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].uhrp_url, "uhrp://abc");
        assert_eq!(records[0].identity_key, "keyA");
        assert_eq!(records[0].download_url, "https://a.example/cdn/1");
        assert_eq!(records[0].expiry_time, 1_900_000_000);
        assert_eq!(records[0].content_length, 1024);
    }

    #[tokio::test]
    async fn sort_order_serialization() {
        let asc_json = serde_json::to_string(&UHRPSortOrder::Asc).unwrap();
        assert_eq!(asc_json, "\"asc\"");
        let desc_json = serde_json::to_string(&UHRPSortOrder::Desc).unwrap();
        assert_eq!(desc_json, "\"desc\"");
    }

    #[tokio::test]
    async fn query_serialization_roundtrip() {
        let query = UHRPQuery {
            find_all: Some(true),
            uhrp_url: Some("uhrp://abc".into()),
            identity_key: Some("keyA".into()),
            limit: Some(10),
            skip: Some(5),
            sort_order: Some(UHRPSortOrder::Asc),
            include_expired: None,
            now_unix_seconds: None,
        };
        let json = serde_json::to_value(&query).unwrap();
        let back: UHRPQuery = serde_json::from_value(json).unwrap();
        assert_eq!(back.uhrp_url.as_deref(), Some("uhrp://abc"));
        assert_eq!(back.identity_key.as_deref(), Some("keyA"));
        assert_eq!(back.limit, Some(10));
        assert_eq!(back.skip, Some(5));
        assert_eq!(back.sort_order, Some(UHRPSortOrder::Asc));
    }
}
