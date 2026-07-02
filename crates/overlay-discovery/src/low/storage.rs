//! LOW Storage trait — backend-agnostic storage for LOW lobby records.
//!
//! One row per admitted `tm_low` token UTXO (`low_records` in D1). The
//! concrete implementation (D1, in-memory) is provided by the deployment
//! crate; `MemoryLowStorage` here backs the unit tests. Structure mirrors
//! `ship::storage`.

use async_trait::async_trait;
use overlay_engine::types::UTXOReference;
use serde::{Deserialize, Serialize};

/// Record type discriminator for `low_records` rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LowRecordType {
    /// TABLE_OPEN — an open-table announcement (`LOW.table.v1`).
    #[serde(rename = "table")]
    Table,
    /// GAME_UTXO — a live pot-outpoint pointer (`LOW.gameutxo.v1`).
    #[serde(rename = "gameutxo")]
    GameUtxo,
}

impl LowRecordType {
    /// Stable string form stored in the `recordType` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            LowRecordType::Table => "table",
            LowRecordType::GameUtxo => "gameutxo",
        }
    }

    /// Parse the stable string form.
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s {
            "table" => Some(LowRecordType::Table),
            "gameutxo" => Some(LowRecordType::GameUtxo),
            _ => None,
        }
    }
}

/// A LOW lobby record as stored in the index.
///
/// TABLE_OPEN rows carry the full table metadata; GAME_UTXO rows only
/// carry identity + gameId (the pot outpoint lives in the token fields
/// returned to clients as BEEF, not in the index).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LowRecord {
    #[serde(rename = "recordType")]
    pub record_type: LowRecordType,
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Host identity key (33-byte compressed pubkey, lowercase hex).
    #[serde(rename = "hostIdentity")]
    pub host_identity: String,
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// TABLE_OPEN only: stake in satoshis.
    #[serde(rename = "stakeSats")]
    pub stake_sats: Option<u64>,
    /// TABLE_OPEN only: rules hash (32 bytes, lowercase hex).
    #[serde(rename = "rulesHash")]
    pub rules_hash: Option<String>,
    /// TABLE_OPEN only: relay URL.
    #[serde(rename = "relayUrl")]
    pub relay_url: Option<String>,
    /// TABLE_OPEN only: expiry block height.
    #[serde(rename = "expiryHeight")]
    pub expiry_height: Option<u32>,
}

/// `ls_low` query shapes — tagged JSON, e.g.
/// `{"type":"findOpenTables","stakeMin":100,"stakeMax":5000}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum LowQuery {
    /// All unspent TABLE_OPEN records, optionally filtered by stake range.
    #[serde(rename = "findOpenTables")]
    FindOpenTables {
        #[serde(rename = "stakeMin", default, skip_serializing_if = "Option::is_none")]
        stake_min: Option<u64>,
        #[serde(rename = "stakeMax", default, skip_serializing_if = "Option::is_none")]
        stake_max: Option<u64>,
    },
    /// All records (TABLE_OPEN + GAME_UTXO) for one game.
    #[serde(rename = "byGameId")]
    ByGameId {
        #[serde(rename = "gameId")]
        game_id: String,
    },
    /// All records published by one host identity key.
    #[serde(rename = "byHost")]
    ByHost {
        #[serde(rename = "identityKey")]
        identity_key: String,
    },
}

/// Backend-agnostic storage for LOW lobby records.
#[async_trait(?Send)]
pub trait LowStorage {
    /// Store (or idempotently re-store) a record keyed by (txid, outputIndex).
    async fn store_record(&self, record: &LowRecord) -> Result<(), LowStorageError>;

    /// Delete a record by UTXO reference (spend or eviction).
    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), LowStorageError>;

    /// Unspent TABLE_OPEN records, optionally filtered by stake range.
    async fn find_open_tables(
        &self,
        stake_min: Option<u64>,
        stake_max: Option<u64>,
    ) -> Result<Vec<UTXOReference>, LowStorageError>;

    /// All records for a game ID (lowercase hex).
    async fn find_by_game_id(&self, game_id: &str) -> Result<Vec<UTXOReference>, LowStorageError>;

    /// All records for a host identity key (lowercase hex).
    async fn find_by_host(&self, identity_key: &str)
        -> Result<Vec<UTXOReference>, LowStorageError>;
}

/// LOW storage errors.
#[derive(Debug, thiserror::Error)]
pub enum LowStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory LOW storage for testing.
#[derive(Debug, Default)]
pub struct MemoryLowStorage {
    records: std::sync::Mutex<Vec<LowRecord>>,
}

impl MemoryLowStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl LowStorage for MemoryLowStorage {
    async fn store_record(&self, record: &LowRecord) -> Result<(), LowStorageError> {
        let mut records = self.records.lock().unwrap();
        // Idempotent on (txid, outputIndex) — matches D1's INSERT OR REPLACE.
        records.retain(|r| !(r.txid == record.txid && r.output_index == record.output_index));
        records.push(record.clone());
        Ok(())
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), LowStorageError> {
        self.records
            .lock()
            .unwrap()
            .retain(|r| !(r.txid == txid && r.output_index == output_index));
        Ok(())
    }

    async fn find_open_tables(
        &self,
        stake_min: Option<u64>,
        stake_max: Option<u64>,
    ) -> Result<Vec<UTXOReference>, LowStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.record_type == LowRecordType::Table)
            .filter(|r| {
                let stake = r.stake_sats.unwrap_or(0);
                stake_min.is_none_or(|min| stake >= min)
                    && stake_max.is_none_or(|max| stake <= max)
            })
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }

    async fn find_by_game_id(&self, game_id: &str) -> Result<Vec<UTXOReference>, LowStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.game_id == game_id)
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }

    async fn find_by_host(
        &self,
        identity_key: &str,
    ) -> Result<Vec<UTXOReference>, LowStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.host_identity == identity_key)
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn table_record(txid: &str, stake: u64) -> LowRecord {
        LowRecord {
            record_type: LowRecordType::Table,
            txid: txid.into(),
            output_index: 0,
            host_identity: "02".repeat(33),
            game_id: "11".repeat(32),
            stake_sats: Some(stake),
            rules_hash: Some("22".repeat(32)),
            relay_url: Some("https://relay.example.com".into()),
            expiry_height: Some(900000),
        }
    }

    fn gameutxo_record(txid: &str, game_id: &str) -> LowRecord {
        LowRecord {
            record_type: LowRecordType::GameUtxo,
            txid: txid.into(),
            output_index: 1,
            host_identity: "02".repeat(33),
            game_id: game_id.into(),
            stake_sats: None,
            rules_hash: None,
            relay_url: None,
            expiry_height: None,
        }
    }

    #[tokio::test]
    async fn store_and_find_open_tables() {
        let store = MemoryLowStorage::new();
        store.store_record(&table_record("tx1", 1000)).await.unwrap();
        store.store_record(&table_record("tx2", 5000)).await.unwrap();
        store
            .store_record(&gameutxo_record("tx3", &"11".repeat(32)))
            .await
            .unwrap();

        // No filter → both tables, not the pointer
        let all = store.find_open_tables(None, None).await.unwrap();
        assert_eq!(all.len(), 2);

        // Stake range filters
        let low = store.find_open_tables(None, Some(2000)).await.unwrap();
        assert_eq!(low.len(), 1);
        assert_eq!(low[0].txid, "tx1");

        let high = store.find_open_tables(Some(2000), None).await.unwrap();
        assert_eq!(high.len(), 1);
        assert_eq!(high[0].txid, "tx2");

        let none = store
            .find_open_tables(Some(6000), Some(9000))
            .await
            .unwrap();
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn find_by_game_id_returns_both_types() {
        let store = MemoryLowStorage::new();
        store.store_record(&table_record("tx1", 1000)).await.unwrap();
        store
            .store_record(&gameutxo_record("tx2", &"11".repeat(32)))
            .await
            .unwrap();
        store
            .store_record(&gameutxo_record("tx3", &"ff".repeat(32)))
            .await
            .unwrap();

        let results = store.find_by_game_id(&"11".repeat(32)).await.unwrap();
        assert_eq!(results.len(), 2);
        let txids: Vec<&str> = results.iter().map(|r| r.txid.as_str()).collect();
        assert!(txids.contains(&"tx1"));
        assert!(txids.contains(&"tx2"));
    }

    #[tokio::test]
    async fn find_by_host() {
        let store = MemoryLowStorage::new();
        let mut other_host = table_record("tx2", 1000);
        other_host.host_identity = "03".repeat(33);

        store.store_record(&table_record("tx1", 1000)).await.unwrap();
        store.store_record(&other_host).await.unwrap();

        let results = store.find_by_host(&"02".repeat(33)).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn store_is_idempotent_per_outpoint() {
        let store = MemoryLowStorage::new();
        store.store_record(&table_record("tx1", 1000)).await.unwrap();
        store.store_record(&table_record("tx1", 2000)).await.unwrap();
        assert_eq!(store.record_count(), 1);

        // Latest write wins
        let results = store.find_open_tables(Some(1500), None).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn delete_record_removes_only_matching_outpoint() {
        let store = MemoryLowStorage::new();
        store.store_record(&table_record("tx1", 1000)).await.unwrap();
        store
            .store_record(&gameutxo_record("tx1", &"11".repeat(32)))
            .await
            .unwrap(); // same txid, vout 1
        assert_eq!(store.record_count(), 2);

        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 1);

        // Deleting a nonexistent record is fine
        store.delete_record("nope", 9).await.unwrap();
        assert_eq!(store.record_count(), 1);
    }

    #[test]
    fn query_json_shapes() {
        let q: LowQuery = serde_json::from_value(serde_json::json!({
            "type": "findOpenTables", "stakeMin": 100, "stakeMax": 5000
        }))
        .unwrap();
        match q {
            LowQuery::FindOpenTables {
                stake_min,
                stake_max,
            } => {
                assert_eq!(stake_min, Some(100));
                assert_eq!(stake_max, Some(5000));
            }
            _ => panic!("wrong variant"),
        }

        // Bounds optional
        let q: LowQuery =
            serde_json::from_value(serde_json::json!({"type": "findOpenTables"})).unwrap();
        assert!(matches!(
            q,
            LowQuery::FindOpenTables {
                stake_min: None,
                stake_max: None
            }
        ));

        let q: LowQuery = serde_json::from_value(serde_json::json!({
            "type": "byGameId", "gameId": "ab".repeat(32)
        }))
        .unwrap();
        assert!(matches!(q, LowQuery::ByGameId { .. }));

        let q: LowQuery = serde_json::from_value(serde_json::json!({
            "type": "byHost", "identityKey": "02".repeat(33)
        }))
        .unwrap();
        assert!(matches!(q, LowQuery::ByHost { .. }));

        // Unknown type is an error
        assert!(
            serde_json::from_value::<LowQuery>(serde_json::json!({"type": "nope"})).is_err()
        );
    }

    #[test]
    fn record_type_str_roundtrip() {
        for t in [LowRecordType::Table, LowRecordType::GameUtxo] {
            assert_eq!(LowRecordType::from_str_opt(t.as_str()), Some(t));
        }
        assert_eq!(LowRecordType::from_str_opt("bogus"), None);
    }
}
