//! LOW Storage trait — backend-agnostic storage for LOW lobby records.
//!
//! One row per admitted `tm_low` token UTXO (`low_records` in D1). The
//! concrete implementation (D1, in-memory) is provided by the deployment
//! crate; `MemoryLowStorage` here backs the unit tests. Structure mirrors
//! `ship::storage`.

use async_trait::async_trait;
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
///
/// Every variant carries an optional `decoded` flag (bsv-low #290, default
/// `false`): `false` keeps the legacy `OutputList` answer (`{beef,
/// outputIndex}` per row — byte-identical to the pre-#290 wire); `true`
/// returns the DECODED index columns as a `Freeform` array (the shaped
/// answer the index already holds), the same idiom as the other six LOW
/// services. Raw bytes stay available via the app-layer `/beef/:txid` for
/// independent verification (the slim-mirror pattern).
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
        #[serde(default)]
        decoded: bool,
    },
    /// All records (TABLE_OPEN + GAME_UTXO) for one game.
    #[serde(rename = "byGameId")]
    ByGameId {
        #[serde(rename = "gameId")]
        game_id: String,
        #[serde(default)]
        decoded: bool,
    },
    /// All records published by one host identity key.
    #[serde(rename = "byHost")]
    ByHost {
        #[serde(rename = "identityKey")]
        identity_key: String,
        #[serde(default)]
        decoded: bool,
    },
}

/// Newest-first cap on `find_open_tables` results (bsv-low #291). The lobby
/// is a DISPLAY surface, not a money answer: a table beyond the newest 200
/// is invisible until older ones close/expire — an acceptable, documented
/// bound (an unjoined table risks no funds), versus the previous unbounded
/// scan feeding the per-row BEEF hydration.
pub const OPEN_TABLES_RESULT_CAP: usize = 200;

/// Per-HOST quota inside the lobby window (bsv-low #291 gate finding M3,
/// the #281 partitioned-window pattern): without it, ONE identity's
/// [`OPEN_TABLES_RESULT_CAP`] byte-format-admitted junk rows blanked every
/// honest table from the lobby answer. A legitimate host has 1-2 open
/// tables; 5 is headroom. Blanking the lobby now costs
/// `CAP / PER_HOST = 40` distinct identities' worth of admitted on-chain
/// rows. Residual (accepted, display-only): identities are free keypairs,
/// so a multi-identity flood can still displace — no money path reads the
/// lobby (rejoin is keyed byGameId/byHost; money discovery is
/// server-primary).
pub const OPEN_TABLES_PER_HOST_CAP: usize = 5;

/// Cap on `find_by_game_id` / `find_by_host` results (bsv-low #291). A game
/// legitimately has ~2 live rows (its TABLE_OPEN + current GAME_UTXO
/// pointer) and a host a handful — 50 is an order of magnitude of headroom.
pub const LOW_BY_KEY_RESULT_CAP: usize = 50;

/// Backend-agnostic storage for LOW lobby records.
#[async_trait(?Send)]
pub trait LowStorage {
    /// Store (or idempotently re-store) a record keyed by (txid, outputIndex).
    async fn store_record(&self, record: &LowRecord) -> Result<(), LowStorageError>;

    /// Delete a record by UTXO reference (spend or eviction).
    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), LowStorageError>;

    /// Unspent TABLE_OPEN records, optionally filtered by stake range —
    /// FULL index rows (bsv-low #290: the decoded columns are the shaped
    /// answer; the caller chooses whether to return them or just the refs),
    /// newest-first, capped at [`OPEN_TABLES_RESULT_CAP`].
    ///
    /// `tip_height` enforces expiry at QUERY TIME: when `Some(tip)`, a row is
    /// returned only if its `expiryHeight > tip` (strictly greater, mirroring
    /// the client's `expiryHeight > tip` guard). When `None` (tip unavailable)
    /// no expiry filter is applied — fail-open so the lobby degrades to showing
    /// tables rather than hiding everything. The overlay has no passive spend
    /// watcher, so an expired-but-unspent TABLE_OPEN token would otherwise
    /// linger in the lobby forever (bsv-low #148).
    async fn find_open_tables(
        &self,
        stake_min: Option<u64>,
        stake_max: Option<u64>,
        tip_height: Option<u32>,
    ) -> Result<Vec<LowRecord>, LowStorageError>;

    /// All records for a game ID (lowercase hex), capped at
    /// [`LOW_BY_KEY_RESULT_CAP`].
    async fn find_by_game_id(&self, game_id: &str) -> Result<Vec<LowRecord>, LowStorageError>;

    /// All records for a host identity key (lowercase hex), capped at
    /// [`LOW_BY_KEY_RESULT_CAP`].
    async fn find_by_host(&self, identity_key: &str) -> Result<Vec<LowRecord>, LowStorageError>;
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
        tip_height: Option<u32>,
    ) -> Result<Vec<LowRecord>, LowStorageError> {
        let records: Vec<LowRecord> = self
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
            // Query-time expiry (bsv-low #148): with a known tip, keep only
            // rows with `expiryHeight > tip` (strictly greater, mirroring the
            // client's `expiryHeight > tip` and the D1 `expiryHeight > ?`).
            // Tip absent => no filter (fail-open). A table row missing an
            // expiry is excluded under a tip, matching SQL `NULL > ?`.
            .filter(|r| {
                tip_height.is_none_or(|tip| r.expiry_height.is_some_and(|e| e > tip))
            })
            .cloned()
            .collect();
        // NEWEST-FIRST (gate L2: same direction D1 returns), with the
        // per-host quota + overall cap applied exactly like the shipped SQL
        // (gate M3): walk newest -> oldest, admit at most
        // OPEN_TABLES_PER_HOST_CAP rows per hostIdentity, stop at the
        // window cap.
        let mut per_host: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let mut window = Vec::new();
        for r in records.into_iter().rev() {
            let n = per_host.entry(r.host_identity.clone()).or_insert(0);
            if *n >= OPEN_TABLES_PER_HOST_CAP {
                continue;
            }
            *n += 1;
            window.push(r);
            if window.len() >= OPEN_TABLES_RESULT_CAP {
                break;
            }
        }
        Ok(window)
    }

    async fn find_by_game_id(&self, game_id: &str) -> Result<Vec<LowRecord>, LowStorageError> {
        let records: Vec<LowRecord> = self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.game_id == game_id)
            .cloned()
            .collect();
        Ok(newest_first(records, LOW_BY_KEY_RESULT_CAP))
    }

    async fn find_by_host(&self, identity_key: &str) -> Result<Vec<LowRecord>, LowStorageError> {
        let records: Vec<LowRecord> = self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.host_identity == identity_key)
            .cloned()
            .collect();
        Ok(newest_first(records, LOW_BY_KEY_RESULT_CAP))
    }
}

/// NEWEST-first window over an insertion-ordered vec: reverse, then keep the
/// first `cap` rows — the same rows in the same DIRECTION D1's
/// `ORDER BY createdAt DESC LIMIT` returns (gate L2: memory backends must
/// not mask ordering bugs by answering in the opposite order).
fn newest_first<T>(mut rows: Vec<T>, cap: usize) -> Vec<T> {
    rows.reverse();
    rows.truncate(cap);
    rows
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

    fn table_record_expiry(txid: &str, stake: u64, expiry: u32) -> LowRecord {
        let mut r = table_record(txid, stake);
        r.expiry_height = Some(expiry);
        r
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
        let all = store.find_open_tables(None, None, None).await.unwrap();
        assert_eq!(all.len(), 2);

        // Stake range filters
        let low = store.find_open_tables(None, Some(2000), None).await.unwrap();
        assert_eq!(low.len(), 1);
        assert_eq!(low[0].txid, "tx1");

        let high = store.find_open_tables(Some(2000), None, None).await.unwrap();
        assert_eq!(high.len(), 1);
        assert_eq!(high[0].txid, "tx2");

        let none = store
            .find_open_tables(Some(6000), Some(9000), None)
            .await
            .unwrap();
        assert!(none.is_empty());
    }

    /// Gate L2: the memory backend answers in the SAME direction as D1
    /// (newest first) — a backend-dependent order would let memory-backed
    /// tests mask ordering bugs.
    #[tokio::test]
    async fn find_open_tables_is_newest_first_like_d1() {
        let store = MemoryLowStorage::new();
        for (txid, host) in [("txOld", "02aa"), ("txMid", "02bb"), ("txNew", "02cc")] {
            let mut r = table_record(txid, 1000);
            r.host_identity = host.into();
            store.store_record(&r).await.unwrap();
        }
        let rows = store.find_open_tables(None, None, None).await.unwrap();
        assert_eq!(
            rows.iter().map(|r| r.txid.as_str()).collect::<Vec<_>>(),
            vec!["txNew", "txMid", "txOld"],
            "newest first — the direction D1's ORDER BY createdAt DESC returns"
        );

        // …and the by-key reads too.
        let by_game = store.find_by_game_id(&"11".repeat(32)).await.unwrap();
        assert_eq!(by_game[0].txid, "txNew", "byGameId newest first");
        let mut r = table_record("txHost2", 1000);
        r.host_identity = "02aa".into();
        store.store_record(&r).await.unwrap();
        let by_host = store.find_by_host("02aa").await.unwrap();
        assert_eq!(
            by_host.iter().map(|r| r.txid.as_str()).collect::<Vec<_>>(),
            vec!["txHost2", "txOld"],
            "byHost newest first"
        );
    }

    /// Gate M3 (memory mirror of the shipped-SQL partition test): one
    /// flooding identity holds at most OPEN_TABLES_PER_HOST_CAP window
    /// slots; other hosts' older tables survive.
    #[tokio::test]
    async fn find_open_tables_per_host_quota_bounds_a_flood() {
        let store = MemoryLowStorage::new();
        let mut honest = table_record("txHONEST", 1000);
        honest.host_identity = "02aa".into();
        store.store_record(&honest).await.unwrap();
        for i in 0..OPEN_TABLES_RESULT_CAP {
            let mut r = table_record(&format!("txFLOOD{i}"), 1000);
            r.host_identity = "02ff".into();
            store.store_record(&r).await.unwrap();
        }
        let rows = store.find_open_tables(None, None, None).await.unwrap();
        let flood = rows.iter().filter(|r| r.host_identity == "02ff").count();
        assert_eq!(flood, OPEN_TABLES_PER_HOST_CAP, "flooder capped at its quota");
        assert!(
            rows.iter().any(|r| r.txid == "txHONEST"),
            "the honest host's older table survives the flood"
        );
    }

    #[tokio::test]
    async fn find_open_tables_excludes_expired_when_tip_present() {
        let store = MemoryLowStorage::new();
        // fresh: expiry 900_000 is ABOVE tip → visible.
        store
            .store_record(&table_record_expiry("fresh", 1000, 900_000))
            .await
            .unwrap();
        // stale: expiry 899_000 is BELOW tip → hidden (expired-but-unspent).
        store
            .store_record(&table_record_expiry("stale", 1000, 899_000))
            .await
            .unwrap();

        let tip = 899_500u32;
        let visible = store
            .find_open_tables(None, None, Some(tip))
            .await
            .unwrap();
        assert_eq!(visible.len(), 1, "only the non-expired table should return");
        assert_eq!(visible[0].txid, "fresh");
    }

    #[tokio::test]
    async fn find_open_tables_strictly_greater_at_tip_boundary() {
        let store = MemoryLowStorage::new();
        // expiry == tip must be EXCLUDED (strictly-greater, mirrors client).
        store
            .store_record(&table_record_expiry("at_tip", 1000, 900_000))
            .await
            .unwrap();
        store
            .store_record(&table_record_expiry("above_tip", 1000, 900_001))
            .await
            .unwrap();

        let visible = store
            .find_open_tables(None, None, Some(900_000))
            .await
            .unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].txid, "above_tip");
    }

    #[tokio::test]
    async fn find_open_tables_no_tip_shows_all_fail_open() {
        let store = MemoryLowStorage::new();
        store
            .store_record(&table_record_expiry("fresh", 1000, 900_000))
            .await
            .unwrap();
        store
            .store_record(&table_record_expiry("stale", 1000, 1))
            .await
            .unwrap();

        // tip unavailable → NO expiry filter → both visible (never hide all).
        let visible = store.find_open_tables(None, None, None).await.unwrap();
        assert_eq!(visible.len(), 2, "tip=None must fail open (show all tables)");
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
        let results = store.find_open_tables(Some(1500), None, None).await.unwrap();
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
                decoded,
            } => {
                assert_eq!(stake_min, Some(100));
                assert_eq!(stake_max, Some(5000));
                // #290: `decoded` absent on the wire defaults to false — the
                // legacy answer shape stays byte-identical.
                assert!(!decoded);
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
                stake_max: None,
                decoded: false,
            }
        ));

        // #290: opting into the decoded answer parses.
        let q: LowQuery = serde_json::from_value(serde_json::json!({
            "type": "findOpenTables", "decoded": true
        }))
        .unwrap();
        assert!(matches!(q, LowQuery::FindOpenTables { decoded: true, .. }));

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
