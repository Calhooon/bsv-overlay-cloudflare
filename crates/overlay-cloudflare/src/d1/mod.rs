//! D1 database helpers — parameterized queries for Cloudflare D1.
//!
//! Adapted from ~/bsv/rust-wallet-infra/src/d1/mod.rs.
//! Provides Query builder with typed bind values and row deserialization.

use serde::de::DeserializeOwned;
use worker::wasm_bindgen::JsValue;
use worker::{D1Database, D1PreparedStatement};

// =============================================================================
// Query Value
// =============================================================================

/// A value that can be bound to a D1 prepared statement.
pub enum QVal {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Blob(Vec<u8>),
    Float(f64),
}

impl QVal {
    pub fn to_js(&self) -> JsValue {
        match self {
            Self::Null => JsValue::null(),
            Self::Int(i) => JsValue::from_f64(*i as f64),
            Self::Text(s) => JsValue::from_str(s),
            Self::Bool(b) => JsValue::from_f64(if *b { 1.0 } else { 0.0 }),
            Self::Blob(b) => worker::serde_wasm_bindgen::to_value(b).unwrap_or(JsValue::null()),
            Self::Float(f) => JsValue::from_f64(*f),
        }
    }
}

// Conversion traits
impl From<i64> for QVal {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<i32> for QVal {
    fn from(v: i32) -> Self {
        Self::Int(v as i64)
    }
}
impl From<u32> for QVal {
    fn from(v: u32) -> Self {
        Self::Int(v as i64)
    }
}
impl From<u64> for QVal {
    fn from(v: u64) -> Self {
        Self::Int(v as i64)
    }
}
impl From<String> for QVal {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}
impl From<&str> for QVal {
    fn from(v: &str) -> Self {
        Self::Text(v.to_string())
    }
}
impl From<bool> for QVal {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl From<Vec<u8>> for QVal {
    fn from(v: Vec<u8>) -> Self {
        Self::Blob(v)
    }
}
impl From<&[u8]> for QVal {
    fn from(v: &[u8]) -> Self {
        Self::Blob(v.to_vec())
    }
}
impl From<f64> for QVal {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl<T: Into<QVal>> From<Option<T>> for QVal {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Self::Null,
        }
    }
}

// =============================================================================
// Query Builder
// =============================================================================

/// Builds a parameterized D1 query with bind values.
pub struct Query {
    sql: String,
    params: Vec<QVal>,
}

impl Query {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    pub fn bind(mut self, val: impl Into<QVal>) -> Self {
        self.params.push(val.into());
        self
    }

    pub fn prepare(self, db: &D1Database) -> Result<D1PreparedStatement, String> {
        let stmt = db.prepare(&self.sql);
        if self.params.is_empty() {
            return Ok(stmt);
        }
        let js_values: Vec<JsValue> = self.params.iter().map(|v| v.to_js()).collect();
        stmt.bind(&js_values).map_err(|e| e.to_string())
    }

    pub async fn fetch_all<T: DeserializeOwned>(self, db: &D1Database) -> Result<Vec<T>, String> {
        let stmt = self.prepare(db)?;
        let result = stmt.all().await.map_err(|e| e.to_string())?;
        result.results::<T>().map_err(|e| e.to_string())
    }

    pub async fn fetch_optional<T: DeserializeOwned>(
        self,
        db: &D1Database,
    ) -> Result<Option<T>, String> {
        let stmt = self.prepare(db)?;
        stmt.first::<T>(None).await.map_err(|e| e.to_string())
    }

    pub async fn execute(self, db: &D1Database) -> Result<(), String> {
        let stmt = self.prepare(db)?;
        stmt.run().await.map_err(|e| e.to_string())?;
        Ok(())
    }
}

// =============================================================================
// WHERE clause builder
// =============================================================================

#[derive(Default)]
pub struct WhereBuilder {
    clauses: Vec<String>,
    params: Vec<QVal>,
}

impl WhereBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn eq(mut self, col: &str, val: impl Into<QVal>) -> Self {
        self.clauses.push(format!("{col} = ?"));
        self.params.push(val.into());
        self
    }

    /// Append a raw parameterized clause. `clause` must contain exactly the
    /// same number of `?` placeholders as `params.len()`. Useful for OR
    /// groups the builder doesn't express directly.
    pub fn raw(mut self, clause: &str, params: Vec<QVal>) -> Self {
        self.clauses.push(clause.to_string());
        self.params.extend(params);
        self
    }

    pub fn gte(mut self, col: &str, val: impl Into<QVal>) -> Self {
        self.clauses.push(format!("{col} >= ?"));
        self.params.push(val.into());
        self
    }

    /// Add an `IN (?, ?, ...)` clause with multiple values.
    pub fn in_vals(mut self, col: &str, vals: Vec<QVal>) -> Self {
        if vals.is_empty() {
            return self;
        }
        let placeholders = vec!["?"; vals.len()].join(", ");
        self.clauses.push(format!("{col} IN ({placeholders})"));
        self.params.extend(vals);
        self
    }

    pub fn build(self) -> (String, Vec<QVal>) {
        if self.clauses.is_empty() {
            (String::new(), Vec::new())
        } else {
            (
                format!(" WHERE {}", self.clauses.join(" AND ")),
                self.params,
            )
        }
    }
}

// =============================================================================
// Migration helper
// =============================================================================

/// Run a list of SQL migration statements against D1.
pub async fn run_migrations(db: &D1Database, statements: &[&str]) -> Result<(), String> {
    for sql in statements {
        Query::new(*sql).execute(db).await?;
    }
    Ok(())
}

/// Number of overlay migration statements.
pub const OVERLAY_MIGRATION_COUNT: usize = 23;

/// Overlay Engine schema migrations.
pub const OVERLAY_MIGRATIONS: &[&str] = &[
    // outputs table
    "CREATE TABLE IF NOT EXISTS outputs (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        outputScript BLOB,
        topic TEXT NOT NULL,
        satoshis INTEGER DEFAULT 0,
        outputsConsumed TEXT DEFAULT '[]',
        consumedBy TEXT DEFAULT '[]',
        spent INTEGER DEFAULT 0,
        blockHeight INTEGER,
        score REAL DEFAULT 0
    )",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_outputs ON outputs(txid, outputIndex, topic)",
    // transactions table (BEEF storage)
    "CREATE TABLE IF NOT EXISTS transactions (
        txid TEXT PRIMARY KEY,
        beef BLOB
    )",
    // applied transactions (deduplication)
    "CREATE TABLE IF NOT EXISTS applied_transactions (
        txid TEXT NOT NULL,
        topic TEXT NOT NULL
    )",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_applied ON applied_transactions(txid, topic)",
    // GASP sync state
    "CREATE TABLE IF NOT EXISTS host_sync_state (
        host TEXT NOT NULL,
        topic TEXT NOT NULL,
        since INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (host, topic)
    )",
    // SHIP records
    "CREATE TABLE IF NOT EXISTS ship_records (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        identityKey TEXT NOT NULL,
        domain TEXT NOT NULL,
        topic TEXT NOT NULL,
        createdAt TEXT NOT NULL DEFAULT (datetime('now'))
    )",
    "CREATE INDEX IF NOT EXISTS idx_ship ON ship_records(domain, topic)",
    // SLAP records
    "CREATE TABLE IF NOT EXISTS slap_records (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        identityKey TEXT NOT NULL,
        domain TEXT NOT NULL,
        service TEXT NOT NULL,
        createdAt TEXT NOT NULL DEFAULT (datetime('now'))
    )",
    "CREATE INDEX IF NOT EXISTS idx_slap ON slap_records(domain, service)",
    // Agent Registry records
    "CREATE TABLE IF NOT EXISTS agent_records (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        identityKey TEXT NOT NULL,
        certifierKey TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        createdAt TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (txid, outputIndex)
    )",
    "CREATE INDEX IF NOT EXISTS idx_agent_identity ON agent_records(identityKey)",
    "CREATE INDEX IF NOT EXISTS idx_agent_certifier ON agent_records(certifierKey)",
    "CREATE INDEX IF NOT EXISTS idx_agent_endpoint ON agent_records(endpoint)",
    // Agent capabilities (normalized — one row per capability per agent)
    "CREATE TABLE IF NOT EXISTS agent_capabilities (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        capability TEXT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS idx_agent_capability ON agent_capabilities(capability)",
    // Dolphin Milk delegation revocation records (tm_dm_delegation / ls_dm_delegation).
    // Tracks 1-sat PushDrop UTXOs that anchor cross-agent delegation cert
    // revocation status. The presence of a row means the cert is unspent
    // (not revoked). When the issuer spends the UTXO, the engine's
    // spent-output handling deletes the row, so absence == revoked.
    "CREATE TABLE IF NOT EXISTS dm_delegation_records (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        serialNumber TEXT NOT NULL,
        certifierKey TEXT NOT NULL,
        subjectKey TEXT NOT NULL,
        expiresAt TEXT NOT NULL,
        createdAt TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (txid, outputIndex)
    )",
    "CREATE INDEX IF NOT EXISTS idx_dm_delegation_serial ON dm_delegation_records(serialNumber)",
    "CREATE INDEX IF NOT EXISTS idx_dm_delegation_certifier ON dm_delegation_records(certifierKey)",
    // UHRP (Universal Hash Resolution Protocol) advertisement records for
    // tm_uhrp / ls_uhrp. One row per admitted advert UTXO; the outputs
    // table holds the canonical on-chain record, this one is the
    // index-side denormalization for query performance. Deleted by
    // `output_spent` / `output_evicted` in UHRPLookupService.
    "CREATE TABLE IF NOT EXISTS uhrp_records (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        uhrpUrl TEXT NOT NULL,
        identityKey TEXT NOT NULL,
        downloadUrl TEXT NOT NULL,
        expiryTime INTEGER NOT NULL,
        contentLength INTEGER NOT NULL,
        createdAt TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (txid, outputIndex)
    )",
    "CREATE INDEX IF NOT EXISTS idx_uhrp_url ON uhrp_records(uhrpUrl)",
    "CREATE INDEX IF NOT EXISTS idx_uhrp_identity ON uhrp_records(identityKey)",
    // Banned hosts / outpoints — mainline overlay-express 2.2.0 BanService
    // equivalent. `type` is "domain" or "outpoint". `value` is the
    // advertised URL (domain type) or `<txid>.<outputIndex>` (outpoint type).
    "CREATE TABLE IF NOT EXISTS banned_hosts (
        type TEXT NOT NULL,
        value TEXT NOT NULL,
        bannedAt TEXT NOT NULL DEFAULT (datetime('now')),
        bannedBy TEXT,
        reason TEXT,
        PRIMARY KEY (type, value)
    )",
];

// =============================================================================
// Tests (these test the builder logic without D1 — SQL string generation)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qval_conversions() {
        let _ = QVal::from(42i64);
        let _ = QVal::from(42i32);
        let _ = QVal::from(42u32);
        let _ = QVal::from(42u64);
        let _ = QVal::from("hello");
        let _ = QVal::from("hello".to_string());
        let _ = QVal::from(true);
        let _ = QVal::from(vec![0u8, 1, 2]);
        let _ = QVal::from(2.5f64);
        let _ = QVal::from(None::<i64>);
        let _ = QVal::from(Some(42i64));
    }

    #[test]
    fn where_builder_empty() {
        let (clause, params) = WhereBuilder::new().build();
        assert_eq!(clause, "");
        assert!(params.is_empty());
    }

    #[test]
    fn where_builder_single_eq() {
        let (clause, params) = WhereBuilder::new().eq("txid", "abc123").build();
        assert_eq!(clause, " WHERE txid = ?");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn where_builder_multiple() {
        let (clause, params) = WhereBuilder::new()
            .eq("txid", "abc")
            .eq("outputIndex", 0u32)
            .eq("topic", "Hello")
            .build();
        assert_eq!(clause, " WHERE txid = ? AND outputIndex = ? AND topic = ?");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn where_builder_gte() {
        let (clause, params) = WhereBuilder::new()
            .eq("topic", "tm_test")
            .eq("spent", false)
            .gte("score", 100.0f64)
            .build();
        assert_eq!(clause, " WHERE topic = ? AND spent = ? AND score >= ?");
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn migrations_are_valid_sql() {
        // Every migration should be non-empty and end reasonably
        assert_eq!(OVERLAY_MIGRATIONS.len(), OVERLAY_MIGRATION_COUNT);
        for (i, sql) in OVERLAY_MIGRATIONS.iter().enumerate() {
            assert!(!sql.is_empty(), "Migration {i} is empty");
            // Should start with CREATE
            let trimmed = sql.trim().to_uppercase();
            assert!(
                trimmed.starts_with("CREATE"),
                "Migration {i} should start with CREATE, got: {}",
                &trimmed[..30.min(trimmed.len())]
            );
        }
    }

    #[test]
    fn migrations_cover_all_tables() {
        let joined = OVERLAY_MIGRATIONS.join(" ");
        for table in &[
            "outputs",
            "transactions",
            "applied_transactions",
            "host_sync_state",
            "ship_records",
            "slap_records",
            "agent_records",
            "agent_capabilities",
            "dm_delegation_records",
            "uhrp_records",
        ] {
            assert!(
                joined.contains(table),
                "Missing migration for table: {table}"
            );
        }
    }

    #[test]
    fn migrations_cover_all_indexes() {
        let joined = OVERLAY_MIGRATIONS.join(" ");
        for index in &[
            "idx_outputs",
            "idx_applied",
            "idx_ship",
            "idx_slap",
            "idx_agent_identity",
            "idx_agent_certifier",
            "idx_agent_endpoint",
            "idx_agent_capability",
            "idx_dm_delegation_serial",
            "idx_dm_delegation_certifier",
            "idx_uhrp_url",
            "idx_uhrp_identity",
        ] {
            assert!(joined.contains(index), "Missing index: {index}");
        }
    }

    #[test]
    fn query_builder_no_params() {
        let q = Query::new("SELECT * FROM outputs");
        assert!(q.params.is_empty());
    }

    #[test]
    fn query_builder_with_params() {
        let q = Query::new("SELECT * FROM outputs WHERE txid = ? AND outputIndex = ?")
            .bind("abc123")
            .bind(0u32);
        assert_eq!(q.params.len(), 2);
    }
}
