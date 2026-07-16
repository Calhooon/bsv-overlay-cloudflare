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
///
/// The runner executes EVERY statement on EVERY cold start and propagates
/// errors — so an additive `ALTER TABLE ... ADD COLUMN` would fail with
/// "duplicate column name" on the second start. Exactly that case (and only
/// that case) is ignored via [`migration_error_is_benign`].
pub async fn run_migrations(db: &D1Database, statements: &[&str]) -> Result<(), String> {
    for sql in statements {
        if let Err(e) = Query::new(*sql).execute(db).await {
            if migration_error_is_benign(sql, &e) {
                continue;
            }
            return Err(e);
        }
    }
    Ok(())
}

/// Whether a migration-statement error is the expected re-run outcome of an
/// additive migration rather than a real fault: true IFF the statement is an
/// `ALTER TABLE` (case-insensitive, leading whitespace ignored) AND the
/// error message reports a duplicate column (case-insensitive). Any other
/// error — or a duplicate-column report from a non-ALTER statement — is NOT
/// benign and must propagate.
pub fn migration_error_is_benign(sql: &str, err: &str) -> bool {
    sql.trim_start()
        .to_ascii_uppercase()
        .starts_with("ALTER TABLE")
        && err.to_ascii_lowercase().contains("duplicate column")
}

/// Number of overlay migration statements.
pub const OVERLAY_MIGRATION_COUNT: usize = 43;

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
    // LOW poker lobby records (tm_low / ls_low) — bsv-low #39/#40.
    // One row per admitted LOW token UTXO. `recordType` is "table"
    // (TABLE_OPEN announcement) or "gameutxo" (live pot-outpoint
    // pointer). Table metadata columns are NULL for pointer rows —
    // the pot outpoint lives in the token's PushDrop fields, which
    // clients read from the BEEF that /lookup returns. Rows are
    // deleted on spend/eviction: spent TABLE_OPEN = table closed,
    // spent GAME_UTXO = superseded.
    "CREATE TABLE IF NOT EXISTS low_records (
        recordType TEXT NOT NULL,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        hostIdentity TEXT NOT NULL,
        gameId TEXT NOT NULL,
        stakeSats INTEGER,
        rulesHash TEXT,
        relayUrl TEXT,
        expiryHeight INTEGER,
        createdAt TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (txid, outputIndex)
    )",
    "CREATE INDEX IF NOT EXISTS idx_low_game ON low_records(gameId)",
    "CREATE INDEX IF NOT EXISTS idx_low_host ON low_records(hostIdentity)",
    "CREATE INDEX IF NOT EXISTS idx_low_type_stake ON low_records(recordType, stakeSats)",
    // Query-time table-expiry filter (bsv-low #148): findOpenTables adds
    // `AND expiryHeight > ?`. Additive, IF NOT EXISTS — reveal-safe.
    "CREATE INDEX IF NOT EXISTS idx_low_expiry ON low_records(recordType, expiryHeight)",
    // LOW break-glass reveal records (tm_reveal / ls_reveal). One row per
    // admitted LOW/reveal/v2 OP_RETURN artifact UTXO. Keyed by the on-chain
    // outpoint; queried by (gameId, seat) so the watchtower can look up
    // "did the accused seat reveal?" without scanning WoC address history.
    // Rows are NEVER deleted: a reveal is a permanent fact and the admitted
    // output is a provably-unspendable OP_RETURN (the lookup service's
    // spend/eviction hooks are no-ops). The reveal opening (positions +
    // scalars) lives in the token BEEF that /lookup returns, not here.
    "CREATE TABLE IF NOT EXISTS reveal_records (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        gameId TEXT NOT NULL,
        seat INTEGER NOT NULL,
        createdAt TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (txid, outputIndex)
    )",
    "CREATE INDEX IF NOT EXISTS idx_reveal_game ON reveal_records(gameId)",
    "CREATE INDEX IF NOT EXISTS idx_reveal_game_seat ON reveal_records(gameId, seat)",
    // LOW pot-spend landing-proof records (tm_pot / ls_pot). One row per
    // admitted Poc5TemplatePot covenant UTXO. Keyed by the pot funding
    // outpoint (txid, outputIndex); `spent` + `spendingTxid` carry the
    // on-chain landing proof once the settle/refund/sweep is seen. Unlike
    // reveal, this row IS updated (on spend) but is NEVER deleted — a spent
    // pot is the permanent landing proof a client queries before crediting a
    // payout. INSERT OR IGNORE on admission never clobbers a spent row.
    "CREATE TABLE IF NOT EXISTS pot_records (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        spent INTEGER NOT NULL DEFAULT 0,
        spendingTxid TEXT,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )",
    "CREATE INDEX IF NOT EXISTS idx_pot_spending ON pot_records(spendingTxid)",
    // Durable pot BEEF store (tm_pot / ls_pot). One row per pot FUNDING tx
    // and per pot-SPENDING (settle/refund/sweep) tx, keyed by that tx's OWN
    // txid. Exists because the engine's `transactions` table is
    // lifecycle-managed: a BEEF row is only written by insert_output (a
    // settle admits no outputs, so it never gets one) and is DELETED by the
    // deep-delete when a spent unretained coin is cleaned up. Rows here are
    // NEVER deleted; writes are longer-wins/never-clobber (the "vanishing
    // table" lesson). `low-app-layer /beef/:txid` serves this table first.
    "CREATE TABLE IF NOT EXISTS pot_beefs (
        txid TEXT PRIMARY KEY,
        beef BLOB NOT NULL,
        createdAt INTEGER
    )",
    // Prefer-confirmed / never-clobber-with-unconfirmed spend pointers
    // (bsv-low pot landing proof): 1 when the recorded spendingTxid was
    // SPV-confirmed (merkle path validated against the chain tracker) at
    // record time. An unconfirmed claim never overwrites a row with
    // spentConfirmed = 1 (see `D1PotStorage::mark_spent`). Additive ALTER:
    // the runner ignores the re-run "duplicate column" error
    // (`migration_error_is_benign`).
    "ALTER TABLE pot_records ADD COLUMN spentConfirmed INTEGER NOT NULL DEFAULT 0",
    // LOW cross-device "already collected" markers (tm_collected /
    // ls_collected, bsv-low #161). One row per (identity, gameId) pair —
    // FIRST MARKER WINS: the lookup service inserts with INSERT OR IGNORE
    // on the primary key, so a later marker for the same pair never
    // overwrites the first, and rows are NEVER deleted (a collected fact is
    // permanent, like a reveal; the OP_RETURN is provably unspendable).
    // txid + sigHex are handed back verbatim to querying clients, which
    // verify the sig under their OWN wallet — the overlay never does.
    "CREATE TABLE IF NOT EXISTS collected_markers (
        identity TEXT NOT NULL,
        gameId TEXT NOT NULL,
        txid TEXT,
        sigHex TEXT,
        createdAt INTEGER,
        PRIMARY KEY (identity, gameId)
    )",
    // LOW hand-result leaderboard markers, ORIGINAL (superseded) shape —
    // kept verbatim because this runner re-executes every statement and
    // shipped migrations are never edited. SUPERSEDED by
    // `result_markers_v2` below: the (gameId, winner) first-marker-wins
    // primary key was an adversarial-review HIGH (2026-07-16) — admission
    // is byte-format-only, so a garbage-sig front-run naming the REAL
    // winner could permanently occupy the pair slot and censor the
    // genuine countersigned marker for one OP_RETURN fee. No code writes
    // or reads this table anymore.
    "CREATE TABLE IF NOT EXISTS result_markers (
        gameId TEXT NOT NULL,
        winner TEXT NOT NULL,
        loser TEXT NOT NULL,
        potTxid TEXT,
        settleTxid TEXT,
        winnerSigHex TEXT,
        loserSigHex TEXT,
        txid TEXT,
        createdAt INTEGER,
        PRIMARY KEY (gameId, winner)
    )",
    "CREATE INDEX IF NOT EXISTS idx_result_markers_winner ON result_markers(winner)",
    "CREATE INDEX IF NOT EXISTS idx_result_markers_createdAt ON result_markers(createdAt)",
    // LOW hand-result leaderboard markers (tm_result / ls_result,
    // bsv-low #38), CURRENT shape. One row per marker OUTPOINT
    // (txid, outputIndex) — EVERY admitted marker is kept: the lookup
    // service inserts with INSERT OR IGNORE on the primary key, so a
    // replayed submit of the same output is a no-op, but markers for the
    // same (gameId, winner) from DIFFERENT txs are ALL kept — the
    // censorship-front-run fix (garbage and genuine rows coexist; the
    // CLIENT's sig verify separates them and the genuine one counts).
    // Rows are NEVER deleted (a settled result is permanent, like a
    // reveal; the OP_RETURN is provably unspendable). All byte fields are
    // handed back verbatim to querying clients, which verify BOTH sigs
    // client-side ('anyone' ProtoWallet round-trip) — the overlay never
    // does and derives no "confirmed" flag. loserSigHex is NULL when the
    // marker's loserSig push was empty (an unconfirmed claim).
    //
    // Why a NEW table instead of an in-place rebuild: the runner
    // re-executes every statement on every cold start, so a
    // copy/DROP/RENAME dance would re-run against the LIVE table on the
    // next start (re-copying rows with outputIndex=0 → corruption, then
    // dropping the real table). CREATE-only + a one-time INSERT OR IGNORE
    // carry (below) is re-run-safe: nothing writes to the old table
    // anymore, and OR IGNORE dedups on the new primary key.
    "CREATE TABLE IF NOT EXISTS result_markers_v2 (
        gameId TEXT NOT NULL,
        winner TEXT NOT NULL,
        loser TEXT NOT NULL,
        potTxid TEXT,
        settleTxid TEXT,
        winnerSigHex TEXT,
        loserSigHex TEXT,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )",
    // Carry any rows admitted under the superseded shape into v2 with
    // outputIndex 0 (the old schema never stored the vout; 0 is a
    // harmless PK-only placeholder). Idempotent: OR IGNORE on the
    // (txid, outputIndex) key + the source table is write-frozen, so
    // re-runs are no-ops. Old rows with a NULL txid (nullable there,
    // NOT NULL here) cannot be carried and are skipped.
    "INSERT OR IGNORE INTO result_markers_v2 \
     (gameId, winner, loser, potTxid, settleTxid, winnerSigHex, loserSigHex, \
      txid, outputIndex, createdAt) \
     SELECT gameId, winner, loser, potTxid, settleTxid, winnerSigHex, loserSigHex, \
      txid, 0, createdAt FROM result_markers WHERE txid IS NOT NULL",
    // The two ls_result list queries: resultsFor filters by winner,
    // both order by createdAt DESC.
    "CREATE INDEX IF NOT EXISTS idx_result_markers_v2_winner ON result_markers_v2(winner)",
    "CREATE INDEX IF NOT EXISTS idx_result_markers_v2_createdAt ON result_markers_v2(createdAt)",
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
            // Should start with CREATE (idempotent IF NOT EXISTS), ALTER
            // (additive; the runner ignores the re-run duplicate-column
            // error via migration_error_is_benign), or INSERT OR IGNORE
            // (a re-run-safe data carry: the PK dedups replays — used by
            // the result_markers → result_markers_v2 carry, whose source
            // table is write-frozen). Plain INSERT / DROP / RENAME are
            // still banned: the runner re-executes every statement on
            // every cold start.
            let trimmed = sql.trim().to_uppercase();
            assert!(
                trimmed.starts_with("CREATE")
                    || trimmed.starts_with("ALTER TABLE")
                    || trimmed.starts_with("INSERT OR IGNORE"),
                "Migration {i} should start with CREATE, ALTER TABLE, or INSERT OR IGNORE, got: {}",
                &trimmed[..30.min(trimmed.len())]
            );
        }
    }

    #[test]
    fn migration_benign_error_is_duplicate_column_on_alter_only() {
        let alter = "ALTER TABLE pot_records ADD COLUMN spentConfirmed INTEGER NOT NULL DEFAULT 0";
        // The expected re-run outcome of an additive ALTER → benign.
        assert!(migration_error_is_benign(
            alter,
            "duplicate column name: spentConfirmed"
        ));
        // Case-insensitive on both sides, leading whitespace tolerated.
        assert!(migration_error_is_benign(
            "  alter table pot_records ADD COLUMN x INTEGER",
            "D1_ERROR: Duplicate Column name: x"
        ));
        // Any OTHER error on an ALTER is NOT benign.
        assert!(!migration_error_is_benign(alter, "no such table: pot_records"));
        assert!(!migration_error_is_benign(alter, "syntax error near ADD"));
        // A duplicate-column report from a non-ALTER statement is NOT benign.
        assert!(!migration_error_is_benign(
            "CREATE TABLE t (a INTEGER, a INTEGER)",
            "duplicate column name: a"
        ));
        // Empty inputs are never benign.
        assert!(!migration_error_is_benign("", "duplicate column"));
        assert!(!migration_error_is_benign(alter, ""));
    }

    #[test]
    fn pot_records_spent_confirmed_migration_present() {
        // The additive column migration exists and targets pot_records.
        assert!(OVERLAY_MIGRATIONS.iter().any(|sql| {
            sql.trim_start().starts_with("ALTER TABLE pot_records")
                && sql.contains("spentConfirmed INTEGER NOT NULL DEFAULT 0")
        }));
    }

    #[test]
    fn result_markers_carry_migration_is_rerun_safe() {
        // The one non-CREATE/ALTER migration: the result_markers →
        // result_markers_v2 data carry. Pin the two properties that make
        // it safe under the re-run-everything runner: OR IGNORE (PK
        // dedups replays) and the NULL-txid filter (v2's txid is NOT
        // NULL). And it must be the ONLY such statement.
        let carries: Vec<&&str> = OVERLAY_MIGRATIONS
            .iter()
            .filter(|sql| sql.trim_start().to_uppercase().starts_with("INSERT"))
            .collect();
        assert_eq!(carries.len(), 1, "exactly one data-carry migration");
        let carry = carries[0];
        assert!(carry.trim_start().starts_with("INSERT OR IGNORE INTO result_markers_v2"));
        assert!(carry.contains("FROM result_markers WHERE txid IS NOT NULL"));
        assert!(carry.contains("outputIndex"));
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
            "low_records",
            "reveal_records",
            "pot_records",
            "pot_beefs",
            "collected_markers",
            "result_markers",
            "result_markers_v2",
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
            "idx_reveal_game",
            "idx_reveal_game_seat",
            "idx_pot_spending",
            "idx_result_markers_winner",
            "idx_result_markers_createdAt",
            "idx_result_markers_v2_winner",
            "idx_result_markers_v2_createdAt",
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
