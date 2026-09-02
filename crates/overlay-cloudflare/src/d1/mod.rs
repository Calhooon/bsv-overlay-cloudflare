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
#[derive(Debug, Clone, PartialEq)]
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

    /// The SQL text this query will prepare.
    ///
    /// Exists so a WRITE path can be pinned BEHAVIOURALLY without a
    /// `D1Database` (bsv-low #283): `execute` needs a live D1 binding and is
    /// unreachable natively, which is exactly how a writer can be silently
    /// neutered while every test stays green. With these two accessors a test
    /// can take the query the real writer builds and replay it against real
    /// SQLite. Read-only; no way to mutate a built query.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The bound parameters, in bind order. See [`Query::sql`].
    pub fn params(&self) -> &[QVal] {
        &self.params
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

/// Set once THIS isolate has applied [`OVERLAY_MIGRATIONS`] successfully.
///
/// Workers isolates are single-threaded, but the atomic keeps the guard sound
/// under any future threading model. A fresh isolate starts `false`, so every
/// cold start still applies the (idempotent) migrations.
static OVERLAY_MIGRATIONS_APPLIED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Apply [`OVERLAY_MIGRATIONS`] at most once per isolate lifetime.
///
/// The 63-statement migration list is 63 sequential D1 round-trips; running it
/// unguarded on EVERY request taxed every route ~2.5-3s under elevated D1 RTT
/// (bsv-low #255). The flag is set ONLY after a fully successful pass, so a
/// failed attempt propagates its error and the next request retries from the
/// top (`run_migrations` is idempotent by construction). Two requests racing on
/// a fresh isolate at worst both run the idempotent list — never a skip.
pub async fn ensure_overlay_migrations(db: &D1Database) -> Result<(), String> {
    use std::sync::atomic::Ordering;
    if OVERLAY_MIGRATIONS_APPLIED.load(Ordering::Acquire) {
        return Ok(());
    }
    // #411 (2026-08-26): VERSION GATE. The per-isolate atomic above only
    // helps a WARM isolate — a 16-pair run's launch scales up many fresh
    // isolates at once, and each replayed all 114 idempotent statements into
    // ONE D1 (the #255 class as a per-scale-up burst; measured live: submits
    // in that window answered "0 output(s) admitted" and lookups failed).
    // Now a cold start costs ONE read: if the persisted count matches the
    // built-in count, the list is current and the full replay is skipped.
    // The counter row lives in ops_counters, which is itself CREATED by a
    // migration — so any read failure (first boot, pre-gate database) falls
    // through to the full run, exactly as before. Fail-open by construction:
    // a wrong/missing counter can only cause the OLD behavior (full replay),
    // never a skipped migration — the counter is written ONLY after
    // run_migrations returns Ok.
    if migration_state_current(db).await {
        OVERLAY_MIGRATIONS_APPLIED.store(true, Ordering::Release);
        return Ok(());
    }
    run_migrations(db, OVERLAY_MIGRATIONS).await?;
    // Review MEDIUM-4: a swallowed certify-write leaves the gate permanently
    // inert (the burst returns on every scale-up, invisibly) — log it.
    if let Err(e) = Query::new(MIGRATION_CERTIFY_SQL)
        .bind(OVERLAY_MIGRATION_COUNT as f64)
        .bind(migration_list_fingerprint() as f64)
        .execute(db)
        .await
    {
        worker::console_log!(
            "[#411] migration certify-write FAILED (gate inert until it lands): {e}"
        );
    }
    OVERLAY_MIGRATIONS_APPLIED.store(true, Ordering::Release);
    Ok(())
}

/// True IFF the persisted `overlay_migration_count` equals
/// [`OVERLAY_MIGRATION_COUNT`]. Any read error (missing table on first boot,
/// D1 fault) or mismatch answers `false` — the caller then runs the full
/// idempotent list, the pre-gate behavior. Extracted for the comparison test.
async fn migration_state_current(db: &D1Database) -> bool {
    #[derive(serde::Deserialize)]
    struct CountRow {
        value: f64,
    }
    let count_ok =
        match Query::new("SELECT value FROM ops_counters WHERE name = 'overlay_migration_count'")
            .fetch_optional::<CountRow>(db)
            .await
        {
            Ok(Some(row)) => migration_count_matches(row.value),
            _ => false,
        };
    if !count_ok {
        return false;
    }
    match Query::new("SELECT value FROM ops_counters WHERE name = 'overlay_migration_fp'")
        .fetch_optional::<CountRow>(db)
        .await
    {
        Ok(Some(row)) => row.value == migration_list_fingerprint() as f64,
        _ => false,
    }
}

/// PURE: does a persisted counter value certify the CURRENT migration list?
/// Exact equality only — an older count (upgrade pending) and a NEWER count
/// (a rollback to an older worker; its shorter list must still re-run so its
/// own tail statements exist) both answer false.
pub fn migration_count_matches(persisted: f64) -> bool {
    persisted == OVERLAY_MIGRATION_COUNT as f64
}

/// The certify upsert — two rows in one statement (count + content
/// fingerprint). Factored so a pin can assert the SHIPPED string's shape.
pub const MIGRATION_CERTIFY_SQL: &str =
    "INSERT INTO ops_counters (name, value) VALUES ('overlay_migration_count', ?1), \
     ('overlay_migration_fp', ?2) \
     ON CONFLICT(name) DO UPDATE SET value = excluded.value";

/// Review HIGH-2: certify CONTENT, not just count — an in-place edit of a
/// shipped statement keeps the list length unchanged and would otherwise
/// never apply anywhere again (a self-healing failure traded for a permanent
/// one). FNV-1a per statement, order-sensitive wrapping sum, folded to u32
/// (exactly representable in the ops_counters REAL).
pub fn migration_list_fingerprint() -> u32 {
    let mut acc: u32 = 0;
    for sql in OVERLAY_MIGRATIONS {
        let mut h: u32 = 0x811c_9dc5;
        for b in sql.as_bytes() {
            h ^= *b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        acc = acc.wrapping_add(h);
    }
    acc
}

/// Number of overlay migration statements.
///
/// A LITERAL, deliberately — `OVERLAY_MIGRATIONS.len()` here would make
/// `migrations_are_valid_sql`'s equality move on both sides at once and assert
/// nothing (Rule 9). Bump it consciously when adding a migration.
/// 97 → 100 for #327 S8: `collected_markers_v2` + its data carry + its index.
/// 100 → 101 on the #283 merge: the `potparty_records.sigValid` ADD COLUMN.
/// 101 → 102 for #362: the `hopparty_records.markerValid` ADD COLUMN — the
/// same latch generalised to hop markers, which is what retires
/// `/hops-view`'s read-time ECDSA and its 150-row verify budget.
/// 102 → 103 for #355 + #367: `relatch_cursors`, the durable scan position of
/// the pass that re-latches BOTH verdict columns. One statement, one table,
/// overlay-only (no other service reads it, so epoch Rule 24 does not bite).
/// 103 → 104 for #217: `pot_records.firstSpentAt`, the WRITE-ONCE durable
/// hand-END anchor (`spentAt` is the #228 MOVING age gate and cannot double as
/// an audit fact). `/refund-view` reads it, so epoch Rule 24 DOES bite — the
/// app-layer issues the same statement itself (`low_app_layer::schema`).
/// 104 → 106 for bsv-low #371: `network_seen` (the overlay's OWN witness that
/// the network accepted a txid — never a caller's claim) +
/// `pot_records.spenderFinal` (the spender's bytes-finality, latched at spend
/// record time). Together they are the third verdict-gate arm: seen AND final
/// publishes at the SEEN finality bar; non-final (parked refunds, #323) and
/// unwitnessed (attacker-planted pointers, Rule 21) keep the merkle bar
/// verbatim. Both read by the app-layer, so epoch Rule 24 bites for both.
/// 106 → 107 for bsv-low handoff #2b: `pot_beefs.structurally_unprovable` —
/// the proof-poll retirement latch for stored txs that can NEVER acquire a
/// merkle proof because a chaintracks-verified spend of the same pot
/// outpoint by a DIFFERENT txid was recorded (the dominant class: a
/// superseded pre-signed refund, ~64% of refunds per bsv-low #369).
/// Overlay-internal — the app-layer reads `pot_beefs` only through explicit
/// `hex(beef)` joins, so epoch Rule 24 does NOT bite (no schema catch-up).
/// 111 → 112 for bsv-low #406: `pot_records.settleSigners` (who signed the
/// recorded spend — the verdict group's third member; display-tier ending
/// narration). See the migration comment.
/// 109 → 111 for brain-cutover M1: `claimValid` (tiered result-claim
/// verdict latch) + `rowValid` (hand-marker verdict latch) — see the
/// migration comments.
/// 107 → 109 for bsv-low #382: `hand_markers` (per-seat showdown-hand marker
/// index, outpoint-keyed from birth per the #327 S8 lesson) + its
/// gameId/createdAt read index. Overlay-internal (clients read via /lookup
/// ls_hand); display-only — no money path and no app-layer join reads it.
/// 117 → 118 for bsv-low #403 board paging (2026-08-29):
/// `idx_pot_records_chain_wins` — the whole-era chain-wins spine's scan
/// index (`low-app-layer logic::chain_wins_cte`). Index only, additive.
pub const OVERLAY_MIGRATION_COUNT: usize = 135;

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
    // BEEF proof-completion flag (#192/#193): 1 once a transactions-store row's
    // OWN merkle BUMP is stitched in (a proven, chaintracks-verified fact),
    // else 0. Written on every BEEF write; the proof-completion cron enumerates
    // ONLY proofless rows (`WHERE has_proof = 0 ORDER BY RANDOM()`), so it
    // reaches the whole historical backlog (not just the newest N) and never
    // re-fetches a proven row. Additive ALTER — the runner ignores the re-run
    // "duplicate column" error (`migration_error_is_benign`); existing rows
    // default to 0 (queued for completion).
    "ALTER TABLE transactions ADD COLUMN has_proof INTEGER NOT NULL DEFAULT 0",
    "CREATE INDEX IF NOT EXISTS idx_transactions_has_proof ON transactions(has_proof)",
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
    // LOW cross-device "already collected" markers, ORIGINAL (superseded)
    // shape — kept verbatim because this runner re-executes every statement
    // and shipped migrations are never edited. SUPERSEDED by
    // `collected_markers_v2` below: the (identity, gameId) first-marker-wins
    // primary key is the same squattable-namespace shape that was ripped out
    // of `result_markers` as an adversarial-review HIGH (bsv-low #327 S8,
    // epoch Rule 2). Admission is byte-format-only and the identityKey push is
    // arbitrary attacker-supplied bytes, so a garbage-sig marker naming a
    // VICTIM could permanently occupy the pair slot and censor that victim's
    // genuine marker forever. No code writes or reads this table anymore.
    "CREATE TABLE IF NOT EXISTS collected_markers (
        identity TEXT NOT NULL,
        gameId TEXT NOT NULL,
        txid TEXT,
        sigHex TEXT,
        createdAt INTEGER,
        PRIMARY KEY (identity, gameId)
    )",
    // LOW cross-device "already collected" markers (tm_collected /
    // ls_collected, bsv-low #161), CURRENT shape. One row per marker OUTPOINT
    // (txid, outputIndex) — EVERY admitted marker is kept: the lookup service
    // inserts with INSERT OR IGNORE on the primary key, so a replayed submit of
    // the same output is a no-op, but markers for the same (identity, gameId)
    // from DIFFERENT txs ALL coexist. That is the whole fix (epoch Rule 3:
    // exclusivity IS the bug — an index is a set, not a slot): a squatter can
    // now only ever occupy the worthless outpoint it actually fabricated, and
    // the victim's genuine marker sits alongside it. The CLIENT's signature
    // verify separates them, which is what `app/src/lib/collected.ts`'s
    // groupByKey + selectVerified was already written to do — that hardening
    // was DEAD CODE against the old schema, because the genuine sibling row
    // could never be stored (Rule 18: a fixture built from the same wrong model
    // as the code confirms the model).
    //
    // Rows are NEVER deleted (a collected fact is permanent, like a reveal; the
    // OP_RETURN is provably unspendable). txid + sigHex are handed back verbatim
    // to querying clients, which verify the sig under their OWN wallet — the
    // overlay never does.
    //
    // Why a NEW table instead of an in-place rebuild: the runner re-executes
    // every statement on every cold start, so a copy/DROP/RENAME dance would
    // re-run against the LIVE table on the next start. CREATE-only + a one-time
    // INSERT OR IGNORE carry (below) is re-run-safe.
    "CREATE TABLE IF NOT EXISTS collected_markers_v2 (
        identity TEXT NOT NULL,
        gameId TEXT NOT NULL,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        sigHex TEXT,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )",
    // Carry any rows admitted under the superseded shape into v2 with
    // outputIndex 0 (the old schema never stored the vout; 0 is a harmless
    // PK-only placeholder). Idempotent: OR IGNORE on the (txid, outputIndex)
    // key + the source table is write-frozen, so re-runs are no-ops. Old rows
    // with a NULL txid (nullable there, NOT NULL here) cannot be carried and
    // are skipped. NEVER orphans an honest row — an existing genuine marker
    // survives the re-key and keeps answering lookups.
    "INSERT OR IGNORE INTO collected_markers_v2 \
     (identity, gameId, txid, outputIndex, sigHex, createdAt) \
     SELECT identity, gameId, txid, 0, sigHex, createdAt \
     FROM collected_markers WHERE txid IS NOT NULL",
    // The ls_collected batched read filters `identity = ? AND gameId IN (…)`.
    "CREATE INDEX IF NOT EXISTS idx_collected_markers_v2_identity_gameId \
     ON collected_markers_v2(identity, gameId)",
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
    // LOW/result/v2 wire markers add the winner's five revealed cards
    // (the "lowest winning hand" leaderboard): 10 lowercase hex chars —
    // 5 card-index bytes, each 0..=51, distinct, parse-validated. NULL
    // for rows admitted from v1 wire markers (still accepted —
    // back-compat). Additive ALTER: the runner ignores the re-run
    // "duplicate column" error (`migration_error_is_benign`).
    "ALTER TABLE result_markers_v2 ADD COLUMN cardsHex TEXT",
    // LOW rung-3 transcript-proof bundle markers (tm_proof / ls_proof,
    // bsv-low leaderboard ladder rung 3). One row per marker OUTPOINT
    // (txid, outputIndex) — EVERY admitted marker kept via INSERT OR
    // IGNORE on the primary key (the tm_result censorship lesson:
    // admission is byte-format-only, so a (gameId, winner)-keyed
    // first-marker-wins index would let a garbage bundle front-run the
    // real proof for one OP_RETURN fee; with outpoint keying garbage and
    // genuine bundles coexist and the CLIENT verifies each). Rows are
    // NEVER deleted (a published proof is permanent; the OP_RETURN is
    // provably unspendable). `bundle` is the canonical-JSON proof-bundle
    // BYTES as a BLOB (the pot_beefs idiom — read back via hex());
    // ~10–15 KB each, format-capped at 64 KiB. The overlay never parses
    // or verifies it — clients check the transcript cryptography.
    "CREATE TABLE IF NOT EXISTS proof_markers (
        gameId TEXT NOT NULL,
        winner TEXT NOT NULL,
        sigHex TEXT,
        bundle BLOB NOT NULL,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )",
    // The ls_proof list query filters by (gameId, winner) and orders by
    // createdAt DESC.
    "CREATE INDEX IF NOT EXISTS idx_proof_markers_game_winner ON proof_markers(gameId, winner)",
    // LOW by-identity pot-participation markers (tm_potparty / ls_potparty,
    // bsv-low #188 — the seed-only recovery index). One row per marker
    // OUTPOINT (txid, outputIndex) — EVERY admitted marker is kept via
    // INSERT OR IGNORE on the primary key (the tm_result censorship lesson:
    // admission is byte-format-only, so an identity-keyed first-marker-wins
    // index would let a garbage marker front-run the real one for one
    // OP_RETURN fee; with outpoint keying garbage and genuine rows coexist).
    // Rows are NEVER deleted (a pot-participation fact is permanent recovery
    // history, like a pot record; the OP_RETURN is provably unspendable).
    // All byte fields are handed back verbatim to querying clients; the
    // overlay never verifies the sig. Each seat publishes its OWN marker, so
    // the (potTxid, potVout) index returns both parties.
    "CREATE TABLE IF NOT EXISTS potparty_records (
        identity TEXT NOT NULL,
        opponentIdentity TEXT NOT NULL,
        gameId TEXT NOT NULL,
        potTxid TEXT NOT NULL,
        potVout INTEGER NOT NULL,
        recoveryHeight INTEGER NOT NULL,
        sigHex TEXT,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )",
    // partyFor filters by identity; byPot filters by (potTxid, potVout);
    // both order by createdAt DESC.
    "CREATE INDEX IF NOT EXISTS idx_potparty_identity ON potparty_records(identity)",
    "CREATE INDEX IF NOT EXISTS idx_potparty_pot ON potparty_records(potTxid, potVout)",
    "CREATE INDEX IF NOT EXISTS idx_potparty_createdAt ON potparty_records(createdAt)",
    // LOW pre-signed refund-backup markers (tm_potrefund / ls_potrefund,
    // bsv-low #191 — the keyless recovery re-broadcast index). One row per
    // marker OUTPOINT (txid, outputIndex) — EVERY admitted marker is kept
    // via INSERT OR IGNORE on the primary key (the tm_result censorship
    // lesson: admission is byte-format-only, so an identity-/pot-keyed
    // first-marker-wins index would let a garbage marker front-run the real
    // one for one OP_RETURN fee; with outpoint keying garbage and genuine
    // rows coexist). Rows are NEVER deleted (a pre-signed refund backup is
    // permanent recovery history; the OP_RETURN is provably unspendable).
    // All byte fields (refundRawHex + sigHex) are handed back verbatim; the
    // overlay never parses or verifies them. BOTH seats may publish a backup
    // for a pot, so the (potTxid, potVout) index returns every one.
    "CREATE TABLE IF NOT EXISTS potrefund_records (
        identity TEXT NOT NULL,
        gameId TEXT NOT NULL,
        potTxid TEXT NOT NULL,
        potVout INTEGER NOT NULL,
        refundRawHex TEXT,
        sigHex TEXT,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )",
    // byPot filters by (potTxid, potVout); partyFor filters by identity;
    // both order by createdAt DESC.
    "CREATE INDEX IF NOT EXISTS idx_potrefund_pot ON potrefund_records(potTxid, potVout)",
    "CREATE INDEX IF NOT EXISTS idx_potrefund_identity ON potrefund_records(identity)",
    // pot_beefs proof-completion flag (#192/#193): 1 when the stored BEEF
    // STRUCTURALLY carries a BUMP for its OWN txid, else 0. Written on
    // every `store_beef`/`compact_pot_beef`. bsv-low#304: this flag is
    // derived from submitted bytes with NO SPV, so it is NOT a trust
    // signal — the completion-pass candidate set and every money-relevant
    // read moved to the VERIFIED `proof_verified` latch added below; this
    // column stays as the structural record (additive-only list).
    // A `compact_pot_beef` write BYPASSES the longer-wins guard — a bumped BEEF
    // is authoritative even when SHORTER (trimmed ancestry). Additive ALTER —
    // the runner ignores the re-run "duplicate column" error
    // (`migration_error_is_benign`); existing rows default to 0.
    "ALTER TABLE pot_beefs ADD COLUMN has_proof INTEGER NOT NULL DEFAULT 0",
    "CREATE INDEX IF NOT EXISTS idx_pot_beefs_has_proof ON pot_beefs(has_proof)",
    // ── Observability (#192/#193, P4) ─────────────────────────────────────
    // The proof-completion cron's most expensive omission in zanaadu was a
    // dead pass hiding for WEEKS. These three tables make a dead completion
    // pass surface in a DAY (see `crate::ops` + `GET /health/invariants`).
    //
    // ops_heartbeat: a SINGLETON (id = 0) upserted at the end of every cron
    // completion pass — `last_tick_ms` is the wall-clock of the last live
    // pass, `tick_count` the monotonic pass count. `/health/invariants`
    // reads it to 503 when the pass has been dead too long.
    "CREATE TABLE IF NOT EXISTS ops_heartbeat (
        id INTEGER PRIMARY KEY,
        last_tick_ms INTEGER NOT NULL DEFAULT 0,
        tick_count INTEGER NOT NULL DEFAULT 0
    )",
    // ops_counters: persistent monotonic counters
    // (proofs_completed_total / fetch_failed_total / pot_beefs_compacted_total),
    // incremented each tick via an upsert. Name-keyed so new counters are
    // additive without a schema change.
    "CREATE TABLE IF NOT EXISTS ops_counters (
        name TEXT PRIMARY KEY,
        value INTEGER NOT NULL DEFAULT 0
    )",
    // proofless_watch: first-seen ledger for proofless txids. A tx still
    // proofless > 24h is flagged (logged + counted) — the signal that a
    // proof genuinely isn't landing (vs. merely not-yet-mined). Rows are
    // deleted once the tx proves, so the table stays bounded to the live
    // proofless set.
    "CREATE TABLE IF NOT EXISTS proofless_watch (
        txid TEXT PRIMARY KEY,
        first_seen_ms INTEGER NOT NULL
    )",
    // Spend-confirmation chaser (#186): the candidate scan is
    // `WHERE spent = 1 AND spentConfirmed = 0 ORDER BY RANDOM()`. A composite
    // index so the scan doesn't table-scan pot_records as the landing-proof
    // table grows. Additive, IF NOT EXISTS.
    "CREATE INDEX IF NOT EXISTS idx_pot_spent_unconfirmed ON pot_records(spent, spentConfirmed)",
    // ── Push-primary backstop age anchors (bsv-low #228 / arcade#259) ─────
    // /arc-ingest is now the PRIMARY proof source (Arcade pushes a verified
    // merklePath ~150 ms post-MINED); the poll passes are a BACKSTOP that
    // only touches rows old enough that their push evidently isn't coming.
    //
    // transactions.created_at: first-store time (unix seconds), written
    // preserve-or-now on every BEEF write (`COALESCE(existing, unixepoch())`).
    // NULLABLE on purpose: pre-migration rows stay NULL and the candidate
    // query treats NULL as OLD (eligible) — the fail-safe direction (poll
    // more, never starve a row of its backstop). Additive ALTER — the runner
    // ignores the re-run "duplicate column" error.
    "ALTER TABLE transactions ADD COLUMN created_at INTEGER",
    // pot_records.spentAt: when the CURRENT spend pointer was recorded (unix
    // seconds), written by every accepted `mark_spent`. Anchors the #186
    // spend-chaser's age gate on the SPEND, not the pot admission (a pot can
    // be spent long after admission). NULL (pre-migration) = OLD/eligible.
    "ALTER TABLE pot_records ADD COLUMN spentAt INTEGER",
    // potparty v2 SEAT-BINDING fields (bsv-low #230): the seat's committed
    // settle pubkey + the settle key's signature over the seat-binding
    // preimage, carried VERBATIM (admission stays byte-format-only; the
    // app-layer reader verifies). NULLABLE on purpose: v1 rows stay NULL and
    // keep today's behaviour. Additive ALTERs — the runner ignores the
    // re-run "duplicate column" error (`migration_error_is_benign`).
    "ALTER TABLE potparty_records ADD COLUMN seatSettlePubkey TEXT",
    "ALTER TABLE potparty_records ADD COLUMN seatSigHex TEXT",
    // ── #284 decoded pot columns (decode-once at admission) ───────────────
    // The DECODED covenant params + the spend VERDICT, denormalized onto
    // pot_records so /results and /leaderboard become pure column reads
    // instead of re-parsing the pot_beefs BLOBs on every request. Every
    // value is a RE-PRESENTATION of bytes already admitted (the funding
    // lock's committed param pushes, the funding output's value, the exact
    // template-match verdict of the recorded spend) — a pure function of
    // hash-bound chain bytes, decoded by `overlay_discovery::pot::covenant`.
    // Admission stays BYTE-FORMAT-ONLY and the overlay still verifies no
    // signature and chooses no truth: the app-layer remains the verifying
    // reader (seat/claim signatures, marker hints). All NULLABLE additive
    // ALTERs (the runner ignores the re-run "duplicate column" error via
    // `migration_error_is_benign`); pre-#284 rows stay NULL and are decoded
    // lazily by the backfill pass (`proof_fetcher::backfill_decoded_params`)
    // or served via the read-path BEEF fallback.
    //
    // lockKind: 'covenant' | 'bare' | 'p2pkh'; NULL = not yet decoded (or
    // decode attempted on an unrecognized shape — paramsDecoded says which).
    "ALTER TABLE pot_records ADD COLUMN lockKind TEXT",
    // The committed SETTLE keys (66-hex lowercase compressed pubkeys).
    "ALTER TABLE pot_records ADD COLUMN pubA TEXT",
    "ALTER TABLE pot_records ADD COLUMN pubB TEXT",
    "ALTER TABLE pot_records ADD COLUMN pubTower TEXT",
    // The committed payout homes + rake home (40-hex lowercase hash160s).
    "ALTER TABLE pot_records ADD COLUMN payPkhA TEXT",
    "ALTER TABLE pot_records ADD COLUMN payPkhB TEXT",
    "ALTER TABLE pot_records ADD COLUMN rakePkh TEXT",
    // The committed amounts/height (script-number params, as integers).
    "ALTER TABLE pot_records ADD COLUMN stakeA INTEGER",
    "ALTER TABLE pot_records ADD COLUMN stakeB INTEGER",
    "ALTER TABLE pot_records ADD COLUMN feeSats INTEGER",
    "ALTER TABLE pot_records ADD COLUMN recoveryHeight INTEGER",
    // The funding output's value, from the admitted BEEF's parsed tx — what
    // the stake-conservation check compares stakeA+stakeB against.
    "ALTER TABLE pot_records ADD COLUMN potSats INTEGER",
    // 0 = decode not yet attempted (a backfill candidate); 1 = attempted and
    // recorded (lockKind says what the lock turned out to be).
    "ALTER TABLE pot_records ADD COLUMN paramsDecoded INTEGER NOT NULL DEFAULT 0",
    // The template-match verdict of the recorded spend
    // ('winner-a'|'winner-b'|'tie'|'refund'; NULL = unresolved/never
    // classified). Meaningful ONLY when verdictTxid = spendingTxid — a
    // later spend-pointer overwrite leaves a stale verdict behind on
    // purpose (the reader's equality check is the guard; see
    // `mark_spent_sql`). Bare pots NEVER get a stored verdict: their refund
    // rule depends on an unverified marker hint (app-layer-only).
    "ALTER TABLE pot_records ADD COLUMN verdict TEXT",
    "ALTER TABLE pot_records ADD COLUMN verdictTxid TEXT",
    // Block height from the SPV-verified BUMP at spend-confirm time; NULL
    // until proven. Written only alongside a CONFIRMED spend pointer.
    "ALTER TABLE pot_records ADD COLUMN spentHeight INTEGER",
    // The backfill candidate scan (`WHERE paramsDecoded = 0`).
    "CREATE INDEX IF NOT EXISTS idx_pot_params_undecoded ON pot_records(paramsDecoded)",
    // ── #291 hot-sort indexes (all additive CREATE INDEX IF NOT EXISTS) ───
    // Lobby (`ls_low findOpenTables`, every poll): `WHERE recordType = ?
    // [stake/expiry residuals] ORDER BY createdAt DESC LIMIT n` — the
    // composite serves the filter AND the sort in one backward index scan
    // (createdAt was previously unindexed → filesort per poll).
    "CREATE INDEX IF NOT EXISTS idx_low_type_created ON low_records(recordType, createdAt)",
    // potrefund hot queries both `ORDER BY createdAt DESC, rowid DESC`
    // (there was NO createdAt index at all on this table).
    "CREATE INDEX IF NOT EXISTS idx_potrefund_createdAt ON potrefund_records(createdAt)",
    // /leaderboard + /results: `WHERE winner = ? ORDER BY createdAt DESC` —
    // the two existing single-column indexes forced pick-one-then-sort.
    "CREATE INDEX IF NOT EXISTS idx_result_markers_v2_winner_created \
     ON result_markers_v2(winner, createdAt)",
    // potparty partyFor window: `WHERE identity = ? ORDER BY createdAt DESC`.
    "CREATE INDEX IF NOT EXISTS idx_potparty_identity_created \
     ON potparty_records(identity, createdAt)",
    // find_utxos_for_topic (GASP sync + admission): `WHERE topic = ? AND
    // spent = ? ORDER BY score` — the sole outputs index (txid, outputIndex,
    // topic) has no topic-leftmost prefix, so this was a full scan+filesort.
    "CREATE INDEX IF NOT EXISTS idx_outputs_topic_spent_score \
     ON outputs(topic, spent, score)",
    // Proof-completion candidate scan (#228 backstop age gate):
    // `WHERE has_proof = 0 AND (created_at IS NULL OR created_at <= ?)` —
    // the composite covers both predicates (supersedes the single-column
    // has_proof index for this query; that one stays, additive-only list).
    "CREATE INDEX IF NOT EXISTS idx_transactions_proof_created \
     ON transactions(has_proof, created_at)",
    // bsv-low #289: base64 of `bundle`, encoded ONCE at admission so the
    // ls_proof read path stops triple-transcoding the blob (hex() in SQL →
    // hex::decode → base64 per row, 2× bundle bytes on the D1 wire). A pure
    // re-presentation of the same admitted bytes (#284 precedent); the BLOB
    // column stays and NULL here falls back to it at read time. Additive
    // ALTER — the runner ignores the re-run "duplicate column" error.
    "ALTER TABLE proof_markers ADD COLUMN bundleB64 TEXT",
    // ── bsv-low#302: GASP peer health (dead-peer quarantine) ─────────────
    // One row per (host, topic) sync pairing the engine has ever attempted.
    // `consecutive_failures` resets to 0 on any successful sync; a timeout
    // or error increments it. `last_attempt` (unixepoch secs) ages the
    // re-probe window — quarantine-SKIPPED ticks do NOT touch the row (a
    // skip is not an attempt), so the window always re-opens. Rows are
    // NEVER deleted: quarantine is a skip-with-re-probe, not a removal.
    "CREATE TABLE IF NOT EXISTS gasp_peer_health (
        host TEXT NOT NULL,
        topic TEXT NOT NULL,
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        last_attempt INTEGER,
        last_success INTEGER,
        PRIMARY KEY (host, topic)
    )",
    // ── bsv-low#304: VERIFIED proof latch for pot_beefs ──────────────────
    // `has_proof` above is STRUCTURAL — `store_beef` latches it from the
    // submitted bytes with zero SPV, so a fake-bumped BEEF admitted via the
    // ungated paths (historical-tx / GASP sync / peer crawl) carried
    // has_proof = 1 and (a) escaped the completion pass forever and (b) fed
    // /tx-any's index leg an attacker-chosen confirmed/height. This column
    // is latched ONLY by the VERIFYING writers (`compact_pot_beef` after a
    // chaintracks-verified stitch, `mark_pot_beef_proven` after a
    // chaintracks re-verify of a stored bump); every admit-path write
    // resets it to 0. Money-relevant reads (the /tx-any confirmed/height
    // answer, the /beef serve-time trimming license, the completion-pass
    // candidate set) trust ONLY this latch. Existing rows default to 0 —
    // the completion pass re-verifies the backlog (stored-bump re-verify
    // first, so an honest backlog latches without external fetches).
    "ALTER TABLE pot_beefs ADD COLUMN proof_verified INTEGER NOT NULL DEFAULT 0",
    // The completion-pass candidate scan (`WHERE proof_verified = 0 …`).
    "CREATE INDEX IF NOT EXISTS idx_pot_beefs_proof_verified \
     ON pot_beefs(proof_verified)",
    // ── bsv-low #315: LOW hop-in-flight markers (tm_hopparty / ls_hopparty,
    // #252 stage 2b) ─────────────────────────────────────────────────────
    // One row per marker OUTPOINT (txid, outputIndex) — EVERY admitted
    // marker is kept via INSERT OR IGNORE on the primary key (the
    // tm_result censorship lesson; admission is byte-format-only, so an
    // identity-keyed first-marker-wins index would be front-runnable for
    // one dust OP_RETURN). Rows are NEVER deleted (a hop-in-flight fact is
    // permanent recovery history; the OP_RETURN is provably unspendable).
    //
    // THE MARKER RIDES THE HOP TX, so `txid` is BOTH the marker's
    // containing txid and the HOP txid — there is no `hopTxid` column,
    // because a transaction cannot embed its own txid and the container
    // already has it (the 2026-08-04 wire revision). The hop outpoint is
    // `(txid, hopVout)`.
    //
    // TYPED + INDEXED FROM THE FIRST MIGRATION (#310 decode-at-write):
    // every wire field is its own typed column, never a raw blob re-parsed
    // at read — AND the three `hop*OnChain`/`containerOutputs` columns are
    // the CONTAINER's own facts, decoded once at admission from the very
    // BEEF being admitted (the #284 posture: a pure re-presentation of
    // hash-bound bytes, chosen by nobody). They are what the app-layer
    // reader compares the marker's CLAIMS against, which is why the read
    // path needs no `outputs` join and no BEEF re-parse. hopLockHex /
    // hopSatsOnChain are NULL exactly when the container has no output at
    // hopVout — an absence PROVEN by containerOutputs, not guessed.
    // Signature bytes are carried back verbatim; the overlay never
    // verifies them (the reader labels; clients re-verify).
    "CREATE TABLE IF NOT EXISTS hopparty_records (
        identity TEXT NOT NULL,
        opponentIdentity TEXT NOT NULL,
        gameId TEXT NOT NULL,
        hopVout INTEGER NOT NULL,
        hopSats INTEGER NOT NULL,
        seatSettlePubkey TEXT NOT NULL,
        seatSigHex TEXT NOT NULL,
        identitySigHex TEXT NOT NULL,
        hopLockHex TEXT,
        hopSatsOnChain INTEGER,
        containerOutputs INTEGER NOT NULL,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )",
    // hopsFor filters by identity and orders by createdAt; the hop
    // outpoint (txid, hopVout) serves byHop and the /hops-view window; the
    // /live-view identity+gameId join key gets its own index.
    "CREATE INDEX IF NOT EXISTS idx_hopparty_identity ON hopparty_records(identity)",
    "CREATE INDEX IF NOT EXISTS idx_hopparty_identity_created \
     ON hopparty_records(identity, createdAt)",
    "CREATE INDEX IF NOT EXISTS idx_hopparty_hop ON hopparty_records(txid, hopVout)",
    "CREATE INDEX IF NOT EXISTS idx_hopparty_game ON hopparty_records(gameId)",
    // ── #283 potparty marker-validity latch (decode-once at admission) ────
    // Whether every signature the marker carries VERIFIED, computed once by
    // `overlay_discovery::potparty::validity::record_sig_valid` at write
    // time (the #284 decoded-columns pattern applied to a predicate instead
    // of a value). 1 = all verified, 0 = at least one did not, NULL = the
    // row predates this migration and was never evaluated.
    //
    // ORDERING HINT ONLY. Admission stays byte-format-only: this column
    // changes no admission decision, a 0-latched marker is still stored and
    // still served, and every consumer that draws a conclusion from a marker
    // re-verifies unconditionally — so a lying or stale value can mis-order
    // candidates and can never admit one. Its whole job is to let a SQL
    // window ORDER BY "does this verify", which is the only ordering an
    // attacker cannot out-stamp, out-number or (bsv-low#347) get for free.
    //
    // NULLABLE on purpose: a MIGRATION cannot backfill it, because SQL
    // cannot verify a signature.
    //
    // That is a fact about SQL, and an earlier revision of this note
    // presented it as a fact about the SYSTEM ("the legacy tier drains by
    // republish"). Both halves were wrong and the adversarial gate corrected
    // them (Rule 10). (a) The republish does NOT happen: the client's
    // `decidePartyStep` stops as soon as an indexed row exists for the pot,
    // and a legacy row IS an indexed row. (b) The overlay is RUST and every
    // input `record_sig_valid` needs is already in the row, so a bounded lazy
    // re-latch pass — `SELECT … LIMIT N` -> compute -> `UPDATE` — is
    // perfectly possible and small. **That pass now exists**
    // (`crate::relatch`, bsv-low#355 + #367), and it is what retires the
    // legacy tier; it was not in the #283 change itself.
    //
    // WRITE-ONCE AT ADMISSION, and that is the wider hazard (gate round 2,
    // MED-4). A TRANSIENT predicate fault (a `bsv-rs` DER/`to_der` behaviour
    // change, a wallet emitting a non-canonical signature mid-rollout, a
    // partial deploy) demotes every honest row admitted in that window to
    // rank 0 — BELOW the legacy tier. Pre-latch those rows ordered neutrally;
    // post-latch they are last. That is the Rule 6 trade, and its victims are
    // wiped-device users with a silently short enumeration who will never
    // file a bug (Rule 14). Until #355 it was PERMANENT, because this column
    // was set by `INSERT OR IGNORE` and by nothing else.
    //
    // #355 is therefore a RE-LATCH OF EVERY ROW, not a backfill of the NULL
    // ones, and its closure criterion is: **every row's `sigValid` equals
    // `record_sig_valid` recomputed at the pass's own predicate version** —
    // reported as a count of rows changed plus a count still `NULL`. The
    // narrower "zero rows with `sigValid IS NULL`" criterion structurally
    // SKIPS the 0s, which are exactly the rows a fault would have created.
    // The pass's UPDATE is the ONE other statement that writes this column
    // (`potparty_write::potparty_relatch_query`); the cursor it rides on is
    // `relatch_cursors`, at the end of this list.
    //
    // Additive ALTER — the runner ignores the re-run "duplicate column"
    // error (`migration_error_is_benign`). NOTE the app-layer Worker issues
    // this same statement itself (`low_app_layer::schema`) because it never
    // runs this list; the two are pinned byte-identical.
    "ALTER TABLE potparty_records ADD COLUMN sigValid INTEGER",
    // ── #362 hopparty marker-validity latch (decode-once at admission) ────
    // The #283 pattern, generalised to hop markers, and for a sharper
    // reason: `/hops-view` was answering "does this marker verify?" at READ
    // time — two ECDSA verifies plus a BRC-42 derivation plus a container
    // re-check, PER ROW, PER REQUEST — behind a rationing constant
    // (`HOPS_VIEW_VERIFY_BUDGET = 150`). A budget on a correctness check
    // means the check is on the wrong side of the system (epoch Rule 25):
    // reads scale with readers, admissions scale with writers, and under
    // read-time verification a junk row an attacker files ONCE makes every
    // honest reader pay the ECDSA to reject it, forever.
    //
    // `markerValid` is computed once by
    // `overlay_discovery::hopparty::validity::record_marker_valid` at write
    // time, from facts already in hand: 1 = the container's own output at
    // hopVout pays exactly the claimed hopSats to P2PKH(seatSettlePubkey)
    // AND both signatures verified; 0 = at least one bar failed; NULL = the
    // row predates this migration and was never evaluated.
    //
    // ORDERING HINT ONLY, exactly as sigValid: admission stays
    // byte-format-only, a 0-latched marker is still stored and still SERVED
    // (labelled `markerVerified: "unverified"`), and `/hops-view` carries
    // both signatures back so the client re-verifies. It is a LEADING SORT
    // KEY and never a `WHERE` — hiding a row would recreate the
    // invisible-money class (#358).
    //
    // What it buys that the read-time filter could not: verification now
    // happens BEFORE paging, so the top tier requires a container that
    // really pays the VICTIM'S OWN settle key. The previously-documented
    // evictions (a reactive flood at hop value + 1; one coin CHAINED through
    // k transactions at ~1.06x the hop in RECOVERABLE capital) no longer
    // reach it — measured in low-app-layer's hops_view_sqlite cells.
    //
    // NULLABLE on purpose: a MIGRATION cannot backfill it, because SQL
    // cannot verify a signature. And unlike potparty there is not even a
    // republish that could re-latch a legacy row — a hopparty marker must
    // ride the hop transaction, which is already on chain. So the re-latch
    // pass (`crate::relatch`, bsv-low#367) is the ONLY repair path this
    // column can ever have; its closure criterion is EVERY row's markerValid
    // equalling `record_marker_valid` recomputed at the pass's own predicate
    // version — never "zero rows with markerValid IS NULL", which
    // structurally skips the 0s a transient predicate fault would have
    // created (Rule 6/14).
    //
    // Additive ALTER — the runner ignores the re-run "duplicate column"
    // error (`migration_error_is_benign`). NOTE the app-layer Worker issues
    // this same statement itself (`low_app_layer::schema`) because it never
    // runs this list; the two are pinned byte-identical (epoch Rule 24).
    "ALTER TABLE hopparty_records ADD COLUMN markerValid INTEGER",
    // ── #355 + #367 re-latch cursors ──────────────────────────────────────
    // The durable scan position of the lazy RE-LATCH pass (`crate::relatch`),
    // one row per re-latched table, keyed by the table's own name.
    //
    // Both verdict columns above are written ONCE at admission and were, until
    // that pass, re-evaluated by nothing. So a TRANSIENT predicate fault (a
    // `bsv-rs` DER behaviour change, a wallet emitting a non-canonical
    // signature mid-rollout, a partial deploy) permanently demoted every
    // honest row admitted in its window to rank 0 — BELOW even the legacy
    // NULL tier — with no self-healing path (epoch Rule 6). The pass sweeps
    // EVERY row and rewrites any whose stored verdict differs from the
    // predicate recomputed now; this table is what makes that sweep resumable
    // across ticks instead of a repeated head-scan.
    //
    // `cursorRowid` is a rowid high-water mark WITHIN the current sweep and WRAPS
    // to 0 at the tail (`sweeps` then increments), because the criterion is a
    // FIXPOINT — "every row's verdict equals the predicate recomputed at the
    // pass's own version" — not a one-shot backfill. Losing this table costs
    // nothing but a restarted sweep: re-verifying a converged page writes
    // nothing.
    "CREATE TABLE IF NOT EXISTS relatch_cursors (
        tableName TEXT PRIMARY KEY,
        cursorRowid INTEGER NOT NULL DEFAULT 0,
        sweeps INTEGER NOT NULL DEFAULT 0
    )",
    // ── #217 durable hand-END anchor (the presence audit trail, RECORD half) ─
    // `pot_records.firstSpentAt`: the FIRST time this overlay recorded an
    // ACCEPTED spend pointer for this pot outpoint, in unix seconds. Written
    // preserve-or-now (`COALESCE(firstSpentAt, unixepoch())`) by every branch
    // of `d1_discovery::mark_spent_sql`, so it is WRITE-ONCE and MONOTONE.
    //
    // Why the existing `spentAt` could not serve this. `spentAt` is the #228
    // backstop AGE anchor and is deliberately RE-STAMPED by every accepted
    // spend write — an unconfirmed pointer, then its confirm, then a
    // reorg-displacing spender each reset it, on purpose, so the poll chaser
    // measures from the CURRENT pointer. That makes it a correct age gate and
    // a WRONG audit fact: read as "the hand ended at", it silently answers
    // "the last time anything happened to this pot's spend". A field whose
    // meaning depends on which writer touched it last is exactly the shape
    // #217 forbids, so the durable stamp is a SEPARATE column with a separate
    // name and neither is derivable from the other.
    //
    // PROVENANCE, stated because the consumer must not guess (epoch Rule 21's
    // covenant-vs-marker sort): this is a SERVER-OBSERVED time — when THIS
    // overlay saw the spend — not a network fact and not a client claim. The
    // network-anchored complement is `spentHeight` (the height of an
    // SPV-verified BUMP), which is why `/refund-view` serves the two together
    // and never collapses them. Nothing in this schema stores a
    // CLIENT-CLAIMED timestamp; if one is ever added it needs its own column
    // name, never this one.
    //
    // NULL is a first-class answer and is PERMANENT for rows whose spend was
    // recorded before this migration (epoch Rule 6/25: a migration cannot
    // backfill a time nobody observed, and there is no republish that could
    // re-latch it — unlike `sigValid`, no re-latch pass can ever repair this
    // column). NULL means "no accepted spend write since this shipped", never
    // "unspent" — the `spent`/`spentConfirmed` pair answers that.
    //
    // Additive ALTER — the runner ignores the re-run "duplicate column" error
    // (`migration_error_is_benign`). NOTE the app-layer Worker issues this
    // same statement itself (`low_app_layer::schema`) because it never runs
    // this list; the two are pinned byte-identical (epoch Rule 24).
    "ALTER TABLE pot_records ADD COLUMN firstSpentAt INTEGER",
    // ── bsv-low #371: the SEEN-latch pair that lets a verdict publish at the
    // product's real finality bar (owner ruling 3: SEEN_ON_NETWORK is finality;
    // merkle proofs arrive later and gate nothing).
    //
    // `network_seen`: one row per txid THIS OVERLAY ITSELF witnessed accepted
    // by the network — WRITER CENSUS (review 2026-08-26; keep in lockstep
    // with the routes latch pin, count 4): (a) the broadcast-gated submit arm
    // on Arcade's SEEN_ON_NETWORK verdict, (b) the ungated arm's backgrounded
    // Arcade corroboration (`GET /tx/{txid}` reaching SEEN-or-better, orphan
    // excluded per #267), (c) the #397 AcceptedPending background re-check —
    // latching only on a real network_witnessed answer, and (d) the #413
    // dual-broadcast delivery latch — only on the corroborator's >=SEEN
    // verdict of OUR OWN TAAL/GP broadcast. All four are broadcaster verdicts
    // of overlay-performed broadcasts. NEVER written from
    // a caller's claim: a stranger's /submit cannot mint a row for a tx the
    // network has not seen, which is exactly why the app-layer may trust it
    // (epoch Rule 21: an attacker-planted spend pointer carries no seen-latch
    // and stays behind the merkle bar). Write-once (INSERT OR IGNORE), keyed
    // on the txid the network accepted — not a claimable name (Rule 2).
    // Read by the app-layer's /results, /refund-view and /hops-view (epoch
    // Rule 24 bites: `low_app_layer::schema` issues this same statement,
    // pinned byte-identical).
    "CREATE TABLE IF NOT EXISTS network_seen (txid TEXT PRIMARY KEY, seenAt INTEGER)",
    // `pot_records.spenderFinal`: 1 when the recorded spender's OWN BYTES parse
    // as FINAL (`!(lockTime > 0 && any input sequence < 0xffffffff)`), 0 when
    // non-final, NULL = recorded before this shipped (falls back to the merkle
    // bar — honest, self-draining). Computed ONCE at spend-record time from the
    // spending tx the engine already parsed (epoch Rule 25: never re-parse at
    // read). A tower-parked NON-FINAL refund therefore keeps the #323
    // confirmed-only bar VERBATIM; only a final spend the overlay itself saw
    // network-accepted can publish early. NOTE the app-layer issues this same
    // statement (`low_app_layer::schema`, epoch Rule 24, pinned byte-identical).
    "ALTER TABLE pot_records ADD COLUMN spenderFinal INTEGER",
    // ── bsv-low handoff #2b: the proof-poll RETIREMENT latch.
    //
    // `pot_beefs.structurally_unprovable`: 1 = this stored tx can never
    // acquire a merkle proof — its OWN bytes spend a pot outpoint for which
    // a chaintracks-verified spend by a DIFFERENT txid has been recorded
    // (double-spend impossibility; the dominant class is a SUPERSEDED
    // pre-signed refund, which never mines BY DESIGN). NULL = pre-migration /
    // never examined — stays a full poll candidate (fail-safe: poll MORE).
    //
    // Latched ONLY at a confirm moment (epoch Rule 25 — the moment the fact
    // becomes knowable): the D1 confirm writers (`mark_spent(confirmed)`,
    // `mark_confirmed_for_spender` on a CAS hit) derive the superseded
    // sibling txids from `potrefund_records.refundRawHex` VERIFIED against
    // each raw's own bytes (the raw must actually spend the confirmed
    // outpoint — a fabricated marker can never latch an unrelated honest
    // tx). NEVER latched on an unconfirmed displacement (epoch Rule 6: a
    // displaced-unconfirmed pointer can re-win; that latch would be
    // permanent-vs-self-healing). Confirm BEATS the latch: every verified
    // writer (`compact_pot_beef`, `mark_pot_beef_proven`, the batch flip)
    // clears it, so a reorged-in row that really proves is never suppressed.
    // Surfaced on /health/invariants as `retiredUnprovableTotal` (Rule 13).
    // Overlay-internal: the app-layer reads pot_beefs only via explicit
    // hex(beef) joins — no Rule 24 catch-up.
    "ALTER TABLE pot_beefs ADD COLUMN structurally_unprovable INTEGER",
    // ── bsv-low #382: LOW per-seat showdown-hand markers (tm_hand/ls_hand).
    //
    // One row per marker OUTPOINT (the collected_markers_v2 #327 S8 key from
    // birth — (gameId, identity) are both public claimable names and admission
    // is byte-format-only, so any per-pair slot would be a censorship
    // primitive). Every admitted marker kept; rows never deleted (a revealed
    // hand is a permanent fact; the OP_RETURN is provably unspendable). The
    // sig is verified CLIENT-side only, publicly, under the row's own named
    // identity — the overlay derives no verdict and no view suppresses on
    // presence. Display index only: no money path reads it.
    // Overlay-internal for now (the client reads via /lookup ls_hand) — no
    // app-layer schema catch-up (epoch Rule 24 does not bite yet; if /results
    // later joins this table, the app-layer must issue this same statement).
    "CREATE TABLE IF NOT EXISTS hand_markers (
        gameId TEXT NOT NULL,
        identity TEXT NOT NULL,
        potTxid TEXT NOT NULL,
        cardsHex TEXT NOT NULL,
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        sigHex TEXT,
        createdAt INTEGER,
        PRIMARY KEY (txid, outputIndex)
    )",
    // The ls_hand batched read filters `gameId IN (…)` with a newest-first
    // per-game window.
    "CREATE INDEX IF NOT EXISTS idx_hand_markers_gameId_created \
     ON hand_markers(gameId, createdAt)",
    // 109 → 111, brain-cutover M1 (bsv-low docs/PLAN-BRAIN-CUTOVER-2026-08.md):
    // the #283/#362 verdict-latch family grows two members, computed ONCE at
    // admission by the D1 writer and repaired by the relatch fixpoint sweep.
    //
    // `claimValid` is a TIER, not a bool: NULL = admitted before the latch
    // (compute-at-serve until the sweep retires it), 0 = invalid (as if never
    // published), 1 = winner-sig-valid (the client's 'unconfirmed'),
    // 2 = countersigned ('confirmed'). `rowValid` is the boolean hand-marker
    // latch (verifyHandRow's recipe). BOTH are read by the app-layer
    // (/results, /leaderboard, the M2 hands join) — epoch Rule 24 bites:
    // the byte-identical statements live in low_app_layer::schema, and the
    // hand_markers CREATE above is mirrored there too (the migration comment
    // on it said "if /results later joins this table" — it now does).
    "ALTER TABLE result_markers_v2 ADD COLUMN claimValid INTEGER",
    "ALTER TABLE hand_markers ADD COLUMN rowValid INTEGER",
    // 111 → 112, bsv-low #406: `pot_records.settleSigners` — WHO SIGNED the
    // recorded spend ('coop' = the two seats, 'tower-a'/'tower-b' = the tower
    // + that seat, 'unresolved' = re-derived from durable bytes and no pair
    // verified, NULL = not yet established). Derived by verifying the spend's
    // own signatures against the committed key triple over the network's
    // BIP-143 digest (`overlay_discovery::pot::settle_signers_for_spend` —
    // the missing_j discriminator's server-side mirror). Part of the #284
    // VERDICT GROUP: written only alongside verdict/verdictTxid (same
    // statement / same CAS), so it shares the pointer lineage and readers
    // guard `verdictTxid == spendingTxid` exactly as for the verdict.
    // DISPLAY-TIER by contract: feeds the client's ending narration; it must
    // never become a COUNT/rank/WHERE bar without its own gate. Read by the
    // app-layer (/leaderboard, /results) — epoch Rule 24: the byte-identical
    // statement lives in low_app_layer::schema.
    "ALTER TABLE pot_records ADD COLUMN settleSigners TEXT",
    // #411 (2026-08-26): the two hottest app-layer read views measured 7.5s /
    // 3.6s wallTime under 16-pair burst (wrangler tail, attempt 7d live load).
    // /leaderboard's inner subquery aggregates pot_records per REQUEST
    // (GROUP BY txid -> MIN(createdAt)) and its window PARTITIONs
    // result_markers_v2 by potTxid; neither had a covering index, so both
    // were full scans growing with all history. Reference pattern
    // (ts-stack 2025-11-11-001-utxo-lookup-index.ts): one index per query
    // shape. Additive + idempotent (IF NOT EXISTS) per the M9 rerun rule.
    "CREATE INDEX IF NOT EXISTS idx_result_markers_v2_potTxid_createdAt      ON result_markers_v2(potTxid, createdAt)",
    "CREATE INDEX IF NOT EXISTS idx_pot_records_txid_createdAt      ON pot_records(txid, createdAt)",
    // #411 round 2 (2026-08-26): the indexes above lowered constants but the
    // window-fn spine still MATERIALIZES all of result_markers_v2 per request
    // (DENSE_RANK cannot be cut by LIMIT), measured 3-10s per cache miss
    // under 16-pair burst. `lb_marker_rows` is the WRITE-TIME spine: one row
    // per marker (rn ≤ 4 per pot, the stage-1 inner-row shape), stamped at
    // result-marker admission (`lb_row_insert_query`) and flipped
    // unknown→known at pot admission (`lb_pot_flip_sql`). `orderAt` is the
    // era/order anchor (COALESCE(potCreatedAt, potFirstMarkerAt)) stored so a
    // PLAIN index serves the read. The read path keeps the old windowed query
    // as a permanent fallback-and-backfill (an under-full page re-runs it and
    // materializes what it found), so a missed write SELF-HEALS and the board
    // never undercounts — the display window's trust model is unchanged (the
    // counting bars still re-check pot_records/network_seen at read).
    "CREATE TABLE IF NOT EXISTS lb_marker_rows (
        txid TEXT NOT NULL,
        outputIndex INTEGER NOT NULL,
        gameId TEXT NOT NULL,
        winner TEXT NOT NULL,
        loser TEXT NOT NULL,
        potTxid TEXT,
        settleTxid TEXT,
        winnerSigHex TEXT,
        loserSigHex TEXT,
        cardsHex TEXT,
        createdAt INTEGER,
        claimValid INTEGER,
        rn INTEGER NOT NULL,
        potCreatedAt INTEGER,
        potFirstMarkerAt INTEGER,
        orderAt INTEGER,
        unknownPot INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (txid, outputIndex)
    )",
    "CREATE INDEX IF NOT EXISTS idx_lb_marker_rows_page ON lb_marker_rows(unknownPot, orderAt DESC, potTxid)",
    "CREATE INDEX IF NOT EXISTS idx_lb_marker_rows_pot ON lb_marker_rows(potTxid)",
    // bsv-low #403 board paging (2026-08-29): the whole-era CHAIN-WINS SPINE
    // (`low-app-layer logic::chain_wins_cte`) aggregates `pot_records` by
    // verdict + landing + era stamp for EVERY page; one composite index
    // serves the scan. The potparty attribution subquery rides the existing
    // `idx_potparty_pot (potTxid, potVout)`. Additive + idempotent.
    "CREATE INDEX IF NOT EXISTS idx_pot_records_chain_wins ON pot_records(outputIndex, verdict, spent, createdAt)",
    // ── INCIDENT D1-CALLBACK-FLOOD 2026-09-01 (docs/INCIDENT-D1-CALLBACK-FLOOD
    // -2026-09-01.md in bsv-low): terminal broadcast verdicts become STATE.
    //
    // `arc_terminal`: one row per txid whose broadcaster reported a TERMINAL
    // status (REJECTED / DOUBLE_SPEND_ATTEMPTED — `ARCADE_FATAL_STATUSES`).
    // Written by `/arc-ingest` (the webhook already DELIVERED this verdict
    // ~99M times while the handler counted-and-discarded it) and by the
    // retire classifier's own poll. EVIDENCE, not a verdict: a row here never
    // retires anything by itself — retirement additionally requires the
    // multi-source absence bar (#212/#213/#214: one provider's word is never
    // a negative). Write-once per txid (INSERT OR IGNORE — first verdict
    // wins; `extra` keeps the reason text, e.g. the UTXO_SPENT competitor).
    "CREATE TABLE IF NOT EXISTS arc_terminal (
        txid TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        extra TEXT,
        first_ms INTEGER NOT NULL
    )",
    // `transactions.retired_ms/-reason`: the `transactions`-store twin of
    // pot_beefs.structurally_unprovable (#2b above) — a row PROVEN network-dead
    // (corroborated terminal verdict + both-indexer definitive 404) leaves
    // every retry pool (proof poll, rebroadcast backstop, proofless watch)
    // but is NEVER deleted: bytes stay stored and served. NULL = live (the
    // fail-safe direction: poll more). Additive ALTERs — the runner ignores
    // the re-run "duplicate column" error (`migration_error_is_benign`).
    "ALTER TABLE transactions ADD COLUMN retired_ms INTEGER",
    "ALTER TABLE transactions ADD COLUMN retired_reason TEXT",
    // `rebroadcast_state`: per-txid attempt ledger for the #273 rebroadcast
    // backstop. The incident's smell was RETRY-FOREVER (a definitively
    // REJECTED subject was re-presented every 15 min for 14 days, identical
    // to a transport blip); every attempt now lands here and candidacy is
    // gated on attempts + spacing (`rebroadcast_eligible`). Rows are state,
    // not history — one per txid, upserted per attempt.
    "CREATE TABLE IF NOT EXISTS rebroadcast_state (
        txid TEXT PRIMARY KEY,
        attempts INTEGER NOT NULL DEFAULT 0,
        last_ms INTEGER NOT NULL,
        last_outcome TEXT
    )",
    // P4 slice 2 (bsv-low, 2026-09-02): the hop container's OWN size + exact
    // fee, read once at admission from the admitted BEEF (`tx_facts`) so the
    // money views can list every tx's size/fee — the opponent's included.
    // Additive; NULL on pre-slice-2 rows; display tier.
    "ALTER TABLE hopparty_records ADD COLUMN sizeBytes INTEGER",
    "ALTER TABLE hopparty_records ADD COLUMN feeSats INTEGER",
    // bsv-low P4 slice 2 (2026-09-02): the pot FUNDING tx's own size + exact
    // fee, read at admission from the admitted BEEF (display tier, NULLable,
    // stored-wins). Additive; the runner ignores the re-run duplicate-column
    // error.
    "ALTER TABLE pot_records ADD COLUMN fundingSizeBytes INTEGER",
    "ALTER TABLE pot_records ADD COLUMN fundingFeeSats INTEGER",
    // …and the recorded SPENDER's, keyed by the pointer it describes
    // (`spenderFactsTxid == spendingTxid` is the reader's guard — the
    // `verdictTxid` idiom). Display tier, NULLable, CAS-written.
    "ALTER TABLE pot_records ADD COLUMN spenderFactsTxid TEXT",
    "ALTER TABLE pot_records ADD COLUMN spenderSizeBytes INTEGER",
    "ALTER TABLE pot_records ADD COLUMN spenderFeeSats INTEGER",
    // bsv-low P1.1 part b (2026-09-02): the proof bundle's admission-time
    // REPLAY verdict and both re-derived hands (`proof::replay`). Additive,
    // NULLable (NULL = admitted before the replay shipped — a reader decodes
    // the retained bundle bytes instead). Display tier: the receipt's showdown.
    "ALTER TABLE proof_markers ADD COLUMN bundleValid INTEGER",
    "ALTER TABLE proof_markers ADD COLUMN winnerSeat INTEGER",
    "ALTER TABLE proof_markers ADD COLUMN seatA TEXT",
    "ALTER TABLE proof_markers ADD COLUMN seatB TEXT",
    "ALTER TABLE proof_markers ADD COLUMN winnerCardsHex TEXT",
    "ALTER TABLE proof_markers ADD COLUMN loserCardsHex TEXT",
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
    fn migration_fingerprint_moves_on_content_and_certify_sql_is_two_row() {
        let fp = super::migration_list_fingerprint();
        assert_eq!(fp, super::migration_list_fingerprint(), "deterministic");
        assert_ne!(fp, 0, "a zero fingerprint would look like a missing row");
        assert!(super::MIGRATION_CERTIFY_SQL.contains("overlay_migration_count"));
        assert!(super::MIGRATION_CERTIFY_SQL.contains("overlay_migration_fp"));
        assert!(super::MIGRATION_CERTIFY_SQL.contains("ON CONFLICT(name)"));
    }

    #[test]
    fn migration_count_gate_is_exact_equality() {
        // #411 version gate: equal certifies; older (upgrade pending) and
        // NEWER (a rolled-back worker whose shorter list must still re-run)
        // both fall through to the full idempotent replay.
        assert!(super::migration_count_matches(
            super::OVERLAY_MIGRATION_COUNT as f64
        ));
        assert!(!super::migration_count_matches(
            (super::OVERLAY_MIGRATION_COUNT - 1) as f64
        ));
        assert!(!super::migration_count_matches(
            (super::OVERLAY_MIGRATION_COUNT + 1) as f64
        ));
        assert!(!super::migration_count_matches(0.0));
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
        assert!(!migration_error_is_benign(
            alter,
            "no such table: pot_records"
        ));
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

    /// bsv-low #315: `hopparty_records` is TYPED + INDEXED from its FIRST
    /// migration (#310 decode-at-write) — every wire field its own NOT NULL
    /// typed column, PK on the marker outpoint, and all four query indexes
    /// present as CREATE (idempotent), never a later ALTER.
    #[test]
    fn hopparty_records_migration_present_typed_and_indexed() {
        let create = OVERLAY_MIGRATIONS
            .iter()
            .find(|sql| sql.contains("CREATE TABLE IF NOT EXISTS hopparty_records"))
            .expect("hopparty_records CREATE TABLE migration exists");
        for col in [
            "identity TEXT NOT NULL",
            "opponentIdentity TEXT NOT NULL",
            "gameId TEXT NOT NULL",
            "hopVout INTEGER NOT NULL",
            "hopSats INTEGER NOT NULL",
            "seatSettlePubkey TEXT NOT NULL",
            "seatSigHex TEXT NOT NULL",
            "identitySigHex TEXT NOT NULL",
            "hopLockHex TEXT",
            "hopSatsOnChain INTEGER",
            "containerOutputs INTEGER NOT NULL",
            "txid TEXT NOT NULL",
            "outputIndex INTEGER NOT NULL",
            "createdAt INTEGER",
            "PRIMARY KEY (txid, outputIndex)",
        ] {
            assert!(create.contains(col), "hopparty_records must declare: {col}");
        }
        // The 2026-08-04 revision DELETED the hopTxid column: the marker
        // rides the hop tx, so `txid` is the hop txid. Asserted positively
        // (the needle is built split so it cannot match itself in source).
        let deleted = ["hop", "Txid"].concat();
        assert!(
            !create.contains(&deleted),
            "hopparty_records must NOT carry a {deleted} column — the container supplies it"
        );
        for idx in [
            "idx_hopparty_identity ON hopparty_records(identity)",
            "idx_hopparty_identity_created",
            "idx_hopparty_hop ON hopparty_records(txid, hopVout)",
            "idx_hopparty_game ON hopparty_records(gameId)",
        ] {
            assert!(
                OVERLAY_MIGRATIONS
                    .iter()
                    .any(|sql| sql.starts_with("CREATE INDEX IF NOT EXISTS") && sql.contains(idx)),
                "hopparty index missing: {idx}"
            );
        }
        // Every WIRE field is typed from the first migration; the only ALTER
        // this table ever takes is the #362 validity LATCH, which is a
        // DERIVED verdict rather than a wire field and therefore could not
        // have existed before the predicate did. Asserted as an exhaustive
        // list, so a future ALTER that quietly re-types or re-shapes a wire
        // column has to come here and say so.
        let alters: Vec<&&str> = OVERLAY_MIGRATIONS
            .iter()
            .filter(|sql| sql.trim_start().starts_with("ALTER TABLE hopparty_records"))
            .collect();
        // P4 slice 2 (bsv-low, 2026-09-02): two more additive, NULLABLE
        // columns — the container's own size and exact fee, read at admission
        // (`tx_facts`), display tier. Still no wire field is re-typed.
        assert_eq!(
            alters,
            vec![
                &"ALTER TABLE hopparty_records ADD COLUMN markerValid INTEGER",
                &"ALTER TABLE hopparty_records ADD COLUMN sizeBytes INTEGER",
                &"ALTER TABLE hopparty_records ADD COLUMN feeSats INTEGER",
            ],
            "hopparty_records takes exactly ONE ALTER — the #362 markerValid \
             latch, additive and NULLABLE (a pre-migration NULL must stay \
             observable: it means 'never evaluated', not 'refuted')"
        );
    }

    #[test]
    fn push_backstop_age_anchor_migrations_present() {
        // #228 push-primary backstop: the two additive age-anchor columns
        // exist and are NULLABLE (no NOT NULL / DEFAULT) — a pre-migration
        // NULL must remain observable so the candidate queries can treat
        // unknown age as OLD/eligible (fail-safe).
        for (table, column) in [("transactions", "created_at"), ("pot_records", "spentAt")] {
            let m = OVERLAY_MIGRATIONS
                .iter()
                .find(|sql| {
                    sql.trim_start()
                        .starts_with(&format!("ALTER TABLE {table}"))
                        && sql.contains(&format!("ADD COLUMN {column} INTEGER"))
                })
                .unwrap_or_else(|| panic!("missing {table}.{column} migration"));
            assert!(
                !m.to_uppercase().contains("NOT NULL"),
                "{table}.{column} must stay nullable (NULL = unknown age = eligible)"
            );
        }
    }

    #[test]
    fn result_markers_v2_cards_hex_migration_present() {
        // The additive v2-cards column migration exists and targets
        // result_markers_v2 (mirrors the pot_records spentConfirmed pin).
        assert!(OVERLAY_MIGRATIONS.iter().any(|sql| {
            sql.trim_start()
                .starts_with("ALTER TABLE result_markers_v2")
                && sql.contains("ADD COLUMN cardsHex TEXT")
        }));
    }

    #[test]
    fn decoded_pot_column_migrations_present_nullable_and_benign_class() {
        // #284: the decoded-param / verdict columns on pot_records. Every
        // one must be an additive ALTER (the ONLY statement class the
        // re-run-everything runner treats as benign on a duplicate-column
        // error) and NULLABLE (pre-#284 rows must stay observably
        // undecoded), except paramsDecoded which carries NOT NULL DEFAULT 0
        // (0 = backfill candidate).
        let nullable = [
            ("lockKind", "TEXT"),
            ("pubA", "TEXT"),
            ("pubB", "TEXT"),
            ("pubTower", "TEXT"),
            ("payPkhA", "TEXT"),
            ("payPkhB", "TEXT"),
            ("rakePkh", "TEXT"),
            ("stakeA", "INTEGER"),
            ("stakeB", "INTEGER"),
            ("feeSats", "INTEGER"),
            ("recoveryHeight", "INTEGER"),
            ("potSats", "INTEGER"),
            ("verdict", "TEXT"),
            ("verdictTxid", "TEXT"),
            ("spentHeight", "INTEGER"),
        ];
        for (column, ty) in nullable {
            let m = OVERLAY_MIGRATIONS
                .iter()
                .find(|sql| {
                    sql.trim_start().starts_with("ALTER TABLE pot_records")
                        && sql.contains(&format!("ADD COLUMN {column} {ty}"))
                })
                .unwrap_or_else(|| panic!("missing pot_records.{column} migration"));
            assert!(
                !m.to_uppercase().contains("NOT NULL"),
                "pot_records.{column} must stay nullable (NULL = not decoded)"
            );
            assert!(
                migration_error_is_benign(m, &format!("duplicate column name: {column}")),
                "pot_records.{column} re-run must be the benign class"
            );
        }
        // paramsDecoded is the one defaulted flag column.
        assert!(OVERLAY_MIGRATIONS.iter().any(|sql| {
            sql.trim_start().starts_with("ALTER TABLE pot_records")
                && sql.contains("ADD COLUMN paramsDecoded INTEGER NOT NULL DEFAULT 0")
        }));
        // The backfill candidate index is idempotent (IF NOT EXISTS).
        assert!(OVERLAY_MIGRATIONS.iter().any(|sql| sql
            .trim_start()
            .starts_with("CREATE INDEX IF NOT EXISTS idx_pot_params_undecoded")));
    }

    #[test]
    fn proof_bundle_b64_migration_present_nullable_and_benign_class() {
        // bsv-low #289: the admission-time base64 column on proof_markers.
        // Additive ALTER, NULLABLE (pre-#289 rows fall back to the BLOB at
        // read time — never erased), benign on re-run.
        let m = OVERLAY_MIGRATIONS
            .iter()
            .find(|sql| {
                sql.trim_start().starts_with("ALTER TABLE proof_markers")
                    && sql.contains("ADD COLUMN bundleB64 TEXT")
            })
            .expect("missing proof_markers.bundleB64 migration");
        assert!(
            !m.to_uppercase().contains("NOT NULL"),
            "proof_markers.bundleB64 must stay nullable (NULL = pre-#289 row)"
        );
        assert!(
            migration_error_is_benign(m, "duplicate column name: bundleB64"),
            "bundleB64 re-run must be the benign class"
        );
    }

    #[test]
    fn result_markers_carry_migration_is_rerun_safe() {
        // The non-CREATE/ALTER migrations: the `*_markers` → `*_markers_v2`
        // data carries. Pin the two properties that make each safe under the
        // re-run-everything runner: OR IGNORE (PK dedups replays) and the
        // NULL-txid filter (v2's txid is NOT NULL).
        //
        // #327 S8 added the SECOND carry (collected_markers → _v2). The count
        // is asserted POSITIVELY and each carry is matched by name, so adding
        // a third without pinning it fails here loudly rather than slipping
        // through an over-broad filter (Rule 9).
        let carries: Vec<&&str> = OVERLAY_MIGRATIONS
            .iter()
            .filter(|sql| sql.trim_start().to_uppercase().starts_with("INSERT"))
            .collect();
        assert_eq!(carries.len(), 2, "exactly two data-carry migrations");
        for (target, source) in [
            (
                "result_markers_v2",
                "FROM result_markers WHERE txid IS NOT NULL",
            ),
            (
                "collected_markers_v2",
                "FROM collected_markers WHERE txid IS NOT NULL",
            ),
        ] {
            let carry = carries
                .iter()
                .find(|sql| {
                    sql.trim_start()
                        .starts_with(&format!("INSERT OR IGNORE INTO {target}"))
                })
                .unwrap_or_else(|| panic!("no OR IGNORE carry into {target}"));
            assert!(carry.contains(source), "{target} carry source");
            assert!(carry.contains("outputIndex"), "{target} carry outputIndex");
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
            "low_records",
            "reveal_records",
            "pot_records",
            "pot_beefs",
            "collected_markers",
            "collected_markers_v2",
            "result_markers",
            "result_markers_v2",
            "proof_markers",
            "potparty_records",
            "potrefund_records",
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
            "idx_pot_spent_unconfirmed",
            "idx_pot_params_undecoded",
            "idx_result_markers_winner",
            "idx_result_markers_createdAt",
            "idx_result_markers_v2_winner",
            "idx_result_markers_v2_createdAt",
            "idx_proof_markers_game_winner",
            "idx_potparty_identity",
            "idx_potparty_pot",
            "idx_potparty_createdAt",
            "idx_potrefund_pot",
            "idx_potrefund_identity",
            // #291 hot-sort indexes
            "idx_low_type_created",
            "idx_potrefund_createdAt",
            "idx_result_markers_v2_winner_created",
            "idx_potparty_identity_created",
            "idx_outputs_topic_spent_score",
            "idx_transactions_proof_created",
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
