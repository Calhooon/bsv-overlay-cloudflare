//! D1 implementation of the overlay_engine::Storage trait.
//!
//! Maps each Storage method to parameterized SQL queries against Cloudflare D1.
//! Schema defined in d1::OVERLAY_MIGRATIONS.
//!
//! Key D1 considerations:
//! - All numbers returned as f64 (cast to u32/u64/i64 as needed)
//! - BLOBs read via hex() SQL function, decoded with hex::decode()
//! - JSON arrays (outputsConsumed, consumedBy) stored as TEXT, parsed with serde_json

use std::rc::Rc;

use async_trait::async_trait;
use overlay_engine::storage::{Storage, StorageError, TransactionBeef};
use overlay_engine::types::{AppliedTransaction, Outpoint, Output};
use serde::Deserialize;
use worker::D1Database;

use crate::d1::{Query, WhereBuilder};

// =============================================================================
// D1 row types (deserialization from D1 result sets)
// =============================================================================

/// Row from outputs table. D1 returns all numbers as f64.
#[derive(Deserialize)]
struct OutputRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    /// hex-encoded via `hex(outputScript)` in SQL
    #[serde(rename = "outputScript")]
    output_script: Option<String>,
    topic: String,
    satoshis: Option<f64>,
    /// JSON string of Outpoint array
    #[serde(rename = "outputsConsumed")]
    outputs_consumed: Option<String>,
    /// JSON string of Outpoint array
    #[serde(rename = "consumedBy")]
    consumed_by: Option<String>,
    /// 0 or 1 as f64
    spent: Option<f64>,
    #[serde(rename = "blockHeight")]
    block_height: Option<f64>,
    score: Option<f64>,
    /// hex-encoded BEEF from transactions table (only present when JOINed)
    #[serde(default)]
    beef: Option<String>,
}

impl OutputRow {
    fn into_output(self) -> Output {
        Output {
            txid: self.txid,
            output_index: self.output_index as u32,
            output_script: self
                .output_script
                .and_then(|h| hex::decode(h).ok())
                .unwrap_or_default(),
            satoshis: self.satoshis.unwrap_or(0.0) as u64,
            topic: self.topic,
            spent: self.spent.unwrap_or(0.0) != 0.0,
            outputs_consumed: self
                .outputs_consumed
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            consumed_by: self
                .consumed_by
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default(),
            beef: self.beef.and_then(|h| hex::decode(h).ok()),
            block_height: self.block_height.map(|h| h as u32),
            score: self.score,
        }
    }
}

/// Row for count queries.
#[derive(Deserialize)]
struct CountRow {
    cnt: f64,
}

/// Row for GASP sync state.
#[derive(Deserialize)]
struct SyncStateRow {
    since: f64,
}

/// Row from `gasp_peer_health` (bsv-low#302): the failure streak plus the
/// AGE of the last attempt (computed in SQL so the engine needs no clock).
#[derive(Deserialize)]
struct PeerHealthRow {
    consecutive_failures: f64,
    /// `unixepoch() - last_attempt`; NULL when `last_attempt` is NULL
    /// (defensive — the upsert always stamps it).
    #[serde(rename = "secsSince")]
    secs_since: Option<f64>,
}

/// The SHIPPED upsert behind `record_peer_sync_outcome` (bsv-low#302) —
/// a const so the real-SQLite test executes the production string. Binds:
/// `?1` host, `?2` topic, `?3` success (1/0). Success resets the streak and
/// stamps `last_success`; failure increments the streak. `last_attempt` is
/// stamped either way (the re-probe window ages from here). The backend owns
/// the clock (`unixepoch()`), matching the engine contract.
pub(crate) const PEER_HEALTH_UPSERT_SQL: &str = "INSERT INTO gasp_peer_health \
     (host, topic, consecutive_failures, last_attempt, last_success) \
     VALUES (?1, ?2, CASE WHEN ?3 THEN 0 ELSE 1 END, unixepoch(), \
             CASE WHEN ?3 THEN unixepoch() ELSE NULL END) \
     ON CONFLICT(host, topic) DO UPDATE SET \
       consecutive_failures = CASE WHEN ?3 THEN 0 ELSE consecutive_failures + 1 END, \
       last_attempt = unixepoch(), \
       last_success = CASE WHEN ?3 THEN unixepoch() ELSE last_success END";

/// The SHIPPED health read (bsv-low#302). Age is computed relative in SQL
/// (`unixepoch() - last_attempt`) so wasm and the rusqlite test agree on
/// semantics without any host clock in the engine.
pub(crate) const PEER_HEALTH_SELECT_SQL: &str = "SELECT consecutive_failures, \
            (unixepoch() - last_attempt) AS secsSince \
     FROM gasp_peer_health WHERE host = ?1 AND topic = ?2";

/// Row for proof-completion scanning: a stored transaction's txid + its BEEF
/// read back as hex (`hex(beef) AS beef`, the same idiom as `OutputRow::beef`).
#[derive(Deserialize)]
struct TxBeefRow {
    txid: String,
    beef: Option<String>,
}

/// The two `transactions` upsert statements, factored so the real-SQLite
/// tier executes the SHIPPED strings (a transcribed copy could drift).
///
/// INSERT OR REPLACE deletes-and-reinserts, so every column not listed
/// silently NULLs — `created_at` preserve-or-stamps (#228, the backstop age
/// anchor) and `retired_ms`/`retired_reason` PRESERVE (INCIDENT
/// D1-CALLBACK-FLOOD 2026-09-01: a re-present of a retired tx must not
/// un-retire it and re-open the retry loops; a genuine revival heals via a
/// verified proof, and has_proof=1 outranks the latch at every consumer).
///
/// `INSERT_OUTPUT_TX_SQL` binds: ?1 txid, ?2 beef (admit path — forces
/// has_proof = 0, see the call-site doc). `UPDATE_TX_BEEF_SQL` binds: ?1
/// txid, ?2 beef, ?3 has_proof (the verified-stitch path) — and CONFIRM
/// BEATS THE LATCH: a stitch that lands has_proof = 1 clears the retire
/// columns (the pot_beefs #2b rule, mirrored) so a false retire self-heals.
pub(crate) const INSERT_OUTPUT_TX_SQL: &str =
    "INSERT OR REPLACE INTO transactions (txid, beef, has_proof, created_at, retired_ms, retired_reason) \
     VALUES (?1, ?2, 0, COALESCE((SELECT created_at FROM transactions WHERE txid = ?1), unixepoch()), \
             (SELECT retired_ms FROM transactions WHERE txid = ?1), \
             (SELECT retired_reason FROM transactions WHERE txid = ?1))";
pub(crate) const UPDATE_TX_BEEF_SQL: &str =
    "INSERT OR REPLACE INTO transactions (txid, beef, has_proof, created_at, retired_ms, retired_reason) \
     VALUES (?1, ?2, ?3, COALESCE((SELECT created_at FROM transactions WHERE txid = ?1), unixepoch()), \
             CASE WHEN ?3 = 1 THEN NULL ELSE (SELECT retired_ms FROM transactions WHERE txid = ?1) END, \
             CASE WHEN ?3 = 1 THEN NULL ELSE (SELECT retired_reason FROM transactions WHERE txid = ?1) END)";

/// Rebroadcast-backstop candidate row: TxBeefRow + the `rebroadcast_state`
/// attempt ledger (LEFT JOIN — NULL = never attempted). D1 returns numeric
/// columns as f64 (codebase convention).
#[derive(Deserialize)]
struct RebroadcastCandidateRow {
    txid: String,
    beef: Option<String>,
    attempts: Option<f64>,
    last_ms: Option<f64>,
}

/// One rebroadcast-backstop candidate with its attempt history (INCIDENT
/// D1-CALLBACK-FLOOD 2026-09-01: candidacy is now attempt-bounded — see
/// `proof_fetcher::rebroadcast_eligible`).
pub struct RebroadcastCandidate {
    pub tx: TransactionBeef,
    /// Prior recorded attempts (0 = never attempted).
    pub attempts: i64,
    /// Wall-clock ms of the last recorded attempt (0 = never).
    pub last_ms: i64,
}

// =============================================================================
// SQL fragments
// =============================================================================

const OUTPUT_COLS: &str = "\
    o.txid, o.outputIndex, hex(o.outputScript) as outputScript, \
    o.topic, o.satoshis, o.outputsConsumed, o.consumedBy, \
    o.spent, o.blockHeight, o.score";

const OUTPUT_COLS_BEEF: &str = "\
    o.txid, o.outputIndex, hex(o.outputScript) as outputScript, \
    o.topic, o.satoshis, o.outputsConsumed, o.consumedBy, \
    o.spent, o.blockHeight, o.score, hex(t.beef) as beef";

const FROM_OUTPUTS: &str = "FROM outputs o";
const FROM_OUTPUTS_BEEF: &str = "FROM outputs o LEFT JOIN transactions t ON o.txid = t.txid";

// =============================================================================
// D1Storage
// =============================================================================

/// Cloudflare D1 implementation of the overlay_engine Storage trait.
pub struct D1Storage {
    db: Rc<D1Database>,
}

impl D1Storage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }

    fn select_outputs(include_beef: bool) -> String {
        if include_beef {
            format!("SELECT {OUTPUT_COLS_BEEF} {FROM_OUTPUTS_BEEF}")
        } else {
            format!("SELECT {OUTPUT_COLS} {FROM_OUTPUTS}")
        }
    }

    /// SQL for one batched-outpoint chunk (bsv-low #289): a single row-value
    /// `IN (VALUES …)` query replacing `n` individual `find_output` round
    /// trips. `n` must be ≥ 1. Factored out so the real-SQLite test below can
    /// prove the syntax against the production schema.
    fn outputs_batch_sql(include_beef: bool, n: usize) -> String {
        let base = Self::select_outputs(include_beef);
        let placeholders = vec!["(?, ?)"; n].join(", ");
        format!("{base} WHERE (o.txid, o.outputIndex) IN (VALUES {placeholders})")
    }

    /// bsv-low#273 (gate LOW-1) — the rebroadcast backstop's OWN candidate
    /// window: proofless rows aged INTO candidacy after `min_age_secs`
    /// (younger rows are healthy 0-conf traffic) and OUT of it after
    /// `max_age_secs`. Without the ceiling, permanently-dead rows (a
    /// superseded/conflicting tx that can never land) churn as candidates
    /// forever and dilute the RANDOM sample the genuinely-rescuable rows
    /// share. Aged-out rows are NEVER deleted and remain full candidates of
    /// the proof-completion passes (`find_transactions_for_proof_check`,
    /// which this deliberately does not touch) — only backstop
    /// PRESENCE-PROBE/REBROADCAST candidacy expires. NULL `created_at`
    /// (pre-migration) rows are older than any sane ceiling by construction
    /// → excluded here (same direction).
    pub async fn find_rebroadcast_candidates(
        &self,
        limit: u64,
        min_age_secs: u64,
        max_age_secs: u64,
    ) -> Result<Vec<RebroadcastCandidate>, StorageError> {
        let sql = Self::rebroadcast_candidates_sql(limit, min_age_secs, max_age_secs);
        let rows: Vec<RebroadcastCandidateRow> =
            Query::new(sql).fetch_all(&self.db).await.map_err(d1_err)?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                r.beef
                    .and_then(|h| hex::decode(h).ok())
                    .filter(|b| !b.is_empty())
                    .map(|beef| RebroadcastCandidate {
                        tx: TransactionBeef { txid: r.txid, beef },
                        attempts: r.attempts.unwrap_or(0.0) as i64,
                        last_ms: r.last_ms.unwrap_or(0.0) as i64,
                    })
            })
            .collect())
    }

    /// The shipped SQL behind [`Self::find_rebroadcast_candidates`] —
    /// factored out so the real-SQLite test executes the SHIPPED string
    /// against the production schema.
    ///
    /// INCIDENT D1-CALLBACK-FLOOD 2026-09-01: two additions. (1) RETIRED rows
    /// (`retired_ms` latched on corroborated network-death) leave candidacy —
    /// the 11 UTXO_SPENT double-spends were re-presented every 15 min for up
    /// to 14 days because nothing could ever say "dead". (2) The
    /// `rebroadcast_state` attempt ledger rides along so the caller can gate
    /// on attempts + spacing (`rebroadcast_eligible`) — retry-forever was the
    /// incident's architecture smell, and the bound lives with the candidacy.
    fn rebroadcast_candidates_sql(limit: u64, min_age_secs: u64, max_age_secs: u64) -> String {
        format!(
            "SELECT t.txid, hex(t.beef) as beef, rs.attempts AS attempts, rs.last_ms AS last_ms \
             FROM transactions t \
             LEFT JOIN rebroadcast_state rs ON rs.txid = t.txid \
             WHERE t.has_proof = 0 \
               AND t.retired_ms IS NULL \
               AND t.created_at IS NOT NULL \
               AND t.created_at <= unixepoch() - {min_age_secs} \
               AND t.created_at >= unixepoch() - {max_age_secs} \
             ORDER BY t.created_at DESC LIMIT {limit}"
        )
    }

    /// INCIDENT D1-CALLBACK-FLOOD 2026-09-01 — record one rebroadcast attempt
    /// for `txid` (upsert: attempts increments, last_ms/last_outcome move).
    /// Best-effort: a lost write costs one extra future attempt, and the
    /// attempt cap still holds on the next recorded one.
    pub async fn record_rebroadcast_attempt(&self, txid: &str, outcome: &str) {
        let q = Query::new(
            "INSERT INTO rebroadcast_state (txid, attempts, last_ms, last_outcome) \
             VALUES (?, 1, ?, ?) \
             ON CONFLICT(txid) DO UPDATE SET \
                 attempts = rebroadcast_state.attempts + 1, \
                 last_ms = excluded.last_ms, \
                 last_outcome = excluded.last_outcome",
        )
        .bind(txid)
        .bind(js_sys::Date::now())
        .bind(outcome);
        if let Err(e) = q.execute(&self.db).await {
            worker::console_log!("[rebroadcast-backstop] attempt record failed for {txid}: {e}");
        }
    }

    /// Parse a serialized BEEF and report whether it carries a merkle proof for
    /// `txid` (the tx's OWN proof, not an ancestor's). Used to keep the
    /// `transactions.has_proof` flag accurate on every BEEF write so the
    /// proof-completion cron (#192/#193) can enumerate only proofless rows
    /// (`WHERE has_proof = 0`) and thus reach the whole backlog without
    /// re-parsing the entire table each tick. Unparseable BEEF → `false` (treat
    /// as proofless so it stays in the candidate set, where the engine re-parses
    /// + skips defensively).
    fn beef_has_proof(txid: &str, beef: &[u8]) -> bool {
        bsv_rs::transaction::Beef::from_binary(beef)
            .ok()
            .and_then(|b| {
                b.find_txid(txid)
                    .map(bsv_rs::transaction::BeefTx::has_proof)
            })
            .unwrap_or(false)
    }
}

fn d1_err(e: String) -> StorageError {
    StorageError::Database(e)
}

#[async_trait(?Send)]
impl Storage for D1Storage {
    async fn insert_output(&self, output: &Output) -> Result<(), StorageError> {
        // INSERT OR IGNORE — dedup on (txid, outputIndex, topic) unique index
        Query::new(
            "INSERT OR IGNORE INTO outputs \
             (txid, outputIndex, outputScript, topic, satoshis, \
              outputsConsumed, consumedBy, spent, blockHeight, score) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&*output.txid)
        .bind(output.output_index)
        .bind(output.output_script.as_slice())
        .bind(&*output.topic)
        .bind(output.satoshis)
        .bind(serde_json::to_string(&output.outputs_consumed).unwrap_or_else(|_| "[]".into()))
        .bind(serde_json::to_string(&output.consumed_by).unwrap_or_else(|_| "[]".into()))
        .bind(output.spent)
        .bind(output.block_height.map(|h| h as i64))
        .bind(output.score.unwrap_or(0.0))
        .execute(&self.db)
        .await
        .map_err(d1_err)?;

        // Upsert BEEF into transactions table if provided
        // Upsert the BEEF. `INSERT OR IGNORE` silently kept an EMPTY/short
        // pre-existing row, so a later insert of the REAL BEEF was dropped and
        // the tx stayed un-hydrated → the lookup returned an empty-BEEF row →
        // a fresh LOW table was undecodable/invisible to opponents (the
        // "vanishing table", 2026-07-11). Overwrite only when the incoming BEEF
        // is longer (never clobber a good row with a shorter/empty one).
        if let Some(ref beef) = output.beef {
            if !beef.is_empty() {
                // OR REPLACE (matching update_transaction_beef below), NOT
                // OR IGNORE: IGNORE silently kept an empty/short pre-existing
                // row so the real BEEF was dropped and the tx stayed
                // un-hydrated. The `!beef.is_empty()` guard means we only ever
                // write a real BEEF here, so REPLACE can never clobber a good
                // row with an empty one.
                // ADMIT-TIME BEEFs are NEVER trusted-proven (#192/#193 FIX 3).
                // `/submit` skips SPV on historical topics, so a submitted BEEF
                // may carry a merkle bump that is unverified — or outright
                // forged. A STRUCTURAL bump here is NOT a fact, and serve-time
                // BEEF trimming trusts `has_proof`, so latching one at admit
                // would let a forged bump be trimmed on. Force `has_proof = 0`
                // ALWAYS: the VERIFYING cron pass (`complete_missing_proofs` →
                // ChainProofFetcher → chaintracks verify → `mark_transaction_proven`,
                // or a genuine bump re-verified before it flips the flag) is the
                // SOLE thing that ever latches `has_proof = 1`.
                // created_at is preserve-or-stamp (#228 backstop age anchor):
                // a REPLACE keeps the original first-store time so the
                // push-primary backstop's age gate measures real age.
                // retired_ms/-reason preserve too (INCIDENT D1-CALLBACK-FLOOD
                // 2026-09-01): INSERT OR REPLACE deletes-and-reinserts, so an
                // unlisted column silently NULLs — a RE-PRESENT of a retired
                // tx would un-retire it and re-open the retry loops the latch
                // exists to close. A re-present is not proof of life; a
                // genuine revival heals through the verified proof push
                // (has_proof=1 outranks the latch at every consumer).
                Query::new(INSERT_OUTPUT_TX_SQL)
                    .bind(&*output.txid)
                    .bind(beef.as_slice())
                    .execute(&self.db)
                    .await
                    .map_err(d1_err)?;
            }
        }

        Ok(())
    }

    async fn delete_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        Query::new("DELETE FROM outputs WHERE txid = ? AND outputIndex = ? AND topic = ?")
            .bind(txid)
            .bind(output_index)
            .bind(topic)
            .execute(&self.db)
            .await
            .map_err(d1_err)?;

        // Clean up BEEF if no remaining outputs reference this txid
        let remaining: Option<CountRow> =
            Query::new("SELECT COUNT(*) as cnt FROM outputs WHERE txid = ?")
                .bind(txid)
                .fetch_optional(&self.db)
                .await
                .map_err(d1_err)?;

        if remaining.is_none_or(|r| r.cnt == 0.0) {
            Query::new("DELETE FROM transactions WHERE txid = ?")
                .bind(txid)
                .execute(&self.db)
                .await
                .map_err(d1_err)?;
        }

        Ok(())
    }

    async fn mark_utxo_as_spent(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        Query::new(
            "UPDATE outputs SET spent = 1 \
             WHERE txid = ? AND outputIndex = ? AND topic = ?",
        )
        .bind(txid)
        .bind(output_index)
        .bind(topic)
        .execute(&self.db)
        .await
        .map_err(d1_err)
    }

    async fn update_consumed_by(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        consumed_by: &[Outpoint],
    ) -> Result<(), StorageError> {
        let json = serde_json::to_string(consumed_by)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        Query::new(
            "UPDATE outputs SET consumedBy = ? \
             WHERE txid = ? AND outputIndex = ? AND topic = ?",
        )
        .bind(json)
        .bind(txid)
        .bind(output_index)
        .bind(topic)
        .execute(&self.db)
        .await
        .map_err(d1_err)
    }

    async fn update_transaction_beef(&self, txid: &str, beef: &[u8]) -> Result<(), StorageError> {
        // This is the proof-completion STITCH write-back — every caller
        // (`handle_new_merkle_proof` via the `/arc-ingest` callback and the
        // `complete_missing_proofs` cron) has ALREADY chaintracks-verified the
        // bump before it reaches here, so the bump stitched into `beef` is a
        // fact. It is therefore SAFE to latch has_proof from it (this is how a
        // row legitimately flips proofless → proven). Contrast `insert_output`,
        // which is the untrusted ADMIT path and always writes has_proof = 0.
        let has_proof = i64::from(Self::beef_has_proof(txid, beef));

        // INSERT OR REPLACE — txid is PRIMARY KEY, so this upserts.
        // created_at is preserve-or-stamp (#228): the stitch keeps the row's
        // original first-store time (age stays real for the backstop gate).
        // retired_ms/-reason preserve too (INCIDENT D1-CALLBACK-FLOOD
        // 2026-09-01: REPLACE deletes-and-reinserts — see `insert_output`).
        // A verified stitch that lands has_proof=1 makes the kept latch
        // irrelevant at every consumer (has_proof outranks it), so
        // preserving is safe even on the revival path.
        Query::new(UPDATE_TX_BEEF_SQL)
            .bind(txid)
            .bind(beef)
            .bind(has_proof)
            .execute(&self.db)
            .await
            .map_err(d1_err)
    }

    async fn mark_transaction_proven(&self, txid: &str) -> Result<(), StorageError> {
        // Lightweight flag-flip — NO BEEF rewrite (#192/#193). The
        // proof-completion cron calls this when a scanned candidate ALREADY
        // carries a valid proof but its has_proof flag is still 0 (e.g. a row
        // written before migration added the column, which defaulted every
        // existing row to 0). Without this the row is returned by
        // find_transactions_for_proof_check every tick and skipped, clogging the
        // LIMIT-n candidate window. Idempotent: re-running on a proven row is a
        // no-op.
        // Confirm beats the retire latch (INCIDENT D1-CALLBACK-FLOOD
        // 2026-09-01 — the pot_beefs #2b rule, mirrored): a verified proof
        // clears `retired_ms` so a false retire self-heals here too.
        Query::new(
            "UPDATE transactions SET has_proof = 1, retired_ms = NULL, retired_reason = NULL \
             WHERE txid = ?",
        )
        .bind(txid)
        .execute(&self.db)
        .await
        .map_err(d1_err)
    }

    async fn update_output_block_height(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        block_height: u32,
    ) -> Result<(), StorageError> {
        Query::new(
            "UPDATE outputs SET blockHeight = ? \
             WHERE txid = ? AND outputIndex = ? AND topic = ?",
        )
        .bind(block_height)
        .bind(txid)
        .bind(output_index)
        .bind(topic)
        .execute(&self.db)
        .await
        .map_err(d1_err)
    }

    async fn insert_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        Query::new("INSERT OR IGNORE INTO applied_transactions (txid, topic) VALUES (?, ?)")
            .bind(&*tx.txid)
            .bind(&*tx.topic)
            .execute(&self.db)
            .await
            .map_err(d1_err)
    }

    async fn does_applied_transaction_exist(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<bool, StorageError> {
        let row: Option<CountRow> = Query::new(
            "SELECT COUNT(*) as cnt FROM applied_transactions \
             WHERE txid = ? AND topic = ? LIMIT 1",
        )
        .bind(&*tx.txid)
        .bind(&*tx.topic)
        .fetch_optional(&self.db)
        .await
        .map_err(d1_err)?;

        Ok(row.is_some_and(|r| r.cnt > 0.0))
    }

    async fn find_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: Option<&str>,
        spent: Option<bool>,
        include_beef: bool,
    ) -> Result<Option<Output>, StorageError> {
        let mut wb = WhereBuilder::new()
            .eq("o.txid", txid)
            .eq("o.outputIndex", output_index);

        if let Some(t) = topic {
            wb = wb.eq("o.topic", t);
        }
        if let Some(s) = spent {
            wb = wb.eq("o.spent", s);
        }

        let (where_clause, params) = wb.build();
        let base = Self::select_outputs(include_beef);
        let sql = format!("{base}{where_clause} LIMIT 1");

        let mut query = Query::new(sql);
        for p in params {
            query = query.bind(p);
        }

        let row: Option<OutputRow> = query.fetch_optional(&self.db).await.map_err(d1_err)?;

        Ok(row.map(OutputRow::into_output))
    }

    /// Batched outpoint hydration (bsv-low #289): the engine's `/lookup`
    /// used to call `find_output` once per result row — one D1 round trip
    /// (plus a JOINed BEEF transfer) per lobby row. This override answers the
    /// whole set in `ceil(n / 40)` queries.
    ///
    /// Contract preserved from the default impl: results come back in INPUT
    /// order, outpoints with no stored row are skipped, and (matching
    /// `find_output`'s `LIMIT 1`) at most one row is returned per outpoint
    /// even when the same outpoint is admitted under several topics.
    async fn find_outputs_by_outpoints(
        &self,
        outpoints: &[Outpoint],
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        if outpoints.is_empty() {
            return Ok(Vec::new());
        }

        // D1 caps bound parameters per statement (100); two binds per
        // outpoint → chunks of 40 stay well clear of the limit.
        const CHUNK: usize = 40;

        let mut by_outpoint: std::collections::HashMap<(String, u32), Output> =
            std::collections::HashMap::new();
        for chunk in outpoints.chunks(CHUNK) {
            let sql = Self::outputs_batch_sql(include_beef, chunk.len());
            let mut query = Query::new(sql);
            for op in chunk {
                query = query.bind(&*op.txid).bind(op.output_index);
            }
            let rows: Vec<OutputRow> = query.fetch_all(&self.db).await.map_err(d1_err)?;
            for row in rows {
                let output = row.into_output();
                // First row per outpoint wins — same arbitrary-topic pick as
                // find_output's LIMIT 1.
                by_outpoint
                    .entry((output.txid.clone(), output.output_index))
                    .or_insert(output);
            }
        }

        Ok(outpoints
            .iter()
            .filter_map(|op| {
                by_outpoint
                    .get(&(op.txid.clone(), op.output_index))
                    .cloned()
            })
            .collect())
    }

    async fn find_outputs_for_transaction(
        &self,
        txid: &str,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        let base = Self::select_outputs(include_beef);
        let sql = format!("{base} WHERE o.txid = ?");

        let rows: Vec<OutputRow> = Query::new(sql)
            .bind(txid)
            .fetch_all(&self.db)
            .await
            .map_err(d1_err)?;

        Ok(rows.into_iter().map(OutputRow::into_output).collect())
    }

    /// Known residual (bsv-low #291 gate finding LOW-B — documented, not
    /// fixed): this is the `/requestSyncResponse` responder read, ordered
    /// by `score ASC` with the initiator's cursor being the bare GASP
    /// `since` score. A tie group of IDENTICAL scores at least as large as
    /// the clamped page size would re-serve the same page on every request
    /// — the initiator then terminates on cursor non-advance with the
    /// group's tail unreached (a silent gap, never a spin; see the
    /// termination rule in `gasp.rs`). The durable fix is a compound
    /// `(score, rowid)` cursor, but `since` is a single u64 on the GASP
    /// wire — changing it is out of bounds. Unconstructible in practice:
    /// scores are responder-local millisecond timestamps stamped one
    /// admission round-trip (HTTP submit + D1 write) at a time, so ~1,000
    /// rows sharing one millisecond cannot occur. Deliberately NO rowid
    /// tiebreak in the ORDER BY: under a hypothetical over-page tie group,
    /// SQLite's unspecified within-tie order lets successive runs serve
    /// varying subsets (probabilistic coverage), whereas a deterministic
    /// tiebreak would pin the exact same page forever.
    async fn find_utxos_for_topic(
        &self,
        topic: &str,
        since: Option<f64>,
        limit: Option<u64>,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        let mut wb = WhereBuilder::new()
            .eq("o.topic", topic)
            .eq("o.spent", false);

        if let Some(s) = since {
            wb = wb.gte("o.score", s);
        }

        let (where_clause, params) = wb.build();
        let base = Self::select_outputs(include_beef);
        let mut sql = format!("{base}{where_clause} ORDER BY o.score ASC");

        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {l}"));
        }

        let mut query = Query::new(sql);
        for p in params {
            query = query.bind(p);
        }

        let rows: Vec<OutputRow> = query.fetch_all(&self.db).await.map_err(d1_err)?;

        Ok(rows.into_iter().map(OutputRow::into_output).collect())
    }

    async fn update_last_interaction(
        &self,
        host: &str,
        topic: &str,
        since: u64,
    ) -> Result<(), StorageError> {
        // Upsert — PRIMARY KEY (host, topic)
        Query::new("INSERT OR REPLACE INTO host_sync_state (host, topic, since) VALUES (?, ?, ?)")
            .bind(host)
            .bind(topic)
            .bind(since)
            .execute(&self.db)
            .await
            .map_err(d1_err)
    }

    async fn get_last_interaction(&self, host: &str, topic: &str) -> Result<u64, StorageError> {
        let row: Option<SyncStateRow> =
            Query::new("SELECT since FROM host_sync_state WHERE host = ? AND topic = ?")
                .bind(host)
                .bind(topic)
                .fetch_optional(&self.db)
                .await
                .map_err(d1_err)?;

        Ok(row.map_or(0, |r| r.since as u64))
    }

    async fn record_peer_sync_outcome(
        &self,
        host: &str,
        topic: &str,
        success: bool,
    ) -> Result<(), StorageError> {
        Query::new(PEER_HEALTH_UPSERT_SQL)
            .bind(host)
            .bind(topic)
            .bind(success)
            .execute(&self.db)
            .await
            .map_err(d1_err)
    }

    async fn get_peer_sync_health(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<overlay_engine::storage::PeerSyncHealth, StorageError> {
        let row: Option<PeerHealthRow> = Query::new(PEER_HEALTH_SELECT_SQL)
            .bind(host)
            .bind(topic)
            .fetch_optional(&self.db)
            .await
            .map_err(d1_err)?;
        Ok(row
            .map(|r| overlay_engine::storage::PeerSyncHealth {
                consecutive_failures: r.consecutive_failures.max(0.0) as u64,
                secs_since_last_attempt: r.secs_since.map(|s| s.max(0.0) as u64),
            })
            .unwrap_or_default())
    }

    async fn find_transactions_for_proof_check(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<TransactionBeef>, StorageError> {
        // Return ONLY proofless rows (#192/#193). The has_proof flag is written
        // on every BEEF write, so this is a direct, cheap, indexed scan that
        // reaches the historical backlog — not just the newest N rows — and
        // never re-fetches rows that are already proven. Existing rows from
        // before the migration default to has_proof = 0, so the legacy backlog
        // is naturally queued; as the cron completes proofs the matching rows
        // flip to 1 and drop out.
        //
        // ORDER BY RANDOM(): the window is bounded, and a fixed (insertion)
        // order lets never-mineable rows at the head starve the tail forever
        // (zanaadu prod incident). Random sampling guarantees every proofless
        // row is eventually visited regardless of backlog shape.
        //
        // Push-primary backstop age gate (#228): rows younger than
        // min_age_secs are excluded — their proof is expected via /arc-ingest.
        // NULL created_at (pre-migration) is treated as OLD/eligible
        // (fail-safe: poll more, never starve a row of its backstop).
        // INCIDENT D1-CALLBACK-FLOOD 2026-09-01: retired rows (corroborated
        // network-dead) leave the poll pool too — polling a proven double-
        // spend's proof is a courier GET burned every tick, forever.
        let sql = format!(
            "SELECT txid, hex(beef) as beef FROM transactions \
             WHERE has_proof = 0 \
               AND retired_ms IS NULL \
               AND (created_at IS NULL OR created_at <= unixepoch() - {min_age_secs}) \
             ORDER BY RANDOM() LIMIT {limit}"
        );
        let rows: Vec<TxBeefRow> = Query::new(sql).fetch_all(&self.db).await.map_err(d1_err)?;

        Ok(rows
            .into_iter()
            .filter_map(|r| {
                r.beef
                    .and_then(|h| hex::decode(h).ok())
                    .filter(|b| !b.is_empty())
                    .map(|beef| TransactionBeef { txid: r.txid, beef })
            })
            .collect())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use overlay_engine::types::Outpoint;

    /// bsv-low#273 gate LOW-1: the backstop candidacy bracket, shipped SQL
    /// on the production schema — young rows (healthy 0-conf), aged-out
    /// rows (permanently-dead churn), NULL-stamped rows (pre-migration ⇒
    /// ancient) and proven rows are all excluded; only the in-bracket
    /// proofless row is a candidate. Aged-out ≠ deleted: the row stays in
    /// `transactions` and stays a proof-pass candidate.
    #[test]
    fn rebroadcast_candidates_bracket_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        let now: i64 = conn
            .query_row("SELECT unixepoch()", [], |r| r.get(0))
            .unwrap();
        let insert = |txid: &str, created_at: Option<i64>, has_proof: i64| {
            conn.execute(
                "INSERT INTO transactions (txid, beef, has_proof, created_at) \
                 VALUES (?1, x'beef', ?2, ?3)",
                rusqlite::params![txid, has_proof, created_at],
            )
            .unwrap();
        };
        insert("young", Some(now - 60), 0); // < min age — healthy 0-conf
        insert("inbracket", Some(now - 3600), 0); // the candidate
        insert("agedout", Some(now - 15 * 24 * 3600), 0); // > max age
        insert("nullstamp", None, 0); // pre-migration ⇒ ancient
        insert("proven", Some(now - 3600), 1); // already proven
                                               // INCIDENT D1-CALLBACK-FLOOD 2026-09-01: an in-bracket row RETIRED on
                                               // corroborated network-death leaves candidacy (it used to be
                                               // re-presented every 15 min for the whole 14-day bracket).
        insert("deadretired", Some(now - 3600), 0);
        conn.execute(
            "UPDATE transactions SET retired_ms = 1, retired_reason = 'test' \
             WHERE txid = 'deadretired'",
            [],
        )
        .unwrap();
        // …and the attempt ledger rides along (LEFT JOIN — the CALLER gates
        // on it via `rebroadcast_eligible`; candidacy itself still lists).
        conn.execute(
            "INSERT INTO rebroadcast_state (txid, attempts, last_ms, last_outcome) \
             VALUES ('inbracket', 2, 5, 'rejected')",
            [],
        )
        .unwrap();

        let sql = D1Storage::rebroadcast_candidates_sql(16, 30 * 60, 14 * 24 * 3600);
        let mut stmt = conn.prepare(&sql).expect("shipped SQL must parse");
        let got: Vec<(String, Option<f64>, Option<f64>)> = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<f64>>(2)?,
                    r.get::<_, Option<f64>>(3)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(got.len(), 1, "retired row must not be a candidate: {got:?}");
        assert_eq!(got[0].0, "inbracket");
        assert_eq!(got[0].1, Some(2.0), "attempt ledger joins onto candidacy");
        assert_eq!(got[0].2, Some(5.0));
        // …and the aged-out / NULL / retired rows remain STORED (nothing is
        // deleted by leaving candidacy; the proof-pass pool excludes only the
        // retired row, via its own `retired_ms IS NULL` arm).
        let all_proofless: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM transactions WHERE has_proof = 0",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            all_proofless, 5,
            "nothing was deleted or proven by aging out or retiring"
        );
    }

    /// INCIDENT D1-CALLBACK-FLOOD 2026-09-01: the SHIPPED transactions
    /// upserts PRESERVE the retire latch across INSERT OR REPLACE — a
    /// re-present (admit path) and a re-stitch both keep `retired_ms` /
    /// `retired_reason` and the original `created_at`; without the listed
    /// columns REPLACE would NULL them and un-retire a dead tx.
    #[test]
    fn transactions_upserts_preserve_retire_latch_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        let admit = |txid: &str| {
            conn.execute(
                INSERT_OUTPUT_TX_SQL,
                rusqlite::params![txid, vec![0xbeu8, 0xef]],
            )
            .unwrap();
        };
        admit("tx1");
        let created0: i64 = conn
            .query_row(
                "SELECT created_at FROM transactions WHERE txid = 'tx1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "UPDATE transactions SET retired_ms = 77, retired_reason = 'dead', \
             created_at = created_at - 500 WHERE txid = 'tx1'",
            [],
        )
        .unwrap();
        // The RE-PRESENT: the latch and the (backdated) first-store time survive.
        admit("tx1");
        let (retired, reason, created): (Option<i64>, Option<String>, i64) = conn
            .query_row(
                "SELECT retired_ms, retired_reason, created_at FROM transactions WHERE txid = 'tx1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(retired, Some(77), "a re-present must not un-retire");
        assert_eq!(reason.as_deref(), Some("dead"));
        assert_eq!(
            created,
            created0 - 500,
            "created_at preserve-or-stamp holds"
        );
        // The verified-stitch upsert preserves the latch while still
        // proofless (a re-stitch of unproven bytes is not proof of life)…
        conn.execute(
            UPDATE_TX_BEEF_SQL,
            rusqlite::params!["tx1", vec![0xbeu8, 0xef, 0x01], 0i64],
        )
        .unwrap();
        let retired_still: Option<i64> = conn
            .query_row(
                "SELECT retired_ms FROM transactions WHERE txid = 'tx1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            retired_still,
            Some(77),
            "an unproven re-stitch keeps the latch"
        );
        // …and CONFIRM BEATS THE LATCH: a stitch landing has_proof = 1 clears
        // it (the pot_beefs #2b rule mirrored) — as does `mark_transaction_proven`.
        conn.execute(
            UPDATE_TX_BEEF_SQL,
            rusqlite::params!["tx1", vec![0xbeu8, 0xef, 0x02], 1i64],
        )
        .unwrap();
        let (retired2, reason2, proof): (Option<i64>, Option<String>, i64) = conn
            .query_row(
                "SELECT retired_ms, retired_reason, has_proof FROM transactions WHERE txid = 'tx1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(retired2, None, "a verified proof clears the retire latch");
        assert_eq!(reason2, None);
        assert_eq!(proof, 1);
    }

    /// bsv-low#302: the SHIPPED peer-health upsert + select on the
    /// production schema — failure streaks accumulate, one success resets
    /// to 0, and the select's relative age answers 0-ish for a fresh
    /// attempt. Executes the exact production strings (a transcribed copy
    /// could drift out from under the engine's quarantine rule).
    #[test]
    fn peer_health_upsert_and_select_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        let record = |host: &str, success: bool| {
            conn.execute(
                PEER_HEALTH_UPSERT_SQL,
                rusqlite::params!["https://".to_owned() + host, "tm_test", success],
            )
            .unwrap();
        };
        let health = |host: &str| -> (i64, Option<i64>) {
            conn.query_row(
                PEER_HEALTH_SELECT_SQL,
                rusqlite::params!["https://".to_owned() + host, "tm_test"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
        };

        // Three consecutive failures accumulate.
        record("dead", false);
        record("dead", false);
        record("dead", false);
        let (fails, age) = health("dead");
        assert_eq!(fails, 3);
        assert!(
            age.is_some_and(|a| (0..5).contains(&a)),
            "fresh attempt age ≈ 0: {age:?}"
        );

        // One success resets the streak to 0 — full re-admission.
        record("dead", true);
        assert_eq!(health("dead").0, 0);

        // …and a later failure starts a NEW streak from 1.
        record("dead", false);
        assert_eq!(health("dead").0, 1);

        // Unknown peer: no row — the trait maps that to the pristine
        // default (never quarantined).
        let missing: Option<i64> = conn
            .query_row(
                PEER_HEALTH_SELECT_SQL,
                rusqlite::params!["https://never-seen.example.com", "tm_test"],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(missing, None);
    }

    #[test]
    fn output_row_conversion_basic() {
        let row = OutputRow {
            txid: "abc123".into(),
            output_index: 2.0,
            output_script: Some("76a914".into()),
            topic: "tm_test".into(),
            satoshis: Some(1000.0),
            outputs_consumed: Some("[]".into()),
            consumed_by: Some("[]".into()),
            spent: Some(0.0),
            block_height: Some(850000.0),
            score: Some(42.5),
            beef: None,
        };
        let output = row.into_output();
        assert_eq!(output.txid, "abc123");
        assert_eq!(output.output_index, 2);
        assert_eq!(output.output_script, vec![0x76, 0xa9, 0x14]);
        assert_eq!(output.satoshis, 1000);
        assert_eq!(output.topic, "tm_test");
        assert!(!output.spent);
        assert_eq!(output.block_height, Some(850000));
        assert_eq!(output.score, Some(42.5));
        assert!(output.beef.is_none());
    }

    #[test]
    fn output_row_conversion_with_beef() {
        let row = OutputRow {
            txid: "abc".into(),
            output_index: 0.0,
            output_script: None,
            topic: "t".into(),
            satoshis: None,
            outputs_consumed: None,
            consumed_by: None,
            spent: Some(1.0),
            block_height: None,
            score: None,
            beef: Some("BEEF".into()),
        };
        let output = row.into_output();
        assert!(output.spent);
        assert!(output.output_script.is_empty());
        assert_eq!(output.satoshis, 0);
        assert!(output.block_height.is_none());
        assert_eq!(output.beef.unwrap(), vec![0xBE, 0xEF]);
    }

    #[test]
    fn output_row_json_arrays() {
        let consumed = vec![Outpoint::new("tx1", 0), Outpoint::new("tx2", 1)];
        let json = serde_json::to_string(&consumed).unwrap();

        let row = OutputRow {
            txid: "abc".into(),
            output_index: 0.0,
            output_script: None,
            topic: "t".into(),
            satoshis: None,
            outputs_consumed: Some(json.clone()),
            consumed_by: Some(json),
            spent: None,
            block_height: None,
            score: None,
            beef: None,
        };
        let output = row.into_output();
        assert_eq!(output.outputs_consumed.len(), 2);
        assert_eq!(output.outputs_consumed[0].txid, "tx1");
        assert_eq!(output.consumed_by[1].output_index, 1);
    }

    #[test]
    fn select_outputs_sql_no_beef() {
        let sql = D1Storage::select_outputs(false);
        assert!(sql.contains("FROM outputs o"));
        assert!(!sql.contains("LEFT JOIN"));
        assert!(!sql.contains("beef"));
    }

    #[test]
    fn select_outputs_sql_with_beef() {
        let sql = D1Storage::select_outputs(true);
        assert!(sql.contains("LEFT JOIN transactions t"));
        assert!(sql.contains("hex(t.beef) as beef"));
    }

    /// The batched-outpoint SQL (#289) must actually execute on the
    /// production schema — row-value `IN (VALUES …)` is newer SQLite surface
    /// area, so prove it against a real database built from
    /// `OVERLAY_MIGRATIONS`, and prove the row selection is per-OUTPOINT
    /// (txid alone must not match a different vout).
    #[test]
    fn outputs_batch_sql_selects_exact_outpoints_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }

        for (txid, vout) in [("aa", 0), ("aa", 1), ("bb", 0), ("cc", 7)] {
            conn.execute(
                "INSERT INTO outputs (txid, outputIndex, topic, spent) VALUES (?1, ?2, 'tm_t', 0)",
                rusqlite::params![txid, vout],
            )
            .unwrap();
            conn.execute(
                "INSERT OR IGNORE INTO transactions (txid, beef) VALUES (?1, x'beef')",
                rusqlite::params![txid],
            )
            .unwrap();
        }

        // Ask for (aa,1), (cc,7) and one absent outpoint (bb,9).
        let sql = D1Storage::outputs_batch_sql(true, 3);
        let mut stmt = conn
            .prepare(&sql)
            .expect("batch SQL must parse on real SQLite");
        let rows: Vec<(String, u32)> = stmt
            .query_map(
                rusqlite::params!["aa", 1u32, "cc", 7u32, "bb", 9u32],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?)),
            )
            .unwrap()
            .map(Result::unwrap)
            .collect();

        let mut sorted = rows.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![("aa".into(), 1), ("cc".into(), 7)],
            "exactly the requested present outpoints — never txid-only matches \
             (aa,0 / bb,0) nor phantom rows for absent outpoints"
        );
    }

    #[test]
    fn output_row_null_defaults() {
        let row = OutputRow {
            txid: "x".into(),
            output_index: 0.0,
            output_script: None,
            topic: "t".into(),
            satoshis: None,
            outputs_consumed: None,
            consumed_by: None,
            spent: None,
            block_height: None,
            score: None,
            beef: None,
        };
        let output = row.into_output();
        assert_eq!(output.satoshis, 0);
        assert!(!output.spent);
        assert!(output.outputs_consumed.is_empty());
        assert!(output.consumed_by.is_empty());
        assert!(output.output_script.is_empty());
        assert!(output.beef.is_none());
        assert!(output.block_height.is_none());
        assert!(output.score.is_none());
    }
}
