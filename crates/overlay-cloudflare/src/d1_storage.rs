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
use overlay_engine::storage::{Storage, StorageError};
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
                Query::new("INSERT OR REPLACE INTO transactions (txid, beef) VALUES (?, ?)")
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
        // INSERT OR REPLACE — txid is PRIMARY KEY, so this upserts
        Query::new("INSERT OR REPLACE INTO transactions (txid, beef) VALUES (?, ?)")
            .bind(txid)
            .bind(beef)
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
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use overlay_engine::types::Outpoint;

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
