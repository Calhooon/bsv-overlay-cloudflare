//! Ban storage — mainline overlay-express 2.2.0 BanService equivalent.
//!
//! Two ban kinds: `"domain"` (advertised URL) and `"outpoint"`
//! (`<txid>.<outputIndex>` string). Storage is a single D1 table keyed by
//! `(type, value)`.

use std::rc::Rc;
use worker::D1Database;

use crate::d1::Query;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Ban {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "type")]
    pub ban_type: String,
    pub value: String,
    #[serde(rename = "bannedAt")]
    pub banned_at: String,
    #[serde(rename = "bannedBy")]
    pub banned_by: Option<String>,
    pub reason: Option<String>,
}

pub struct D1BanStorage {
    db: Rc<D1Database>,
}

impl D1BanStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }

    pub async fn add(
        &self,
        ban_type: &str,
        value: &str,
        banned_by: Option<&str>,
        reason: Option<&str>,
    ) -> Result<(), String> {
        Query::new(
            "INSERT OR REPLACE INTO banned_hosts (type, value, bannedBy, reason) VALUES (?, ?, ?, ?)",
        )
        .bind(ban_type)
        .bind(value)
        .bind(banned_by.unwrap_or(""))
        .bind(reason.unwrap_or(""))
        .execute(&self.db)
        .await
    }

    pub async fn remove(&self, ban_type: &str, value: &str) -> Result<(), String> {
        Query::new("DELETE FROM banned_hosts WHERE type = ? AND value = ?")
            .bind(ban_type)
            .bind(value)
            .execute(&self.db)
            .await
    }

    pub async fn list(&self) -> Result<Vec<Ban>, String> {
        let rows: Vec<BanRow> = Query::new(
            "SELECT type, value, bannedAt, bannedBy, reason FROM banned_hosts ORDER BY bannedAt DESC",
        )
        .fetch_all(&self.db)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| Ban {
                // Synthesise a stable _id from (type, value) — mainline uses
                // Mongo ObjectIds. Harness canonicaliser normalises _id so
                // the actual value is parity-invisible.
                id: format!("{}:{}", r.ban_type, r.value),
                ban_type: r.ban_type,
                value: r.value,
                banned_at: r.banned_at,
                banned_by: if r.banned_by.as_deref() == Some("") { None } else { r.banned_by },
                reason: if r.reason.as_deref() == Some("") { None } else { r.reason },
            })
            .collect())
    }

    pub async fn counts(&self) -> Result<(u64, u64), String> {
        let rows: Vec<BanCountRow> = Query::new(
            "SELECT type, COUNT(*) as count FROM banned_hosts GROUP BY type",
        )
        .fetch_all(&self.db)
        .await?;
        let mut domains = 0u64;
        let mut outpoints = 0u64;
        for r in rows {
            if r.ban_type == "domain" {
                domains = r.count as u64;
            } else if r.ban_type == "outpoint" {
                outpoints = r.count as u64;
            }
        }
        Ok((domains, outpoints))
    }
}

#[derive(serde::Deserialize)]
struct BanRow {
    #[serde(rename = "type")]
    ban_type: String,
    value: String,
    #[serde(rename = "bannedAt")]
    banned_at: String,
    #[serde(rename = "bannedBy")]
    banned_by: Option<String>,
    reason: Option<String>,
}

#[derive(serde::Deserialize)]
struct BanCountRow {
    #[serde(rename = "type")]
    ban_type: String,
    count: i64,
}
