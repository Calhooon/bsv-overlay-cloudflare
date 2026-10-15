//! Ban storage — mainline overlay-express 2.2.0 BanService equivalent.
//!
//! Two ban kinds: `"domain"` (advertised URL) and `"outpoint"`
//! (`<txid>.<outputIndex>` string). Storage is a single D1 table keyed by
//! `(type, value)`.

use std::rc::Rc;
use worker::D1Database;

use crate::d1::Query;

/// One ban, as `/admin/bans` lists it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Ban {
    /// A stable id, `type:value`.
    #[serde(rename = "_id")]
    pub id: String,
    /// What is banned: `domain` or `outpoint`.
    #[serde(rename = "type")]
    pub ban_type: String,
    /// The banned domain or outpoint.
    pub value: String,
    /// When it was banned.
    #[serde(rename = "bannedAt")]
    pub banned_at: String,
    /// Who banned it, if recorded.
    #[serde(rename = "bannedBy")]
    pub banned_by: Option<String>,
    /// Why, if recorded.
    pub reason: Option<String>,
}

/// The D1 storage of bans (`banned_hosts`).
pub struct D1BanStorage {
    db: Rc<D1Database>,
}

impl D1BanStorage {
    /// A ban storage over the D1 database `db`.
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }

    /// Ban `value` of kind `ban_type` (replacing a held ban).
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

    /// Lift the ban of `value` of kind `ban_type`.
    pub async fn remove(&self, ban_type: &str, value: &str) -> Result<(), String> {
        Query::new("DELETE FROM banned_hosts WHERE type = ? AND value = ?")
            .bind(ban_type)
            .bind(value)
            .execute(&self.db)
            .await
    }

    /// Every ban, by descending ban time.
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
                banned_by: if r.banned_by.as_deref() == Some("") {
                    None
                } else {
                    r.banned_by
                },
                reason: if r.reason.as_deref() == Some("") {
                    None
                } else {
                    r.reason
                },
            })
            .collect())
    }

    /// How many domains and outpoints are banned.
    pub async fn counts(&self) -> Result<(u64, u64), String> {
        let rows: Vec<BanCountRow> =
            Query::new("SELECT type, COUNT(*) as count FROM banned_hosts GROUP BY type")
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
