//! D1 implementation of SHIP/SLAP storage traits.
//!
//! Maps SHIPStorage and SLAPStorage methods to SQL against ship_records/slap_records tables.
//! Schema defined in d1::OVERLAY_MIGRATIONS.

use std::rc::Rc;

use async_trait::async_trait;
use overlay_discovery::agent::storage::{
    AgentDiscoveryRecord, AgentRecord, AgentStorage, AgentStorageError,
};
use overlay_discovery::collected::storage::{
    CollectedRecord, CollectedStorage, CollectedStorageError,
};
use overlay_discovery::dm_delegation::storage::{
    DmDelegationRecord, DmDelegationStorage, DmDelegationStorageError,
};
use overlay_discovery::low::storage::{LowRecord, LowStorage, LowStorageError};
use overlay_discovery::pot::storage::{
    pot_beef_has_proof, PotRecord, PotStorage, PotStorageError,
};
use overlay_discovery::potparty::storage::{
    PotpartyRecord, PotpartyStorage, PotpartyStorageError,
};
use overlay_discovery::potrefund::storage::{
    PotrefundRecord, PotrefundStorage, PotrefundStorageError,
};
use overlay_discovery::proof::storage::{ProofRecord, ProofStorage, ProofStorageError};
use overlay_discovery::result::storage::{ResultRecord, ResultStorage, ResultStorageError};
use overlay_discovery::reveal::storage::{RevealRecord, RevealStorage, RevealStorageError};
use overlay_discovery::ship::storage::{
    SHIPDiscoveryRecord, SHIPQuery, SHIPStorage, SHIPStorageError, SortOrder,
};
use overlay_discovery::slap::storage::{
    SLAPDiscoveryRecord, SLAPQuery, SLAPStorage, SLAPStorageError,
};
use overlay_discovery::uhrp::storage::{
    current_unix_seconds_i64, UHRPDiscoveryRecord, UHRPQuery, UHRPSortOrder, UHRPStorage,
    UHRPStorageError,
};
use overlay_engine::types::UTXOReference;
use serde::Deserialize;
use worker::D1Database;

use crate::d1::{QVal, Query, WhereBuilder};

// =============================================================================
// Row type
// =============================================================================

/// Row for SHIP/SLAP UTXO reference queries. D1 returns numbers as f64.
#[derive(Deserialize)]
struct UTXORow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
}

impl UTXORow {
    fn into_ref(self) -> UTXOReference {
        UTXOReference {
            txid: self.txid,
            output_index: self.output_index as u32,
        }
    }
}

/// Row for SHIP record queries with domain info (Janitor + advertiser).
#[derive(Deserialize)]
struct SHIPRecordRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "identityKey")]
    identity_key: String,
    domain: String,
    topic: String,
}

/// Row for SLAP record queries with domain info (Janitor + advertiser).
#[derive(Deserialize)]
struct SLAPRecordRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "identityKey")]
    identity_key: String,
    domain: String,
    service: String,
}

/// Row for existence checks.
#[derive(Deserialize)]
struct CountRow {
    cnt: f64,
}

// =============================================================================
// D1SHIPStorage
// =============================================================================

/// Cloudflare D1 implementation of the SHIPStorage trait.
pub struct D1SHIPStorage {
    db: Rc<D1Database>,
}

impl D1SHIPStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn ship_err(e: String) -> SHIPStorageError {
    SHIPStorageError::Database(e)
}

#[async_trait(?Send)]
impl SHIPStorage for D1SHIPStorage {
    async fn has_duplicate_record(
        &self,
        identity_key: &str,
        domain: &str,
        topic: &str,
    ) -> Result<bool, SHIPStorageError> {
        let row: Option<CountRow> = Query::new(
            "SELECT COUNT(*) as cnt FROM ship_records \
             WHERE identityKey = ? AND domain = ? AND topic = ?",
        )
        .bind(identity_key)
        .bind(domain)
        .bind(topic)
        .fetch_optional(&self.db)
        .await
        .map_err(ship_err)?;

        Ok(row.is_some_and(|r| r.cnt > 0.0))
    }

    async fn store_record(
        &self,
        txid: &str,
        output_index: u32,
        identity_key: &str,
        domain: &str,
        topic: &str,
    ) -> Result<(), SHIPStorageError> {
        Query::new(
            "INSERT INTO ship_records (txid, outputIndex, identityKey, domain, topic) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(txid)
        .bind(output_index)
        .bind(identity_key)
        .bind(domain)
        .bind(topic)
        .execute(&self.db)
        .await
        .map_err(ship_err)
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), SHIPStorageError> {
        Query::new("DELETE FROM ship_records WHERE txid = ? AND outputIndex = ?")
            .bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(ship_err)
    }

    async fn find_record(&self, query: &SHIPQuery) -> Result<Vec<UTXOReference>, SHIPStorageError> {
        let mut wb = WhereBuilder::new();

        if let Some(ref d) = query.domain {
            wb = wb.eq("domain", &**d);
        }
        if let Some(ref topics) = query.topics {
            let vals: Vec<QVal> = topics.iter().map(|t| QVal::Text(t.clone())).collect();
            wb = wb.in_vals("topic", vals);
        }
        if let Some(ref ik) = query.identity_key {
            wb = wb.eq("identityKey", &**ik);
        }

        let (where_clause, params) = wb.build();

        let order = match query.sort_order {
            Some(SortOrder::Asc) => "ASC",
            _ => "DESC",
        };
        let mut sql = format!(
            "SELECT txid, outputIndex FROM ship_records{where_clause} ORDER BY createdAt {order}"
        );

        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(skip) = query.skip {
            sql.push_str(&format!(" OFFSET {skip}"));
        }

        let mut q = Query::new(sql);
        for p in params {
            q = q.bind(p);
        }

        let rows: Vec<UTXORow> = q.fetch_all(&self.db).await.map_err(ship_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
        sort_order: Option<SortOrder>,
    ) -> Result<Vec<UTXOReference>, SHIPStorageError> {
        self.find_record(&SHIPQuery {
            find_all: Some(true),
            limit,
            skip,
            sort_order,
            ..Default::default()
        })
        .await
    }

    async fn find_all_records(&self) -> Result<Vec<SHIPDiscoveryRecord>, SHIPStorageError> {
        let rows: Vec<SHIPRecordRow> = Query::new(
            "SELECT txid, outputIndex, identityKey, domain, topic \
             FROM ship_records ORDER BY createdAt DESC",
        )
        .fetch_all(&self.db)
        .await
        .map_err(ship_err)?;

        Ok(rows
            .into_iter()
            .map(|r| SHIPDiscoveryRecord {
                txid: r.txid,
                output_index: r.output_index as u32,
                identity_key: r.identity_key,
                domain: r.domain,
                topic: r.topic,
            })
            .collect())
    }
}

// =============================================================================
// D1SLAPStorage
// =============================================================================

/// Cloudflare D1 implementation of the SLAPStorage trait.
pub struct D1SLAPStorage {
    db: Rc<D1Database>,
}

impl D1SLAPStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn slap_err(e: String) -> SLAPStorageError {
    SLAPStorageError::Database(e)
}

#[async_trait(?Send)]
impl SLAPStorage for D1SLAPStorage {
    async fn has_duplicate_record(
        &self,
        identity_key: &str,
        domain: &str,
        service: &str,
    ) -> Result<bool, SLAPStorageError> {
        let row: Option<CountRow> = Query::new(
            "SELECT COUNT(*) as cnt FROM slap_records \
             WHERE identityKey = ? AND domain = ? AND service = ?",
        )
        .bind(identity_key)
        .bind(domain)
        .bind(service)
        .fetch_optional(&self.db)
        .await
        .map_err(slap_err)?;

        Ok(row.is_some_and(|r| r.cnt > 0.0))
    }

    async fn store_record(
        &self,
        txid: &str,
        output_index: u32,
        identity_key: &str,
        domain: &str,
        service: &str,
    ) -> Result<(), SLAPStorageError> {
        Query::new(
            "INSERT INTO slap_records (txid, outputIndex, identityKey, domain, service) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(txid)
        .bind(output_index)
        .bind(identity_key)
        .bind(domain)
        .bind(service)
        .execute(&self.db)
        .await
        .map_err(slap_err)
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), SLAPStorageError> {
        Query::new("DELETE FROM slap_records WHERE txid = ? AND outputIndex = ?")
            .bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(slap_err)
    }

    async fn find_record(&self, query: &SLAPQuery) -> Result<Vec<UTXOReference>, SLAPStorageError> {
        let mut wb = WhereBuilder::new();

        if let Some(ref d) = query.domain {
            wb = wb.eq("domain", &**d);
        }
        if let Some(ref s) = query.service {
            wb = wb.eq("service", &**s);
        }
        if let Some(ref ik) = query.identity_key {
            wb = wb.eq("identityKey", &**ik);
        }

        let (where_clause, params) = wb.build();

        let order = match query.sort_order {
            Some(SortOrder::Asc) => "ASC",
            _ => "DESC",
        };
        let mut sql = format!(
            "SELECT txid, outputIndex FROM slap_records{where_clause} ORDER BY createdAt {order}"
        );

        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(skip) = query.skip {
            sql.push_str(&format!(" OFFSET {skip}"));
        }

        let mut q = Query::new(sql);
        for p in params {
            q = q.bind(p);
        }

        let rows: Vec<UTXORow> = q.fetch_all(&self.db).await.map_err(slap_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
        sort_order: Option<SortOrder>,
    ) -> Result<Vec<UTXOReference>, SLAPStorageError> {
        self.find_record(&SLAPQuery {
            find_all: Some(true),
            limit,
            skip,
            sort_order,
            ..Default::default()
        })
        .await
    }

    async fn find_all_records(&self) -> Result<Vec<SLAPDiscoveryRecord>, SLAPStorageError> {
        let rows: Vec<SLAPRecordRow> = Query::new(
            "SELECT txid, outputIndex, identityKey, domain, service \
             FROM slap_records ORDER BY createdAt DESC",
        )
        .fetch_all(&self.db)
        .await
        .map_err(slap_err)?;

        Ok(rows
            .into_iter()
            .map(|r| SLAPDiscoveryRecord {
                txid: r.txid,
                output_index: r.output_index as u32,
                identity_key: r.identity_key,
                domain: r.domain,
                service: r.service,
            })
            .collect())
    }
}

// =============================================================================
// D1AgentStorage
// =============================================================================

/// Row for agent discovery record queries (Janitor health checks).
/// D1 column is still `endpoint` for migration compat; mapped to `name` in Rust.
#[derive(Deserialize)]
struct AgentDiscoveryRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "endpoint")]
    name: String,
}

/// Cloudflare D1 implementation of the AgentStorage trait.
pub struct D1AgentStorage {
    db: Rc<D1Database>,
}

impl D1AgentStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn agent_err(e: String) -> AgentStorageError {
    AgentStorageError::Database(e)
}

#[async_trait(?Send)]
impl AgentStorage for D1AgentStorage {
    async fn has_duplicate_record(
        &self,
        identity_key: &str,
        name: &str,
    ) -> Result<bool, AgentStorageError> {
        let row: Option<CountRow> = Query::new(
            "SELECT COUNT(*) as cnt FROM agent_records \
             WHERE identityKey = ? AND endpoint = ?",
        )
        .bind(identity_key)
        .bind(name)
        .fetch_optional(&self.db)
        .await
        .map_err(agent_err)?;

        Ok(row.is_some_and(|r| r.cnt > 0.0))
    }

    async fn store_record(&self, record: &AgentRecord) -> Result<(), AgentStorageError> {
        // Insert main record
        Query::new(
            "INSERT INTO agent_records (txid, outputIndex, identityKey, certifierKey, endpoint) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&*record.txid)
        .bind(record.output_index)
        .bind(&*record.identity_key)
        .bind(&*record.certifier_key)
        .bind(&*record.name)
        .execute(&self.db)
        .await
        .map_err(agent_err)?;

        // Insert one row per capability
        for cap in &record.capabilities {
            Query::new(
                "INSERT INTO agent_capabilities (txid, outputIndex, capability) \
                 VALUES (?, ?, ?)",
            )
            .bind(&*record.txid)
            .bind(record.output_index)
            .bind(&**cap)
            .execute(&self.db)
            .await
            .map_err(agent_err)?;
        }

        Ok(())
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), AgentStorageError> {
        // Delete capabilities first (no FK cascade in D1)
        Query::new("DELETE FROM agent_capabilities WHERE txid = ? AND outputIndex = ?")
            .bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(agent_err)?;

        // Delete the main record
        Query::new("DELETE FROM agent_records WHERE txid = ? AND outputIndex = ?")
            .bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(agent_err)
    }

    async fn find_by_capability(
        &self,
        capability: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let mut sql = "SELECT DISTINCT r.txid, r.outputIndex FROM agent_records r \
                   INNER JOIN agent_capabilities c ON r.txid = c.txid AND r.outputIndex = c.outputIndex \
                   WHERE c.capability = ? \
                   ORDER BY r.createdAt DESC".to_string();
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {l}"));
        }
        if let Some(s) = skip {
            sql.push_str(&format!(" OFFSET {s}"));
        }
        let rows: Vec<UTXORow> = Query::new(sql)
            .bind(capability)
            .fetch_all(&self.db)
            .await
            .map_err(agent_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_by_identity_key(
        &self,
        identity_key: &str,
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let rows: Vec<UTXORow> = Query::new(
            "SELECT txid, outputIndex FROM agent_records WHERE identityKey = ? ORDER BY createdAt DESC",
        )
        .bind(identity_key)
        .fetch_all(&self.db)
        .await
        .map_err(agent_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_by_certifier(
        &self,
        certifier_key: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let mut sql = "SELECT txid, outputIndex FROM agent_records \
             WHERE certifierKey = ? ORDER BY createdAt DESC"
            .to_string();
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {l}"));
        }
        if let Some(s) = skip {
            sql.push_str(&format!(" OFFSET {s}"));
        }
        let rows: Vec<UTXORow> = Query::new(sql)
            .bind(certifier_key)
            .fetch_all(&self.db)
            .await
            .map_err(agent_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_by_name(&self, name: &str) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let rows: Vec<UTXORow> = Query::new(
            "SELECT txid, outputIndex FROM agent_records WHERE endpoint = ? ORDER BY createdAt DESC",
        )
        .bind(name)
        .fetch_all(&self.db)
        .await
        .map_err(agent_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let mut sql =
            "SELECT txid, outputIndex FROM agent_records ORDER BY createdAt DESC".to_string();
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {l}"));
        }
        if let Some(s) = skip {
            sql.push_str(&format!(" OFFSET {s}"));
        }
        let rows: Vec<UTXORow> = Query::new(sql)
            .fetch_all(&self.db)
            .await
            .map_err(agent_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_all_records(&self) -> Result<Vec<AgentDiscoveryRecord>, AgentStorageError> {
        let rows: Vec<AgentDiscoveryRow> = Query::new(
            "SELECT txid, outputIndex, endpoint FROM agent_records ORDER BY createdAt DESC",
        )
        .fetch_all(&self.db)
        .await
        .map_err(agent_err)?;

        Ok(rows
            .into_iter()
            .map(|r| AgentDiscoveryRecord {
                txid: r.txid,
                output_index: r.output_index as u32,
                name: r.name,
            })
            .collect())
    }

    async fn find_existing_by_identity_and_name(
        &self,
        identity_key: &str,
        name: &str,
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let rows: Vec<UTXORow> = Query::new(
            "SELECT txid, outputIndex FROM agent_records \
             WHERE identityKey = ? AND endpoint = ? ORDER BY createdAt DESC",
        )
        .bind(identity_key)
        .bind(name)
        .fetch_all(&self.db)
        .await
        .map_err(agent_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }
}

// =============================================================================
// D1 implementation of DmDelegationStorage trait.
// =============================================================================
//
// Backs `tm_dm_delegation` / `ls_dm_delegation` for dolphin-milk delegation
// revocation cert tracking. Schema lives in `d1::OVERLAY_MIGRATIONS`.

pub struct D1DmDelegationStorage {
    db: Rc<D1Database>,
}

impl D1DmDelegationStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn dm_delegation_err(e: String) -> DmDelegationStorageError {
    DmDelegationStorageError::Database(e)
}

#[async_trait(?Send)]
impl DmDelegationStorage for D1DmDelegationStorage {
    async fn has_duplicate_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<bool, DmDelegationStorageError> {
        let row: Option<CountRow> = Query::new(
            "SELECT COUNT(*) as cnt FROM dm_delegation_records \
             WHERE txid = ? AND outputIndex = ?",
        )
        .bind(txid)
        .bind(output_index)
        .fetch_optional(&self.db)
        .await
        .map_err(dm_delegation_err)?;
        Ok(row.is_some_and(|r| r.cnt > 0.0))
    }

    async fn store_record(
        &self,
        record: &DmDelegationRecord,
    ) -> Result<(), DmDelegationStorageError> {
        Query::new(
            "INSERT INTO dm_delegation_records \
             (txid, outputIndex, serialNumber, certifierKey, subjectKey, expiresAt) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&*record.txid)
        .bind(record.output_index)
        .bind(&*record.serial_number)
        .bind(&*record.certifier_key)
        .bind(&*record.subject_key)
        .bind(&*record.expires_at)
        .execute(&self.db)
        .await
        .map_err(dm_delegation_err)?;
        Ok(())
    }

    async fn delete_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<(), DmDelegationStorageError> {
        Query::new("DELETE FROM dm_delegation_records WHERE txid = ? AND outputIndex = ?")
            .bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(dm_delegation_err)
    }

    async fn find_by_outpoint(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError> {
        let rows: Vec<UTXORow> = Query::new(
            "SELECT txid, outputIndex FROM dm_delegation_records \
             WHERE txid = ? AND outputIndex = ?",
        )
        .bind(txid)
        .bind(output_index)
        .fetch_all(&self.db)
        .await
        .map_err(dm_delegation_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_by_serial(
        &self,
        serial: &str,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError> {
        let rows: Vec<UTXORow> = Query::new(
            "SELECT txid, outputIndex FROM dm_delegation_records \
             WHERE serialNumber = ? ORDER BY createdAt DESC",
        )
        .bind(serial)
        .fetch_all(&self.db)
        .await
        .map_err(dm_delegation_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_by_certifier(
        &self,
        certifier_key: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError> {
        let mut sql = "SELECT txid, outputIndex FROM dm_delegation_records \
             WHERE certifierKey = ? ORDER BY createdAt DESC"
            .to_string();
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {l}"));
        }
        if let Some(s) = skip {
            sql.push_str(&format!(" OFFSET {s}"));
        }
        let rows: Vec<UTXORow> = Query::new(sql)
            .bind(certifier_key)
            .fetch_all(&self.db)
            .await
            .map_err(dm_delegation_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError> {
        let mut sql = "SELECT txid, outputIndex FROM dm_delegation_records ORDER BY createdAt DESC"
            .to_string();
        if let Some(l) = limit {
            sql.push_str(&format!(" LIMIT {l}"));
        }
        if let Some(s) = skip {
            sql.push_str(&format!(" OFFSET {s}"));
        }
        let rows: Vec<UTXORow> = Query::new(sql)
            .fetch_all(&self.db)
            .await
            .map_err(dm_delegation_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }
}

// =============================================================================
// D1UHRPStorage
// =============================================================================

/// Row for UHRP UTXO reference queries with full metadata.
#[derive(Deserialize)]
struct UHRPRecordRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "uhrpUrl")]
    uhrp_url: String,
    #[serde(rename = "identityKey")]
    identity_key: String,
    #[serde(rename = "downloadUrl")]
    download_url: String,
    #[serde(rename = "expiryTime")]
    expiry_time: f64,
    #[serde(rename = "contentLength")]
    content_length: f64,
}

/// Cloudflare D1 implementation of the UHRPStorage trait.
pub struct D1UHRPStorage {
    db: Rc<D1Database>,
}

impl D1UHRPStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn uhrp_err(e: String) -> UHRPStorageError {
    UHRPStorageError::Database(e)
}

#[async_trait(?Send)]
impl UHRPStorage for D1UHRPStorage {
    async fn has_duplicate_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<bool, UHRPStorageError> {
        let row: Option<CountRow> = Query::new(
            "SELECT COUNT(*) as cnt FROM uhrp_records \
             WHERE txid = ? AND outputIndex = ?",
        )
        .bind(txid)
        .bind(output_index)
        .fetch_optional(&self.db)
        .await
        .map_err(uhrp_err)?;

        Ok(row.is_some_and(|r| r.cnt > 0.0))
    }

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
        Query::new(
            "INSERT INTO uhrp_records (txid, outputIndex, uhrpUrl, identityKey, downloadUrl, expiryTime, contentLength) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(txid)
        .bind(output_index)
        .bind(uhrp_url)
        .bind(identity_key)
        .bind(download_url)
        .bind(expiry_time)
        .bind(content_length)
        .execute(&self.db)
        .await
        .map_err(uhrp_err)
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), UHRPStorageError> {
        Query::new("DELETE FROM uhrp_records WHERE txid = ? AND outputIndex = ?")
            .bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(uhrp_err)
    }

    async fn find_record(&self, query: &UHRPQuery) -> Result<Vec<UTXOReference>, UHRPStorageError> {
        let mut wb = WhereBuilder::new();
        // Legacy-storage fallback: pre-2026-04-22 admissions indexed
        // `uhrpUrl` as hex-of-hash; post-fix admissions store canonical
        // `uhrp://<base58check>`. Accept either stored form when the
        // caller queries in canonical form — decode the hash, then
        // match the stored column against both representations.
        let hex_fallback = query.uhrp_url.as_deref().and_then(|u| {
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
        if let (Some(u), Some(hex_u)) = (query.uhrp_url.as_ref(), hex_fallback.as_ref()) {
            // OR clause: stored matches either canonical or hex form.
            wb = wb.raw(
                "(uhrpUrl = ? OR uhrpUrl = ?)",
                vec![u.as_str().into(), hex_u.as_str().into()],
            );
        } else if let Some(u) = query.uhrp_url.as_ref() {
            wb = wb.eq("uhrpUrl", u.as_str());
        }
        if let Some(ref ik) = query.identity_key {
            wb = wb.eq("identityKey", &**ik);
        }

        let (mut where_clause, mut params) = wb.build();

        // Opt-in expiry filter. `include_expired = Some(true)` short-circuits
        // (historians / audit consumers). Otherwise we hide records whose
        // `expiry_time` is in the past vs `query.now_unix_seconds` (or our
        // own clock if unset). `expiry_time = 0` is "never expires" and is
        // always visible — matches the UHRP convention where a missing/zero
        // expiry means permanent. `WhereBuilder` has no `OR`, so we
        // hand-append the clause.
        if !query.include_expired.unwrap_or(false) {
            let now = query
                .now_unix_seconds
                .unwrap_or_else(current_unix_seconds_i64);
            let clause = "(expiryTime = 0 OR expiryTime >= ?)";
            where_clause = if where_clause.is_empty() {
                format!(" WHERE {clause}")
            } else {
                format!("{where_clause} AND {clause}")
            };
            params.push(now.into());
        }

        let order = match query.sort_order {
            Some(UHRPSortOrder::Asc) => "ASC",
            _ => "DESC",
        };
        let mut sql = format!(
            "SELECT txid, outputIndex FROM uhrp_records{where_clause} ORDER BY createdAt {order}"
        );
        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(skip) = query.skip {
            sql.push_str(&format!(" OFFSET {skip}"));
        }

        let mut q = Query::new(sql);
        for p in params {
            q = q.bind(p);
        }
        let rows: Vec<UTXORow> = q.fetch_all(&self.db).await.map_err(uhrp_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
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
        let rows: Vec<UHRPRecordRow> = Query::new(
            "SELECT txid, outputIndex, uhrpUrl, identityKey, downloadUrl, expiryTime, contentLength \
             FROM uhrp_records ORDER BY createdAt DESC",
        )
        .fetch_all(&self.db)
        .await
        .map_err(uhrp_err)?;

        Ok(rows
            .into_iter()
            .map(|r| UHRPDiscoveryRecord {
                txid: r.txid,
                output_index: r.output_index as u32,
                uhrp_url: r.uhrp_url,
                identity_key: r.identity_key,
                download_url: r.download_url,
                expiry_time: r.expiry_time as i64,
                content_length: r.content_length as i64,
            })
            .collect())
    }
}

// =============================================================================
// D1LowStorage
// =============================================================================

/// Cloudflare D1 implementation of the LowStorage trait (tm_low / ls_low).
///
/// Schema: `low_records` in `d1::OVERLAY_MIGRATIONS`. Keyed by
/// (txid, outputIndex); `INSERT OR REPLACE` keeps re-admission idempotent.
pub struct D1LowStorage {
    db: Rc<D1Database>,
}

impl D1LowStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn low_err(e: String) -> LowStorageError {
    LowStorageError::Database(e)
}

#[async_trait(?Send)]
impl LowStorage for D1LowStorage {
    async fn store_record(&self, record: &LowRecord) -> Result<(), LowStorageError> {
        Query::new(
            "INSERT OR REPLACE INTO low_records \
             (recordType, txid, outputIndex, hostIdentity, gameId, stakeSats, rulesHash, relayUrl, expiryHeight) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.record_type.as_str())
        .bind(record.txid.as_str())
        .bind(record.output_index)
        .bind(record.host_identity.as_str())
        .bind(record.game_id.as_str())
        // u64 → i64: poker stakes fit comfortably; D1 INTEGER is i64.
        .bind(record.stake_sats.map(|s| s as i64))
        .bind(record.rules_hash.as_deref())
        .bind(record.relay_url.as_deref())
        .bind(record.expiry_height.map(|h| h as i64))
        .execute(&self.db)
        .await
        .map_err(low_err)
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), LowStorageError> {
        Query::new("DELETE FROM low_records WHERE txid = ? AND outputIndex = ?")
            .bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(low_err)
    }

    async fn find_open_tables(
        &self,
        stake_min: Option<u64>,
        stake_max: Option<u64>,
        tip_height: Option<u32>,
    ) -> Result<Vec<UTXOReference>, LowStorageError> {
        let mut wb = WhereBuilder::new().eq("recordType", "table");
        if let Some(min) = stake_min {
            wb = wb.gte("stakeSats", min as i64);
        }
        if let Some(max) = stake_max {
            wb = wb.raw("stakeSats <= ?", vec![(max as i64).into()]);
        }
        // Query-time expiry enforcement (bsv-low #148): the overlay has no
        // passive spend watcher, so an expired-but-unspent TABLE_OPEN would
        // linger forever. When the tip is known, drop rows with
        // `expiryHeight <= tip`. STRICTLY greater, mirroring the client
        // (`expiryHeight > tip` at Lobby.tsx) so server and client agree. A
        // NULL expiryHeight fails `NULL > ?` and is dropped — same as the
        // in-memory impl. `None` tip => no clause (fail-open, lobby stays up).
        if let Some(tip) = tip_height {
            wb = wb.raw("expiryHeight > ?", vec![(tip as i64).into()]);
        }
        let (where_clause, params) = wb.build();

        let sql = format!(
            "SELECT txid, outputIndex FROM low_records{where_clause} ORDER BY createdAt DESC"
        );
        let mut q = Query::new(sql);
        for p in params {
            q = q.bind(p);
        }
        let rows: Vec<UTXORow> = q.fetch_all(&self.db).await.map_err(low_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_by_game_id(&self, game_id: &str) -> Result<Vec<UTXOReference>, LowStorageError> {
        let rows: Vec<UTXORow> = Query::new(
            "SELECT txid, outputIndex FROM low_records WHERE gameId = ? ORDER BY createdAt DESC",
        )
        .bind(game_id)
        .fetch_all(&self.db)
        .await
        .map_err(low_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_by_host(
        &self,
        identity_key: &str,
    ) -> Result<Vec<UTXOReference>, LowStorageError> {
        let rows: Vec<UTXORow> = Query::new(
            "SELECT txid, outputIndex FROM low_records WHERE hostIdentity = ? \
             ORDER BY createdAt DESC",
        )
        .bind(identity_key)
        .fetch_all(&self.db)
        .await
        .map_err(low_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }
}

// =============================================================================
// D1RevealStorage
// =============================================================================

/// Cloudflare D1 implementation of the RevealStorage trait (tm_reveal /
/// ls_reveal).
///
/// Schema: `reveal_records` in `d1::OVERLAY_MIGRATIONS`. Keyed by
/// (txid, outputIndex); `INSERT OR REPLACE` keeps re-admission idempotent.
/// Rows are NEVER deleted on spend/eviction — a reveal is a permanent fact
/// (the lookup service's spend/eviction hooks are no-ops). `delete_record`
/// exists for API symmetry / manual operator use only.
pub struct D1RevealStorage {
    db: Rc<D1Database>,
}

impl D1RevealStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn reveal_err(e: String) -> RevealStorageError {
    RevealStorageError::Database(e)
}

#[async_trait(?Send)]
impl RevealStorage for D1RevealStorage {
    async fn store_record(&self, record: &RevealRecord) -> Result<(), RevealStorageError> {
        Query::new(
            "INSERT OR REPLACE INTO reveal_records \
             (txid, outputIndex, gameId, seat) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(record.txid.as_str())
        .bind(record.output_index)
        .bind(record.game_id.as_str())
        .bind(record.seat as u32)
        .execute(&self.db)
        .await
        .map_err(reveal_err)
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), RevealStorageError> {
        Query::new("DELETE FROM reveal_records WHERE txid = ? AND outputIndex = ?")
            .bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(reveal_err)
    }

    async fn find_by_game_seat(
        &self,
        game_id: &str,
        seat: u8,
    ) -> Result<Vec<UTXOReference>, RevealStorageError> {
        let rows: Vec<UTXORow> = Query::new(
            "SELECT txid, outputIndex FROM reveal_records \
             WHERE gameId = ? AND seat = ? ORDER BY createdAt DESC",
        )
        .bind(game_id)
        .bind(seat as u32)
        .fetch_all(&self.db)
        .await
        .map_err(reveal_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }

    async fn find_by_game_id(
        &self,
        game_id: &str,
    ) -> Result<Vec<UTXOReference>, RevealStorageError> {
        let rows: Vec<UTXORow> = Query::new(
            "SELECT txid, outputIndex FROM reveal_records WHERE gameId = ? \
             ORDER BY createdAt DESC",
        )
        .bind(game_id)
        .fetch_all(&self.db)
        .await
        .map_err(reveal_err)?;
        Ok(rows.into_iter().map(UTXORow::into_ref).collect())
    }
}

// =============================================================================
// D1PotStorage
// =============================================================================

/// Row for pot-spend record queries. D1 returns numbers as f64 and a
/// nullable TEXT column as `Option<String>`.
#[derive(Deserialize)]
struct PotRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    spent: f64,
    #[serde(rename = "spendingTxid")]
    spending_txid: Option<String>,
    /// `serde(default)` (0.0) tolerates a read that races the additive
    /// `spentConfirmed` migration.
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: f64,
}

impl PotRow {
    fn into_record(self) -> PotRecord {
        PotRecord {
            txid: self.txid,
            output_index: self.output_index as u32,
            spent: self.spent != 0.0,
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed != 0.0,
        }
    }
}

/// Row for the `pot_beefs` length probe (`length(beef) AS len`). D1 returns
/// numbers as f64.
#[derive(Deserialize)]
struct BeefLenRow {
    len: f64,
}

/// Row for the `pot_beefs` read-back: the BLOB as hex (`hex(beef) AS beef`) —
/// the same read-back idiom the engine (`d1_storage.rs` `hex(t.beef)`) and
/// low-app-layer use, avoiding D1 BLOB→JS deserialization quirks. `hex(NULL)`
/// is NULL, so the column arrives `Option`.
#[derive(Deserialize)]
struct BeefHexRow {
    beef: Option<String>,
}

/// Row for the `pot_beefs` proof-completion candidate scan: the stored tx's
/// own txid + its BEEF as hex (`hex(beef) AS beef`).
#[derive(Deserialize)]
struct PotBeefProofRow {
    txid: String,
    beef: Option<String>,
}

/// Decode a `hex(beef)` read-back (SQLite `hex()` emits UPPERCASE;
/// `hex::decode` accepts either case). Empty/undecodable → `None` — an
/// unusable row is never served as bytes.
fn decode_pot_beef_hex(row_beef: Option<String>) -> Option<Vec<u8>> {
    let bytes = hex::decode(row_beef?).ok()?;
    if bytes.is_empty() {
        None
    } else {
        Some(bytes)
    }
}

/// The `store_beef` write gate — longer-wins, never-clobber (the "vanishing
/// table" lesson, see `d1_storage.rs::insert_output`): write only when the
/// incoming beef is non-empty AND (no row exists OR the incoming beef is
/// strictly LONGER than the stored one).
fn beef_write_allowed(existing_len: Option<usize>, new_len: usize) -> bool {
    new_len > 0 && existing_len.is_none_or(|len| new_len > len)
}

/// Cloudflare D1 implementation of the PotStorage trait (tm_pot / ls_pot).
///
/// Schema: `pot_records` + `pot_beefs` in `d1::OVERLAY_MIGRATIONS`.
/// `pot_records` is keyed by (txid, outputIndex) = the pot funding outpoint.
/// `store_record` is `INSERT OR IGNORE` so a re-admission never clobbers a
/// spent row back to unspent; `mark_spent` is an `UPDATE` with
/// prefer-confirmed / never-clobber-with-unconfirmed semantics
/// ([`mark_spent_sql`]); neither ever DELETEs — a spent pot is the permanent
/// landing proof. `pot_beefs` (keyed
/// by the stored tx's own txid) durably holds the funding AND spending
/// (settle/refund) BEEFs; `store_beef` writes only when absent-or-longer
/// ([`beef_write_allowed`]) and nothing ever deletes a row.
pub struct D1PotStorage {
    db: Rc<D1Database>,
}

impl D1PotStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn pot_err(e: String) -> PotStorageError {
    PotStorageError::Database(e)
}

/// The `mark_spent` UPDATE, by confirmation (prefer-confirmed /
/// never-clobber-with-unconfirmed — see the `PotStorage::mark_spent` trait
/// doc). Both are UPDATE-only (nonexistent outpoint = 0 rows touched) and
/// never DELETE:
///
/// - confirmed: always writes and latches `spentConfirmed = 1`
///   (last-confirmed-wins).
/// - unconfirmed: the `AND spentConfirmed = 0` guard makes an unconfirmed
///   claim a no-op against a confirmed pointer, while preserving
///   last-writer-wins among unconfirmed claims; `spentConfirmed` untouched.
///
/// Both branches stamp `spentAt = unixepoch()` (#228 backstop age anchor):
/// every ACCEPTED spend write resets the age, so the poll chaser's gate
/// measures from the CURRENT spend pointer (its push gets its chance first).
/// A refused unconfirmed-vs-confirmed write touches nothing (WHERE misses).
fn mark_spent_sql(confirmed: bool) -> &'static str {
    if confirmed {
        "UPDATE pot_records SET spent = 1, spendingTxid = ?, spentConfirmed = 1, \
             spentAt = unixepoch() \
         WHERE txid = ? AND outputIndex = ?"
    } else {
        "UPDATE pot_records SET spent = 1, spendingTxid = ?, spentAt = unixepoch() \
         WHERE txid = ? AND outputIndex = ? AND spentConfirmed = 0"
    }
}

#[async_trait(?Send)]
impl PotStorage for D1PotStorage {
    async fn store_record(&self, record: &PotRecord) -> Result<(), PotStorageError> {
        // INSERT OR IGNORE: insert-if-absent, never clobber a spent row.
        Query::new(
            "INSERT OR IGNORE INTO pot_records \
             (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(record.txid.as_str())
        .bind(record.output_index)
        .bind(if record.spent { 1u32 } else { 0u32 })
        .bind(record.spending_txid.as_deref())
        .bind(if record.spent_confirmed { 1u32 } else { 0u32 })
        .bind(current_unix_seconds_i64())
        .execute(&self.db)
        .await
        .map_err(pot_err)
    }

    async fn mark_spent(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        confirmed: bool,
    ) -> Result<(), PotStorageError> {
        // UPDATE-only: records the spender on an existing row (a nonexistent
        // outpoint is a no-op — an output must be admitted before it spends).
        //
        // Prefer-confirmed / never-clobber-with-unconfirmed (trait doc):
        // - confirmed → ALWAYS write + latch spentConfirmed = 1
        //   (chain truth; last-confirmed-wins).
        // - unconfirmed → write ONLY IF spentConfirmed = 0 (an unconfirmed
        //   claim never clobbers a confirmed pointer; last-writer-wins among
        //   unconfirmed claims is preserved); spentConfirmed untouched.
        Query::new(mark_spent_sql(confirmed))
            .bind(spending_txid)
            .bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(pot_err)
    }

    async fn get_spent_status(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Option<PotRecord>, PotStorageError> {
        let row: Option<PotRow> = Query::new(
            "SELECT txid, outputIndex, spent, spendingTxid, spentConfirmed FROM pot_records \
             WHERE txid = ? AND outputIndex = ?",
        )
        .bind(txid)
        .bind(output_index)
        .fetch_optional(&self.db)
        .await
        .map_err(pot_err)?;
        Ok(row.map(PotRow::into_record))
    }

    async fn find_spent_unconfirmed(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        // Spent-but-unconfirmed pot rows (#186), RANDOM-sampled so a
        // never-mineable head cannot starve the tail — the same anti-starvation
        // shape as find_pot_beefs_for_proof_check. `limit`/`min_age_secs` are
        // u64s (not user input), interpolated to match that sibling's idiom.
        // The (spent, spentConfirmed) composite index backs the scan.
        //
        // Push-primary backstop age gate (#228), anchored on spentAt (the
        // CURRENT spend pointer's record time): a young spend's proof is
        // expected via /arc-ingest. NULL spentAt (pre-migration) = eligible.
        let sql = format!(
            "SELECT txid, outputIndex, spent, spendingTxid, spentConfirmed FROM pot_records \
             WHERE spent = 1 AND spentConfirmed = 0 \
               AND (spentAt IS NULL OR spentAt <= unixepoch() - {min_age_secs}) \
             ORDER BY RANDOM() LIMIT {limit}"
        );
        let rows: Vec<PotRow> = Query::new(sql).fetch_all(&self.db).await.map_err(pot_err)?;
        Ok(rows.into_iter().map(PotRow::into_record).collect())
    }

    async fn find_unconfirmed_by_spending_txid(
        &self,
        spending_txid: &str,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        // The PUSH consumer's lookup (#228): every pot outpoint whose recorded
        // spender is this txid and whose spend is still unconfirmed — the rows
        // /arc-ingest upgrades (mark_spent confirmed) once the spending tx's
        // pushed bump chaintracks-verifies. Backed by idx_pot_spending.
        let rows: Vec<PotRow> = Query::new(
            "SELECT txid, outputIndex, spent, spendingTxid, spentConfirmed FROM pot_records \
             WHERE spendingTxid = ? AND spent = 1 AND spentConfirmed = 0",
        )
        .bind(spending_txid)
        .fetch_all(&self.db)
        .await
        .map_err(pot_err)?;
        Ok(rows.into_iter().map(PotRow::into_record).collect())
    }

    async fn store_beef(&self, txid: &str, beef: &[u8]) -> Result<(), PotStorageError> {
        // Probe the existing row's length first; write only when absent or
        // strictly longer ([`beef_write_allowed`] — never clobber a good row
        // with a shorter/empty one).
        let existing: Option<BeefLenRow> =
            Query::new("SELECT length(beef) AS len FROM pot_beefs WHERE txid = ?")
                .bind(txid)
                .fetch_optional(&self.db)
                .await
                .map_err(pot_err)?;
        if !beef_write_allowed(existing.map(|r| r.len as usize), beef.len()) {
            return Ok(());
        }

        // OR REPLACE + BLOB bind — the same idiom as the engine's
        // transactions upsert (`d1_storage.rs::insert_output`): the guard
        // above means we only ever replace with a strictly longer beef.
        // has_proof (#192/#193) records whether this beef already carries a
        // BUMP for its own txid, so the completion cron enumerates only
        // proofless rows.
        let has_proof = i64::from(pot_beef_has_proof(txid, beef));
        // createdAt is preserve-or-stamp (#228 backstop age anchor): a
        // longer-beef rewrite keeps the original first-store time so the
        // push-primary backstop's age gate measures real age.
        Query::new(
            "INSERT OR REPLACE INTO pot_beefs (txid, beef, createdAt, has_proof) \
             VALUES (?, ?, COALESCE((SELECT createdAt FROM pot_beefs WHERE txid = ?), ?), ?)",
        )
        .bind(txid)
        .bind(beef)
        .bind(txid)
        .bind(current_unix_seconds_i64())
        .bind(has_proof)
        .execute(&self.db)
        .await
        .map_err(pot_err)
    }

    async fn get_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, PotStorageError> {
        let row: Option<BeefHexRow> =
            Query::new("SELECT hex(beef) AS beef FROM pot_beefs WHERE txid = ?")
                .bind(txid)
                .fetch_optional(&self.db)
                .await
                .map_err(pot_err)?;
        Ok(row.and_then(|r| decode_pot_beef_hex(r.beef)))
    }

    async fn find_pot_beefs_for_proof_check(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<(String, Vec<u8>)>, PotStorageError> {
        // ONLY proofless rows (#192/#193), RANDOM-sampled so a never-mineable
        // head cannot starve the tail (zanaadu prod incident). Reaches the whole
        // historical backlog (rows written before the has_proof column default
        // to 0). Bytes are read back as hex (the pot_beefs idiom).
        //
        // Push-primary backstop age gate (#228): young rows wait for their
        // /arc-ingest push; NULL createdAt (pre-migration) = eligible.
        let sql = format!(
            "SELECT txid, hex(beef) AS beef FROM pot_beefs \
             WHERE has_proof = 0 \
               AND (createdAt IS NULL OR createdAt <= unixepoch() - {min_age_secs}) \
             ORDER BY RANDOM() LIMIT {limit}"
        );
        let rows: Vec<PotBeefProofRow> =
            Query::new(sql).fetch_all(&self.db).await.map_err(pot_err)?;
        Ok(rows
            .into_iter()
            .filter_map(|r| decode_pot_beef_hex(r.beef).map(|beef| (r.txid, beef)))
            .collect())
    }

    async fn compact_pot_beef(&self, txid: &str, new_beef: &[u8]) -> Result<(), PotStorageError> {
        // Fail-closed: overwrite ONLY when the new beef actually proves txid
        // (its own BUMP present ⇒ self-contained). This BYPASSES the longer-wins
        // `beef_write_allowed` guard — a bumped BEEF is authoritative even when
        // SHORTER (its proven ancestry has been trimmed away). has_proof is
        // latched to 1 so the row drops out of the completion candidate set.
        if !pot_beef_has_proof(txid, new_beef) {
            return Ok(());
        }
        Query::new(
            "INSERT OR REPLACE INTO pot_beefs (txid, beef, createdAt, has_proof) \
             VALUES (?, ?, ?, 1)",
        )
        .bind(txid)
        .bind(new_beef)
        .bind(current_unix_seconds_i64())
        .execute(&self.db)
        .await
        .map_err(pot_err)
    }
}

// =============================================================================
// D1CollectedStorage
// =============================================================================

/// Row for collected-marker queries. All columns are TEXT; `txid` /
/// `sigHex` are nullable in the schema so they arrive `Option`.
#[derive(Deserialize)]
struct CollectedRow {
    identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    txid: Option<String>,
    #[serde(rename = "sigHex")]
    sig_hex: Option<String>,
}

impl CollectedRow {
    fn into_record(self) -> CollectedRecord {
        CollectedRecord {
            identity: self.identity,
            game_id: self.game_id,
            txid: self.txid,
            sig_hex: self.sig_hex,
        }
    }
}

/// Cloudflare D1 implementation of the CollectedStorage trait
/// (tm_collected / ls_collected, bsv-low #161).
///
/// Schema: `collected_markers` in `d1::OVERLAY_MIGRATIONS`. Keyed by
/// (identity, gameId); `INSERT OR IGNORE` makes the FIRST marker for a
/// pair win — a later marker never overwrites it — and rows are NEVER
/// deleted (a collected fact is permanent, like a reveal; the lookup
/// service's spend/eviction hooks are no-ops).
pub struct D1CollectedStorage {
    db: Rc<D1Database>,
}

impl D1CollectedStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn collected_err(e: String) -> CollectedStorageError {
    CollectedStorageError::Database(e)
}

#[async_trait(?Send)]
impl CollectedStorage for D1CollectedStorage {
    async fn store_record(&self, record: &CollectedRecord) -> Result<(), CollectedStorageError> {
        // INSERT OR IGNORE on the (identity, gameId) primary key — first
        // marker wins; never overwrite, never delete.
        Query::new(
            "INSERT OR IGNORE INTO collected_markers \
             (identity, gameId, txid, sigHex, createdAt) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(record.identity.as_str())
        .bind(record.game_id.as_str())
        .bind(record.txid.as_deref())
        .bind(record.sig_hex.as_deref())
        .bind(current_unix_seconds_i64())
        .execute(&self.db)
        .await
        .map_err(collected_err)
    }

    async fn get_record(
        &self,
        identity: &str,
        game_id: &str,
    ) -> Result<Option<CollectedRecord>, CollectedStorageError> {
        let row: Option<CollectedRow> = Query::new(
            "SELECT identity, gameId, txid, sigHex FROM collected_markers \
             WHERE identity = ? AND gameId = ?",
        )
        .bind(identity)
        .bind(game_id)
        .fetch_optional(&self.db)
        .await
        .map_err(collected_err)?;
        Ok(row.map(CollectedRow::into_record))
    }
}

// =============================================================================
// D1ResultStorage
// =============================================================================

/// Row for result-marker queries. TEXT columns arrive as `String` /
/// `Option<String>` (loserSigHex is nullable — NULL = an unconfirmed
/// claim); `outputIndex` / `createdAt` are INTEGER columns but D1
/// returns numbers as f64.
#[derive(Deserialize)]
struct ResultRow {
    #[serde(rename = "gameId")]
    game_id: String,
    winner: String,
    loser: String,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "settleTxid")]
    settle_txid: String,
    #[serde(rename = "winnerSigHex")]
    winner_sig_hex: String,
    #[serde(rename = "loserSigHex")]
    loser_sig_hex: Option<String>,
    #[serde(rename = "cardsHex")]
    cards_hex: Option<String>,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "createdAt")]
    created_at: Option<f64>,
}

impl ResultRow {
    fn into_record(self) -> ResultRecord {
        ResultRecord {
            game_id: self.game_id,
            winner: self.winner,
            loser: self.loser,
            pot_txid: self.pot_txid,
            settle_txid: self.settle_txid,
            winner_sig_hex: self.winner_sig_hex,
            loser_sig_hex: self.loser_sig_hex,
            cards_hex: self.cards_hex,
            txid: self.txid,
            output_index: self.output_index as u32,
            created_at: self.created_at.unwrap_or(0.0) as i64,
        }
    }
}

/// Cloudflare D1 implementation of the ResultStorage trait
/// (tm_result / ls_result, bsv-low #38).
///
/// Schema: `result_markers_v2` in `d1::OVERLAY_MIGRATIONS`. Keyed by the
/// marker OUTPOINT (txid, outputIndex); `INSERT OR IGNORE` makes a
/// replayed submit of the same output a no-op, while markers for the
/// same (gameId, winner) from DIFFERENT txs are ALL kept (the
/// censorship-front-run fix — a garbage-sig marker can never occupy a
/// pair slot and hide the genuine one; clients verify sigs and count the
/// genuine row). Rows are NEVER deleted (a settled result is permanent,
/// like a reveal; the lookup service's spend/eviction hooks are no-ops).
/// `createdAt` is stamped here at insert (the record's value is ignored)
/// and drives the newest-first list ordering; `rowid DESC` breaks
/// same-second ties in insertion order.
pub struct D1ResultStorage {
    db: Rc<D1Database>,
}

impl D1ResultStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn result_err(e: String) -> ResultStorageError {
    ResultStorageError::Database(e)
}

const RESULT_SELECT: &str = "SELECT gameId, winner, loser, potTxid, settleTxid, \
     winnerSigHex, loserSigHex, cardsHex, txid, outputIndex, createdAt FROM result_markers_v2";

#[async_trait(?Send)]
impl ResultStorage for D1ResultStorage {
    async fn store_record(&self, record: &ResultRecord) -> Result<(), ResultStorageError> {
        // INSERT OR IGNORE on the (txid, outputIndex) primary key — a
        // replayed submit of the same output is a no-op; markers for the
        // same (gameId, winner) from different txs are ALL kept; never
        // overwrite, never delete.
        Query::new(
            "INSERT OR IGNORE INTO result_markers_v2 \
             (gameId, winner, loser, potTxid, settleTxid, winnerSigHex, \
              loserSigHex, cardsHex, txid, outputIndex, createdAt) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.game_id.as_str())
        .bind(record.winner.as_str())
        .bind(record.loser.as_str())
        .bind(record.pot_txid.as_str())
        .bind(record.settle_txid.as_str())
        .bind(record.winner_sig_hex.as_str())
        .bind(record.loser_sig_hex.as_deref())
        .bind(record.cards_hex.as_deref())
        .bind(record.txid.as_str())
        .bind(record.output_index)
        .bind(current_unix_seconds_i64())
        .execute(&self.db)
        .await
        .map_err(result_err)
    }

    async fn list_for_winner(
        &self,
        winner: &str,
        limit: usize,
    ) -> Result<Vec<ResultRecord>, ResultStorageError> {
        let rows: Vec<ResultRow> = Query::new(format!(
            "{RESULT_SELECT} WHERE winner = ? \
             ORDER BY createdAt DESC, rowid DESC LIMIT ?"
        ))
        .bind(winner)
        .bind(limit as u32)
        .fetch_all(&self.db)
        .await
        .map_err(result_err)?;
        Ok(rows.into_iter().map(ResultRow::into_record).collect())
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<ResultRecord>, ResultStorageError> {
        let rows: Vec<ResultRow> = Query::new(format!(
            "{RESULT_SELECT} ORDER BY createdAt DESC, rowid DESC LIMIT ?"
        ))
        .bind(limit as u32)
        .fetch_all(&self.db)
        .await
        .map_err(result_err)?;
        Ok(rows.into_iter().map(ResultRow::into_record).collect())
    }
}

// =============================================================================
// D1PotpartyStorage
// =============================================================================

/// Row for potparty-marker queries. TEXT columns arrive as `String`;
/// `potVout` / `recoveryHeight` / `outputIndex` / `createdAt` are INTEGER
/// columns but D1 returns numbers as f64.
#[derive(Deserialize)]
struct PotpartyRow {
    identity: String,
    #[serde(rename = "opponentIdentity")]
    opponent_identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "potVout")]
    pot_vout: f64,
    #[serde(rename = "recoveryHeight")]
    recovery_height: f64,
    #[serde(rename = "sigHex")]
    sig_hex: Option<String>,
    /// v2 (#230) seat-binding fields — NULL for v1 rows (and for rows
    /// admitted before the additive migration).
    #[serde(rename = "seatSettlePubkey", default)]
    seat_settle_pubkey: Option<String>,
    #[serde(rename = "seatSigHex", default)]
    seat_sig_hex: Option<String>,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "createdAt")]
    created_at: Option<f64>,
}

impl PotpartyRow {
    fn into_record(self) -> PotpartyRecord {
        PotpartyRecord {
            identity: self.identity,
            opponent_identity: self.opponent_identity,
            game_id: self.game_id,
            pot_txid: self.pot_txid,
            pot_vout: self.pot_vout as u32,
            recovery_height: self.recovery_height as u32,
            // The column is nullable in the schema but the admit path always
            // writes it; an impossible NULL reads back as "".
            sig_hex: self.sig_hex.unwrap_or_default(),
            seat_settle_pubkey: self.seat_settle_pubkey,
            seat_sig_hex: self.seat_sig_hex,
            txid: self.txid,
            output_index: self.output_index as u32,
            created_at: self.created_at.unwrap_or(0.0) as i64,
        }
    }
}

/// Cloudflare D1 implementation of the PotpartyStorage trait
/// (tm_potparty / ls_potparty, bsv-low #188).
///
/// Schema: `potparty_records` in `d1::OVERLAY_MIGRATIONS`. Keyed by the
/// marker OUTPOINT (txid, outputIndex); `INSERT OR IGNORE` makes a replayed
/// submit of the same output a no-op, while markers for the same identity
/// from DIFFERENT txs are ALL kept (the censorship-front-run fix). Rows are
/// NEVER deleted (a pot-participation fact is permanent recovery history,
/// like a pot record; the lookup service's spend/eviction hooks are
/// no-ops). `createdAt` is stamped here at insert (the record's value is
/// ignored) and drives the newest-first list ordering; `rowid DESC` breaks
/// same-second ties in insertion order.
pub struct D1PotpartyStorage {
    db: Rc<D1Database>,
}

impl D1PotpartyStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn potparty_err(e: String) -> PotpartyStorageError {
    PotpartyStorageError::Database(e)
}

const POTPARTY_SELECT: &str = "SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
     recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt \
     FROM potparty_records";

/// `ls_potparty partyFor` — the identity-scoped RECOVERY-DISCOVERY window
/// ("which pots am I a party to?", i.e. which pots may owe me money).
///
/// # Why this is not `WHERE identity = ? ORDER BY createdAt DESC LIMIT ?`
///
/// (bsv-low #281.) `tm_potparty` admission is BYTE-FORMAT-ONLY by doctrine —
/// the overlay is an INDEX, not an authority, and never verifies the marker's
/// `sig`. ANYONE can therefore file a marker naming ANY identity for one dust
/// `OP_RETURN`, and under a flat newest-first window `limit` junk rows pushed
/// the victim's REAL pots out of the answer. Because this is the surface a
/// seed-only client uses to FIND the pots it must drive a refund/settle exit
/// on, that displacement is a MONEY path. The cheapest variant needs no
/// forgery at all: re-broadcast the victim's OWN on-chain marker bytes — a
/// different tx is a different outpoint, hence a different row.
///
/// # TWO rows per pot, not one — the client depends on it
///
/// `limit` counts POTS (`potRank`), and each pot yields AT MOST TWO rows: the
/// oldest v1 marker and the oldest v2 (seat-binding) marker, partitioned by
/// `(potTxid, potVout, has-seat-key)`. Collapsing to ONE row per pot is
/// WRONG here, in two different ways that both cost the honest player money:
///
///  - **Dropping the v2 columns is a permanent fee leak.** `decideV2Step`
///    (`potPartyPending.ts`) only returns `'done'` when an indexed row for
///    (gameId, potTxid, identity) carries `seatSettlePubkey`. Omit the
///    column — or return only the pot's v1 row — and `v2Indexed` NEVER
///    latches, so `workV2Half` publishes a real `createAction` `OP_RETURN`
///    on EVERY sweep, forever: the honest player pays sats indefinitely and
///    manufactures precisely the dust rows this window exists to bound.
///  - **Returning only the v2 row can erase the pot entirely.**
///    `lookupPotParty` (`overlay.ts`) VERIFIES both v2 signatures client-side
///    and DROPS a row that fails — relying, in its own words, on "the pot's
///    v1 sibling row" for discovery. Hand it only a forged v2 row and the pot
///    vanishes from `creditSweep`'s recovery list. The v1 sibling must
///    always be there.
///
/// # The bounds, all deterministic
///
///  1. **Per-POT-OUTPOINT collapse** (within each of the v1 / v2 groups) —
///     the window counts POTS, not rows, so one pot can never consume it and
///     the zero-forgery replay variant dies outright. The representative is
///     the OLDEST marker of its group; partitioning on the full OUTPOINT
///     (not just the txid) matches every other key in the system.
///  2. **Existence tier with a RESERVED QUOTA** — a row whose pot outpoint is
///     absent from `pot_records` normally sorts behind every row whose pot
///     exists, so markers naming INVENTED pots (free, unlimited, each its own
///     partition) cannot displace a real one. A STRICT tier, though, silently
///     becomes a FILTER once `limit` binds — a genuinely fresh pot whose
///     `tm_pot` admission is still in flight would fall off the page, which
///     is exactly the pot a recovering client most needs. So the newest
///     `quota` unknown pots are PROMOTED into the main tier and compete on
///     recency; the rest stay demoted.
///  3. Ordering is by the POT's own admission stamp (`pot_records.createdAt`
///     — an attacker cannot backdate or advance it by filing markers), then
///     the marker stamp, then `rowid` as a total order. EVERY level carries
///     an explicit `ORDER BY`.
///
/// BINDS, in order: `identity`, `limit` (POTS), `quota` (unknown-pot slots).
/// Rows returned are therefore bounded by `2 × limit`.
///
/// # Residual — stated plainly, because the improvement is modest
///
/// This does NOT make the window expensive to fill. An attacker who copies
/// `limit` REAL, recently-admitted pot txids out of the very index being
/// queried — they are public — and files one marker per pot naming the victim
/// achieves the same displacement at the same ~`limit`-dust cost as before.
/// The honest net gain is **from "any N junk rows" to "N junk rows naming
/// real, recent pot txids"**, plus the outright death of the zero-forgery
/// replay variant and of free invented-pot flooding. The v2 slot is
/// likewise front-runnable (#252's backfill publishes honest v2 markers long
/// after a pot txid became public, so a forged row can be OLDER) — bounded
/// to the honest client failing to LATCH and republishing, never to a wrong
/// answer, because `lookupPotParty` verifies both signatures itself. Closing
/// the rest needs signature verification at admission or read time, which
/// this crate does not do by doctrine (the READER verifies).
pub fn potparty_list_for_identity_sql() -> String {
    // NUMBERED parameters (?1 identity, ?2 limit, ?3 quota, ?4 row_cap): the
    // quota appears twice and the identity sits in the INNERMOST subquery, so
    // anonymous `?` would bind in a textual order nobody could keep straight.
    "SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
            recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
            txid, outputIndex, createdAt \
     FROM (SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
                  recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
                  txid, outputIndex, createdAt, isV2, markerRowid, \
                  potCreatedAt, potFirstMarkerAt, tier, \
                  DENSE_RANK() OVER (ORDER BY tier ASC, \
                                              COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
                                              potTxid ASC, potVout ASC) AS finalRank \
           FROM (SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
                        recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
                        txid, outputIndex, createdAt, isV2, markerRowid, \
                        potCreatedAt, potFirstMarkerAt, \
                        CASE WHEN unknownPot = 0 OR potRank <= ?3 THEN 0 ELSE 1 END AS tier \
                 FROM (SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
                              recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
                              txid, outputIndex, createdAt, isV2, markerRowid, \
                              potCreatedAt, potFirstMarkerAt, unknownPot, \
                              DENSE_RANK() OVER (PARTITION BY unknownPot \
                                                 ORDER BY COALESCE(potCreatedAt, \
                                                                   potFirstMarkerAt) DESC, \
                                                          potTxid ASC, potVout ASC) AS potRank \
                       FROM (SELECT pp.identity AS identity, \
                                    pp.opponentIdentity AS opponentIdentity, \
                                    pp.gameId AS gameId, pp.potTxid AS potTxid, \
                                    pp.potVout AS potVout, \
                                    pp.recoveryHeight AS recoveryHeight, \
                                    pp.sigHex AS sigHex, \
                                    pp.seatSettlePubkey AS seatSettlePubkey, \
                                    pp.seatSigHex AS seatSigHex, \
                                    pp.txid AS txid, pp.outputIndex AS outputIndex, \
                                    pp.createdAt AS createdAt, pp.rowid AS markerRowid, \
                                    r.createdAt AS potCreatedAt, \
                                    MIN(pp.createdAt) OVER (PARTITION BY pp.potTxid, \
                                                                         pp.potVout) \
                                        AS potFirstMarkerAt, \
                                    CASE WHEN pp.seatSettlePubkey IS NULL \
                                         THEN 0 ELSE 1 END AS isV2, \
                                    CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot, \
                                    ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout, \
                                                                    CASE WHEN \
                                                                      pp.seatSettlePubkey IS NULL \
                                                                      THEN 0 ELSE 1 END \
                                                       ORDER BY pp.createdAt ASC, \
                                                                pp.rowid ASC) AS rn \
                             FROM potparty_records pp \
                             LEFT JOIN pot_records r \
                                    ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
                             WHERE pp.identity = ?1) \
                       WHERE rn = 1))) \
     WHERE finalRank <= ?2 \
     ORDER BY tier ASC, COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
              potTxid ASC, potVout ASC, isV2 DESC, markerRowid ASC \
     LIMIT ?4"
        .to_string()
}

/// `ls_potrefund partyFor` — the identity-scoped pre-signed-refund-backup
/// window. Same bounds as [`potparty_list_for_identity_sql`] (per-POT-OUTPOINT
/// collapse, existence tier with a reserved quota, a fully explicit ORDER BY
/// at every level, `limit` counting POTS) — read that doc for the dust-DoS
/// they close and the residual they do not. Two differences:
///
///  - there is no v2/seat-binding split here, so each pot yields exactly ONE
///    row (its oldest backup marker) and the row cap equals the pot cap; and
///  - the stakes are the highest of the family: these rows carry
///    `refundRawHex`, the pre-signed refund a seed-only client re-broadcasts
///    to bring its ante home when the tower's dead-man switch never fired.
///    Displacing them off the window is displacing the money.
///
/// BINDS, in order: `identity`, `limit` (POTS), `quota` (unknown-pot slots),
/// `row_cap`.
pub fn potrefund_list_for_identity_sql() -> String {
    // NUMBERED parameters — see `potparty_list_for_identity_sql`.
    "SELECT identity, gameId, potTxid, potVout, refundRawHex, \
            sigHex, txid, outputIndex, createdAt \
     FROM (SELECT identity, gameId, potTxid, potVout, refundRawHex, sigHex, \
                  txid, outputIndex, createdAt, markerRowid, \
                  potCreatedAt, potFirstMarkerAt, tier, \
                  DENSE_RANK() OVER (ORDER BY tier ASC, \
                                              COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
                                              potTxid ASC, potVout ASC) AS finalRank \
           FROM (SELECT identity, gameId, potTxid, potVout, refundRawHex, sigHex, \
                        txid, outputIndex, createdAt, markerRowid, \
                        potCreatedAt, potFirstMarkerAt, \
                        CASE WHEN unknownPot = 0 OR potRank <= ?3 THEN 0 ELSE 1 END AS tier \
                 FROM (SELECT identity, gameId, potTxid, potVout, refundRawHex, sigHex, \
                              txid, outputIndex, createdAt, markerRowid, \
                              potCreatedAt, potFirstMarkerAt, unknownPot, \
                              DENSE_RANK() OVER (PARTITION BY unknownPot \
                                                 ORDER BY COALESCE(potCreatedAt, \
                                                                   potFirstMarkerAt) DESC, \
                                                          potTxid ASC, potVout ASC) AS potRank \
                       FROM (SELECT pr.identity AS identity, pr.gameId AS gameId, \
                                    pr.potTxid AS potTxid, pr.potVout AS potVout, \
                                    pr.refundRawHex AS refundRawHex, pr.sigHex AS sigHex, \
                                    pr.txid AS txid, pr.outputIndex AS outputIndex, \
                                    pr.createdAt AS createdAt, pr.rowid AS markerRowid, \
                                    r.createdAt AS potCreatedAt, \
                                    MIN(pr.createdAt) OVER (PARTITION BY pr.potTxid, \
                                                                         pr.potVout) \
                                        AS potFirstMarkerAt, \
                                    CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot, \
                                    ROW_NUMBER() OVER (PARTITION BY pr.potTxid, pr.potVout \
                                                       ORDER BY pr.createdAt ASC, \
                                                                pr.rowid ASC) AS rn \
                             FROM potrefund_records pr \
                             LEFT JOIN pot_records r \
                                    ON r.txid = pr.potTxid AND r.outputIndex = pr.potVout \
                             WHERE pr.identity = ?1) \
                       WHERE rn = 1))) \
     WHERE finalRank <= ?2 \
     ORDER BY tier ASC, COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
              potTxid ASC, potVout ASC, markerRowid ASC \
     LIMIT ?4"
        .to_string()
}

/// `ls_potparty byPot` / `ls_potrefund byPot` — the POT-scoped windows, in
/// OLDEST-first order (bsv-low #281).
///
/// The pot outpoint is public from the moment funding lands, so under a
/// NEWEST-first window `limit` dust markers naming the pot buried BOTH honest
/// seat markers — and for `ls_potrefund` that is the only backup that can
/// bring the money home (`lookupPotRefund` unions every row's `refundRawHex`,
/// so ordering is not otherwise load-bearing). The honest markers are
/// published AT funding, so oldest-first puts them permanently at the head of
/// the window: an attacker would have to land `limit` admitted rows BEFORE
/// the seats themselves, and cannot spam its way in front afterwards.
/// `rowid ASC` breaks same-second ties into a total order.
///
/// Built from the caller's own SELECT list so the tests execute the SHIPPED
/// string rather than a transcription of it.
pub fn list_for_pot_sql(select: &str) -> String {
    format!(
        "{select} WHERE potTxid = ? AND potVout = ? \
         ORDER BY createdAt ASC, rowid ASC LIMIT ?"
    )
}

/// How many of the newest pots ABSENT from `pot_records` are promoted into
/// the main tier instead of being demoted behind every indexed pot
/// (bsv-low #281 F3): a STRICT existence tier silently becomes a FILTER once
/// `limit` binds, dropping exactly the fresh pot a recovering client most
/// needs. One tenth of the page, at least one slot.
pub fn unknown_pot_quota(limit: usize) -> usize {
    (limit / 10).max(1)
}

/// Row cap for the identity-scoped windows: `limit` pots, at most two rows
/// each (a v1 and a v2 marker), plus the promoted unknown-pot slice.
pub fn identity_window_row_cap(limit: usize) -> usize {
    limit
        .saturating_add(unknown_pot_quota(limit))
        .saturating_mul(2)
}

#[async_trait(?Send)]
impl PotpartyStorage for D1PotpartyStorage {
    async fn store_record(&self, record: &PotpartyRecord) -> Result<(), PotpartyStorageError> {
        // INSERT OR IGNORE on the (txid, outputIndex) primary key — a
        // replayed submit of the same output is a no-op; markers for the
        // same identity from different txs are ALL kept; never overwrite,
        // never delete.
        Query::new(
            "INSERT OR IGNORE INTO potparty_records \
             (identity, opponentIdentity, gameId, potTxid, potVout, \
              recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
              txid, outputIndex, createdAt) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.identity.as_str())
        .bind(record.opponent_identity.as_str())
        .bind(record.game_id.as_str())
        .bind(record.pot_txid.as_str())
        .bind(record.pot_vout)
        .bind(record.recovery_height)
        .bind(record.sig_hex.as_str())
        .bind(record.seat_settle_pubkey.as_deref())
        .bind(record.seat_sig_hex.as_deref())
        .bind(record.txid.as_str())
        .bind(record.output_index)
        .bind(current_unix_seconds_i64())
        .execute(&self.db)
        .await
        .map_err(potparty_err)
    }

    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError> {
        // Per-pot + existence-tiered window — see
        // `potparty_list_for_identity_sql` for the dust-DoS this shape closes
        // (bsv-low #281).
        let rows: Vec<PotpartyRow> = Query::new(potparty_list_for_identity_sql())
            .bind(identity)
            .bind(limit as u32)
            .bind(unknown_pot_quota(limit) as u32)
            .bind(identity_window_row_cap(limit) as u32)
            .fetch_all(&self.db)
            .await
            .map_err(potparty_err)?;
        Ok(rows.into_iter().map(PotpartyRow::into_record).collect())
    }

    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError> {
        // OLDEST FIRST — see `list_for_pot_sql` (bsv-low #281).
        let rows: Vec<PotpartyRow> = Query::new(list_for_pot_sql(POTPARTY_SELECT))
            .bind(pot_txid)
            .bind(pot_vout)
            .bind(limit as u32)
            .fetch_all(&self.db)
            .await
            .map_err(potparty_err)?;
        Ok(rows.into_iter().map(PotpartyRow::into_record).collect())
    }
}

// =============================================================================
// D1PotrefundStorage
// =============================================================================

/// Row for potrefund-marker queries. TEXT columns arrive as `String`;
/// `potVout` / `outputIndex` / `createdAt` are INTEGER columns but D1
/// returns numbers as f64.
#[derive(Deserialize)]
struct PotrefundRow {
    identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "potVout")]
    pot_vout: f64,
    #[serde(rename = "refundRawHex")]
    refund_raw_hex: Option<String>,
    #[serde(rename = "sigHex")]
    sig_hex: Option<String>,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "createdAt")]
    created_at: Option<f64>,
}

impl PotrefundRow {
    fn into_record(self) -> PotrefundRecord {
        PotrefundRecord {
            identity: self.identity,
            game_id: self.game_id,
            pot_txid: self.pot_txid,
            pot_vout: self.pot_vout as u32,
            // Both columns are nullable in the schema but the admit path
            // always writes them; an impossible NULL reads back as "".
            refund_raw_hex: self.refund_raw_hex.unwrap_or_default(),
            sig_hex: self.sig_hex.unwrap_or_default(),
            txid: self.txid,
            output_index: self.output_index as u32,
            created_at: self.created_at.unwrap_or(0.0) as i64,
        }
    }
}

/// Cloudflare D1 implementation of the PotrefundStorage trait
/// (tm_potrefund / ls_potrefund, bsv-low #191).
///
/// Schema: `potrefund_records` in `d1::OVERLAY_MIGRATIONS`. Keyed by the
/// marker OUTPOINT (txid, outputIndex); `INSERT OR IGNORE` makes a replayed
/// submit of the same output a no-op, while markers for the same pot from
/// DIFFERENT txs are ALL kept (the censorship-front-run fix, and both seats
/// may publish a backup). Rows are NEVER deleted (a pre-signed refund backup
/// is permanent recovery history; the lookup service's spend/eviction hooks
/// are no-ops). `createdAt` is stamped here at insert (the record's value is
/// ignored) and drives the newest-first list ordering; `rowid DESC` breaks
/// same-second ties in insertion order.
pub struct D1PotrefundStorage {
    db: Rc<D1Database>,
}

impl D1PotrefundStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn potrefund_err(e: String) -> PotrefundStorageError {
    PotrefundStorageError::Database(e)
}

const POTREFUND_SELECT: &str = "SELECT identity, gameId, potTxid, potVout, refundRawHex, \
     sigHex, txid, outputIndex, createdAt FROM potrefund_records";

#[async_trait(?Send)]
impl PotrefundStorage for D1PotrefundStorage {
    async fn store_record(&self, record: &PotrefundRecord) -> Result<(), PotrefundStorageError> {
        // INSERT OR IGNORE on the (txid, outputIndex) primary key — a
        // replayed submit of the same output is a no-op; markers for the
        // same pot from different txs are ALL kept; never overwrite, never
        // delete.
        Query::new(
            "INSERT OR IGNORE INTO potrefund_records \
             (identity, gameId, potTxid, potVout, refundRawHex, \
              sigHex, txid, outputIndex, createdAt) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.identity.as_str())
        .bind(record.game_id.as_str())
        .bind(record.pot_txid.as_str())
        .bind(record.pot_vout)
        .bind(record.refund_raw_hex.as_str())
        .bind(record.sig_hex.as_str())
        .bind(record.txid.as_str())
        .bind(record.output_index)
        .bind(current_unix_seconds_i64())
        .execute(&self.db)
        .await
        .map_err(potrefund_err)
    }

    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<PotrefundRecord>, PotrefundStorageError> {
        // Per-pot + existence-tiered window — the IDENTICAL dust-DoS shape
        // `potparty_list_for_identity_sql` documents (bsv-low #281), and if
        // anything the sharper money path: these rows carry the PRE-SIGNED
        // REFUND a seed-only client re-broadcasts when the tower never fired.
        let rows: Vec<PotrefundRow> = Query::new(potrefund_list_for_identity_sql())
            .bind(identity)
            .bind(limit as u32)
            .bind(unknown_pot_quota(limit) as u32)
            .bind(identity_window_row_cap(limit) as u32)
            .fetch_all(&self.db)
            .await
            .map_err(potrefund_err)?;
        Ok(rows.into_iter().map(PotrefundRow::into_record).collect())
    }

    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
    ) -> Result<Vec<PotrefundRecord>, PotrefundStorageError> {
        // OLDEST FIRST — see `list_for_pot_sql` (bsv-low #281).
        let rows: Vec<PotrefundRow> = Query::new(list_for_pot_sql(POTREFUND_SELECT))
            .bind(pot_txid)
            .bind(pot_vout)
            .bind(limit as u32)
            .fetch_all(&self.db)
            .await
            .map_err(potrefund_err)?;
        Ok(rows.into_iter().map(PotrefundRow::into_record).collect())
    }
}

// =============================================================================
// D1ProofStorage
// =============================================================================

/// Row for proof-marker queries. The bundle BLOB is selected as
/// `hex(bundle)` (the `pot_beefs` idiom) and decoded back to bytes;
/// `outputIndex` / `createdAt` are INTEGER columns but D1 returns
/// numbers as f64.
#[derive(Deserialize)]
struct ProofRow {
    #[serde(rename = "gameId")]
    game_id: String,
    winner: String,
    #[serde(rename = "sigHex")]
    sig_hex: Option<String>,
    /// hex(bundle) — decoded in `into_record`.
    bundle: String,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "createdAt")]
    created_at: Option<f64>,
}

impl ProofRow {
    fn into_record(self) -> ProofRecord {
        ProofRecord {
            game_id: self.game_id,
            winner: self.winner,
            // The column is nullable in the schema but the admit path
            // always writes it; an impossible NULL reads back as "".
            sig_hex: self.sig_hex.unwrap_or_default(),
            // hex(bundle) → bytes. The column is NOT NULL and written
            // from parse-validated bytes; undecodable hex is impossible,
            // but fail toward an empty bundle (which no client verify
            // ever accepts) rather than a panic.
            bundle: hex::decode(&self.bundle).unwrap_or_default(),
            txid: self.txid,
            output_index: self.output_index as u32,
            created_at: self.created_at.unwrap_or(0.0) as i64,
        }
    }
}

/// Cloudflare D1 implementation of the ProofStorage trait
/// (tm_proof / ls_proof, bsv-low leaderboard rung 3).
///
/// Schema: `proof_markers` in `d1::OVERLAY_MIGRATIONS`. Keyed by the
/// marker OUTPOINT (txid, outputIndex); `INSERT OR IGNORE` makes a
/// replayed submit of the same output a no-op, while bundles for the
/// same (gameId, winner) from DIFFERENT txs are ALL kept (the tm_result
/// censorship lesson — a garbage bundle can never front-run the real
/// proof out of the index; clients verify each bundle). Rows are NEVER
/// deleted (permanence — the lookup service's spend/eviction hooks are
/// no-ops). The bundle is stored as a BLOB and read back via `hex()`
/// (the `pot_beefs` idiom). `createdAt` is stamped here at insert;
/// `rowid DESC` breaks same-second ties in insertion order.
pub struct D1ProofStorage {
    db: Rc<D1Database>,
}

impl D1ProofStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self { db }
    }
}

fn proof_err(e: String) -> ProofStorageError {
    ProofStorageError::Database(e)
}

#[async_trait(?Send)]
impl ProofStorage for D1ProofStorage {
    async fn store_record(&self, record: &ProofRecord) -> Result<(), ProofStorageError> {
        // INSERT OR IGNORE on the (txid, outputIndex) primary key — a
        // replayed submit of the same output is a no-op; bundles for the
        // same (gameId, winner) from different txs are ALL kept; never
        // overwrite, never delete.
        Query::new(
            "INSERT OR IGNORE INTO proof_markers \
             (gameId, winner, sigHex, bundle, txid, outputIndex, createdAt) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.game_id.as_str())
        .bind(record.winner.as_str())
        .bind(record.sig_hex.as_str())
        .bind(record.bundle.clone()) // BLOB bind, like pot_beefs
        .bind(record.txid.as_str())
        .bind(record.output_index)
        .bind(current_unix_seconds_i64())
        .execute(&self.db)
        .await
        .map_err(proof_err)
    }

    async fn list_for_game_winner(
        &self,
        game_id: &str,
        winner: &str,
        limit: usize,
    ) -> Result<Vec<ProofRecord>, ProofStorageError> {
        let rows: Vec<ProofRow> = Query::new(
            "SELECT gameId, winner, sigHex, hex(bundle) AS bundle, txid, outputIndex, createdAt \
             FROM proof_markers WHERE gameId = ? AND winner = ? \
             ORDER BY createdAt DESC, rowid DESC LIMIT ?",
        )
        .bind(game_id)
        .bind(winner)
        .bind(limit as u32)
        .fetch_all(&self.db)
        .await
        .map_err(proof_err)?;
        Ok(rows.into_iter().map(ProofRow::into_record).collect())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_row_conversion_decodes_bundle_hex() {
        // hex(bundle) round-trips to the raw bytes; numeric columns come
        // back as f64 from D1.
        let row = ProofRow {
            game_id: "11".repeat(32),
            winner: "02aa".into(),
            sig_hex: Some("3045ab".into()),
            bundle: hex::encode(b"{\"v\":1}").to_uppercase(), // SQLite hex() is uppercase
            txid: "tx1".into(),
            output_index: 2.0,
            created_at: Some(1_234.0),
        };
        let r = row.into_record();
        assert_eq!(r.bundle, b"{\"v\":1}");
        assert_eq!(r.output_index, 2);
        assert_eq!(r.created_at, 1_234);
        assert_eq!(r.sig_hex, "3045ab");
    }

    #[test]
    fn utxo_row_conversion() {
        let row = UTXORow {
            txid: "abc123".into(),
            output_index: 3.0,
        };
        let r = row.into_ref();
        assert_eq!(r.txid, "abc123");
        assert_eq!(r.output_index, 3);
    }

    #[test]
    fn utxo_row_zero_index() {
        let row = UTXORow {
            txid: "xyz".into(),
            output_index: 0.0,
        };
        let r = row.into_ref();
        assert_eq!(r.output_index, 0);
    }

    #[test]
    fn pot_row_unspent_conversion() {
        // spent=0, spendingTxid NULL → unspent record.
        let row = PotRow {
            txid: "pot1".into(),
            output_index: 0.0,
            spent: 0.0,
            spending_txid: None,
            spent_confirmed: 0.0,
        };
        let r = row.into_record();
        assert_eq!(r.txid, "pot1");
        assert_eq!(r.output_index, 0);
        assert!(!r.spent);
        assert_eq!(r.spending_txid, None);
        assert!(!r.spent_confirmed);
    }

    #[test]
    fn pot_row_spent_conversion() {
        // spent=1, spendingTxid set → landing proof.
        let row = PotRow {
            txid: "pot1".into(),
            output_index: 2.0,
            spent: 1.0,
            spending_txid: Some("settleTx".into()),
            spent_confirmed: 1.0,
        };
        let r = row.into_record();
        assert_eq!(r.output_index, 2);
        assert!(r.spent);
        assert_eq!(r.spending_txid.as_deref(), Some("settleTx"));
        assert!(r.spent_confirmed);
    }

    #[test]
    fn pot_row_spent_confirmed_defaults_when_column_absent() {
        // A read that races the additive migration (row JSON without the
        // spentConfirmed column) still deserializes → false.
        let r: PotRow = serde_json::from_value(serde_json::json!({
            "txid": "pot1", "outputIndex": 0.0, "spent": 1.0, "spendingTxid": "settleTx"
        }))
        .unwrap();
        assert!(!r.into_record().spent_confirmed);
    }

    // ── mark_spent SQL (prefer-confirmed / never-clobber-with-unconfirmed) ──

    #[test]
    fn mark_spent_sql_confirmed_always_writes_and_latches_flag() {
        let sql = mark_spent_sql(true);
        // Chain truth: sets the pointer AND the flag…
        assert!(sql.contains("SET spent = 1, spendingTxid = ?, spentConfirmed = 1"));
        // …with no confirmation guard (last-confirmed-wins), UPDATE-only,
        // never DELETE.
        assert!(!sql.contains("spentConfirmed = 0"));
        assert!(sql.starts_with("UPDATE pot_records"));
        assert!(sql.contains("WHERE txid = ? AND outputIndex = ?"));
        assert!(!sql.to_uppercase().contains("DELETE"));
    }

    #[test]
    fn mark_spent_sql_unconfirmed_guarded_and_never_touches_flag() {
        let sql = mark_spent_sql(false);
        // The guard: an unconfirmed claim only lands while no confirmed
        // pointer exists (spentConfirmed = 0)…
        assert!(sql.contains("WHERE txid = ? AND outputIndex = ? AND spentConfirmed = 0"));
        // …and the SET clause never touches the flag (it DOES stamp spentAt —
        // the #228 backstop age anchor — on every accepted write).
        assert!(sql.contains("SET spent = 1, spendingTxid = ?, spentAt = unixepoch() WHERE"));
        assert!(!sql.contains("spentConfirmed = 1"));
        assert!(sql.starts_with("UPDATE pot_records"));
        assert!(!sql.to_uppercase().contains("DELETE"));
    }

    #[test]
    fn pot_beef_hex_readback_decodes() {
        // SQLite hex() emits UPPERCASE — must decode; lowercase too.
        assert_eq!(
            decode_pot_beef_hex(Some("BEEF".into())),
            Some(vec![0xBE, 0xEF])
        );
        assert_eq!(
            decode_pot_beef_hex(Some("beef".into())),
            Some(vec![0xbe, 0xef])
        );
        // NULL column / empty / undecodable → None (never served as bytes).
        assert_eq!(decode_pot_beef_hex(None), None);
        assert_eq!(decode_pot_beef_hex(Some("".into())), None);
        assert_eq!(decode_pot_beef_hex(Some("abc".into())), None);
        assert_eq!(decode_pot_beef_hex(Some("zz".into())), None);
    }

    #[test]
    fn pot_beef_write_gate_longer_wins_never_clobbers() {
        // No row yet → any non-empty beef writes.
        assert!(beef_write_allowed(None, 1));
        assert!(beef_write_allowed(None, 100));
        // Empty is rejected even on a fresh key.
        assert!(!beef_write_allowed(None, 0));
        assert!(!beef_write_allowed(Some(4), 0));
        // Strictly longer wins…
        assert!(beef_write_allowed(Some(4), 5));
        // …shorter/equal never clobbers (the "vanishing table" lesson).
        assert!(!beef_write_allowed(Some(4), 3));
        assert!(!beef_write_allowed(Some(4), 4));
    }

    #[test]
    fn pot_beef_len_row_converts() {
        // D1 returns length(beef) as f64 — the usize cast the write gate
        // consumes.
        let row = BeefLenRow { len: 1234.0 };
        assert_eq!(row.len as usize, 1234);
    }

    // ════════════════════════════════════════════════════════════════════
    // bsv-low #281 — identity-scoped read windows are dust-DoS-bounded
    //
    // These tests EXECUTE the exact shipped SQL against REAL SQLite
    // (`rusqlite`, bundled) over the PRODUCTION schema (`d1::OVERLAY_MIGRATIONS`
    // verbatim — no hand-written CREATE TABLE that could drift). Pinning the
    // SQL text is not enough: the #230 gate called that out as the weakness in
    // its own F2 test. Each suite also runs the LEGACY query it replaced, so
    // the defect stays demonstrated in-repo.
    //
    // F4 (2026-07-28 re-gate) — every ordering test below is built so that
    // PHYSICAL row order (rowid / insertion) CONTRADICTS the order the query
    // promises, and asserts the exact expected sequence. A test whose green
    // could come from SQLite's incidental row order proves nothing; removing
    // the guarantee it names must turn it red.
    // ════════════════════════════════════════════════════════════════════

    /// The `ls_potparty partyFor` query as it shipped BEFORE #281.
    const LEGACY_POTPARTY_PARTY_FOR_SQL: &str = "SELECT identity, opponentIdentity, gameId, \
         potTxid, potVout, recoveryHeight, sigHex, txid, outputIndex, createdAt \
         FROM potparty_records WHERE identity = ? \
         ORDER BY createdAt DESC, rowid DESC LIMIT ?";

    /// The `ls_potrefund partyFor` query as it shipped BEFORE #281.
    const LEGACY_POTREFUND_PARTY_FOR_SQL: &str = "SELECT identity, gameId, potTxid, potVout, \
         refundRawHex, sigHex, txid, outputIndex, createdAt \
         FROM potrefund_records WHERE identity = ? \
         ORDER BY createdAt DESC, rowid DESC LIMIT ?";

    /// A fresh in-memory SQLite carrying the REAL production schema.
    ///
    /// `OVERLAY_MIGRATIONS` is applied statement-by-statement exactly as
    /// `d1::run_migrations` does, tolerating ONLY the error class the
    /// production runner tolerates (a re-run additive `ALTER TABLE` on an
    /// existing column). Anything else fails loudly — a silently-skipped
    /// migration would be schema drift this proof could not see.
    fn production_schema_db() -> rusqlite::Connection {
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
        conn
    }

    fn h64(seed: u8) -> String {
        format!("{seed:02x}").repeat(32)
    }

    fn victim_id() -> String {
        format!("02{}", "a1".repeat(32))
    }

    fn insert_pot(
        conn: &rusqlite::Connection,
        txid: &str,
        vout: u32,
        created_at: i64,
        spent: bool,
    ) {
        conn.execute(
            "INSERT OR IGNORE INTO pot_records \
             (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                txid,
                vout,
                i32::from(spent),
                if spent { Some(h64(0xfe)) } else { None },
                i32::from(spent),
                created_at
            ],
        )
        .expect("insert pot_records");
    }

    /// File a potparty marker. `settle_pubkey = Some` ⇒ a v2 (seat-binding)
    /// marker. The production write is `INSERT OR IGNORE` on the marker
    /// OUTPOINT, so every distinct `(txid, outputIndex)` lands — which is
    /// exactly why anyone can file unlimited rows naming anyone.
    fn insert_potparty(
        conn: &rusqlite::Connection,
        identity: &str,
        pot_txid: &str,
        pot_vout: u32,
        marker_txid: &str,
        created_at: i64,
        settle_pubkey: Option<&str>,
    ) {
        conn.execute(
            "INSERT OR IGNORE INTO potparty_records \
             (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
              sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, '3045id', ?7, ?8, ?9, 0, ?10)",
            rusqlite::params![
                identity,
                h64(0xbb),
                h64(0x11),
                pot_txid,
                pot_vout,
                850_000,
                settle_pubkey,
                settle_pubkey.map(|_| "3045seat"),
                marker_txid,
                created_at
            ],
        )
        .expect("insert potparty_records");
    }

    fn insert_potrefund(
        conn: &rusqlite::Connection,
        identity: &str,
        pot_txid: &str,
        pot_vout: u32,
        marker_txid: &str,
        created_at: i64,
    ) {
        conn.execute(
            "INSERT OR IGNORE INTO potrefund_records \
             (identity, gameId, potTxid, potVout, refundRawHex, sigHex, \
              txid, outputIndex, createdAt) \
             VALUES (?1, ?2, ?3, ?4, '0100de', '3045ab', ?5, 0, ?6)",
            rusqlite::params![
                identity,
                h64(0x11),
                pot_txid,
                pot_vout,
                marker_txid,
                created_at
            ],
        )
        .expect("insert potrefund_records");
    }

    /// Run one of the SHIPPED identity windows with its real bind list —
    /// `(identity, limit, quota, row_cap)` — and project one column.
    fn window_col(
        conn: &rusqlite::Connection,
        sql: &str,
        identity: &str,
        limit: usize,
        col: &str,
    ) -> Vec<String> {
        let mut stmt = conn.prepare(sql).expect("prepare");
        stmt.query_map(
            rusqlite::params![
                identity,
                limit as u32,
                unknown_pot_quota(limit) as u32,
                identity_window_row_cap(limit) as u32
            ],
            |r| r.get::<_, String>(col),
        )
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows")
    }

    /// The legacy 2-bind `(identity, limit)` shape.
    fn legacy_col(
        conn: &rusqlite::Connection,
        sql: &str,
        identity: &str,
        limit: usize,
        col: &str,
    ) -> Vec<String> {
        let mut stmt = conn.prepare(sql).expect("prepare");
        stmt.query_map(rusqlite::params![identity, limit as u32], |r| {
            r.get::<_, String>(col)
        })
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows")
    }

    /// Attacker rows naming invented pots — enough to fill the page alone.
    const DUST_GHOSTS: u32 = 120;
    /// Attacker rows replaying the victim's own marker for its real pot.
    const DUST_REPLAYS: u32 = 60;

    /// The victim's ONE real (funded, admitted, SPENT) pot plus the two cheap
    /// attacker variants, every attacker row stamped NEWER than the honest
    /// marker — recency being the only thing the legacy window ordered on.
    fn seed_dust_attack(conn: &rusqlite::Connection, victim: &str, potparty: bool) -> String {
        let honest_pot = h64(0xaa);
        insert_pot(conn, &honest_pot, 0, 1_000, true);
        let insert: &dyn Fn(&str, &str, &str, i64) = if potparty {
            &|id, pot, mtx, at| insert_potparty(conn, id, pot, 0, mtx, at, None)
        } else {
            &|id, pot, mtx, at| insert_potrefund(conn, id, pot, 0, mtx, at)
        };
        insert(victim, &honest_pot, "txHONEST", 1_001);
        for i in 0..DUST_REPLAYS {
            insert(
                victim,
                &honest_pot,
                &format!("txREPLAY{i:03}"),
                2_000 + i as i64,
            );
        }
        for i in 0..DUST_GHOSTS {
            insert(
                victim,
                &format!("{:064x}", 0xdead_0000u64 + i as u64),
                &format!("txGHOST{i:03}"),
                3_000 + i as i64,
            );
        }
        honest_pot
    }

    /// RED — the defect, executed.
    #[test]
    fn potparty_legacy_window_is_dust_displaceable_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        let honest_pot = seed_dust_attack(&conn, &victim, true);
        let got = legacy_col(
            &conn,
            LEGACY_POTPARTY_PARTY_FOR_SQL,
            &victim,
            100,
            "potTxid",
        );
        assert_eq!(got.len(), 100, "the legacy window returns a full page…");
        assert_eq!(
            got.iter().filter(|t| **t == honest_pot).count(),
            0,
            "…and the victim's REAL pot appears ZERO times — total erasure of \
             the row that leads a recovering client to its money"
        );
    }

    /// GREEN — the shipped window over the same table state.
    #[test]
    fn potparty_window_survives_the_dust_attack_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        let honest_pot = seed_dust_attack(&conn, &victim, true);
        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "potTxid",
        );
        assert_eq!(
            got.iter().filter(|t| **t == honest_pot).count(),
            1,
            "the 1 + DUST_REPLAYS marker rows naming the real pot collapse to ONE slot"
        );
        // The PROMOTED ghosts are newer than the honest pot, so inside the
        // main tier they legitimately sort ahead of it — but promotion is
        // capped at the reserved quota, so the real pot can never be pushed
        // more than one quota down, let alone off the page. Pre-fix it was
        // absent entirely.
        let at = got.iter().position(|t| *t == honest_pot).unwrap();
        assert!(
            at <= unknown_pot_quota(100),
            "the real pot sits within one quota of the top (was: absent), got {at}"
        );
        // Every remaining ghost is DEMOTED behind it — never erased (a pot
        // whose tm_pot admission is still in flight must stay reachable), but
        // never able to displace a real one either.
        assert!(
            got[..at].iter().all(|t| t.starts_with("0000")),
            "only promoted ghosts precede the real pot"
        );
    }

    #[test]
    fn potrefund_legacy_window_is_dust_displaceable_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        let honest_pot = seed_dust_attack(&conn, &victim, false);
        let got = legacy_col(
            &conn,
            LEGACY_POTREFUND_PARTY_FOR_SQL,
            &victim,
            100,
            "potTxid",
        );
        assert_eq!(got.len(), 100);
        assert_eq!(
            got.iter().filter(|t| **t == honest_pot).count(),
            0,
            "the pre-signed refund backup for the victim's real pot is erased"
        );
    }

    #[test]
    fn potrefund_window_survives_the_dust_attack_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        let honest_pot = seed_dust_attack(&conn, &victim, false);
        let got = window_col(
            &conn,
            &potrefund_list_for_identity_sql(),
            &victim,
            100,
            "potTxid",
        );
        let at = got.iter().position(|t| *t == honest_pot).unwrap();
        assert!(
            at <= unknown_pot_quota(100),
            "the refund backup is back, within one quota of the top (was: absent)"
        );
        assert_eq!(got.iter().filter(|t| **t == honest_pot).count(), 1);
    }

    /// F2 (re-gate, HIGH — honest-player MONEY LEAK). `decideV2Step`
    /// (`potPartyPending.ts`) only returns `'done'` when an indexed row for
    /// (gameId, potTxid, identity) carries `seatSettlePubkey`. If this window
    /// omits the column — or returns only the pot's v1 row — `v2Indexed`
    /// never latches and `workV2Half` publishes a REAL, PAID `createAction`
    /// OP_RETURN on every sweep forever, manufacturing the very dust this
    /// window exists to bound.
    #[test]
    fn partyfor_returns_the_v2_seat_columns_and_the_v1_sibling() {
        let conn = production_schema_db();
        let victim = victim_id();
        let pot = h64(0xaa);
        let settle = format!("02{}", "5e".repeat(32));
        insert_pot(&conn, &pot, 0, 1_000, false);
        // v1 at funding, v2 on the #252 republish — the real-world shape.
        insert_potparty(&conn, &victim, &pot, 0, "txV1", 1_001, None);
        insert_potparty(&conn, &victim, &pot, 0, "txV2", 4_000, Some(&settle));

        let sql = potparty_list_for_identity_sql();
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<(String, Option<String>)> = stmt
            .query_map(rusqlite::params![victim, 100u32, 10u32, 220u32], |r| {
                Ok((
                    r.get::<_, String>("txid")?,
                    r.get::<_, Option<String>>("seatSettlePubkey")?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), 2, "one pot yields BOTH its v1 and its v2 row");
        // The v2 row must be present WITH its key — else the client pays an
        // OP_RETURN fee every sweep, forever.
        assert!(
            rows.iter()
                .any(|(tx, pk)| tx == "txV2" && pk.as_deref() == Some(settle.as_str())),
            "the v2 seat columns must survive the window (the fee-leak bug)"
        );
        // The v1 sibling must ALSO be present: `lookupPotParty` verifies v2
        // signatures client-side and DROPS a row that fails, relying on the
        // v1 sibling for discovery. Return only a (forged) v2 and the pot
        // vanishes from the recovery list entirely.
        assert!(
            rows.iter().any(|(tx, pk)| tx == "txV1" && pk.is_none()),
            "the v1 sibling must survive as the discovery fallback"
        );
    }

    /// …and a forged v2 marker cannot take the v1 sibling's slot with it: the
    /// two groups are partitioned separately, so junk in the v2 group is
    /// bounded to the v2 group.
    #[test]
    fn a_forged_v2_marker_cannot_carry_away_the_v1_discovery_row() {
        let conn = production_schema_db();
        let victim = victim_id();
        let pot = h64(0xaa);
        insert_pot(&conn, &pot, 0, 1_000, false);
        insert_potparty(&conn, &victim, &pot, 0, "txV1", 1_001, None);
        for i in 0..40u32 {
            insert_potparty(
                &conn,
                &victim,
                &pot,
                0,
                &format!("txFORGED{i:03}"),
                500 + i as i64, // EARLIER than the honest v1 — front-running
                Some(&format!("03{:064x}", i)),
            );
        }
        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "txid",
        );
        assert_eq!(
            got.len(),
            2,
            "one pot ⇒ at most two rows, however much junk"
        );
        assert!(
            got.contains(&"txV1".to_string()),
            "the honest v1 discovery row survives 40 front-running forgeries"
        );
    }

    /// F3 (re-gate, MEDIUM) — a strict existence tier silently becomes a
    /// FILTER once the limit binds. 100 indexed pots plus ONE newest pot whose
    /// `tm_pot` admission is still in flight: pre-fix the fresh pot ranked
    /// first, post-tier it fell off the page entirely. The reserved quota is
    /// what keeps it visible.
    #[test]
    fn a_fresh_unindexed_pot_is_not_filtered_out_by_the_limit() {
        let conn = production_schema_db();
        let victim = victim_id();
        for i in 0..100u32 {
            let pot = format!("{:064x}", 0x0000_1000u64 + i as u64);
            insert_pot(&conn, &pot, 0, 1_000 + i as i64, true);
            insert_potparty(
                &conn,
                &victim,
                &pot,
                0,
                &format!("txM{i:03}"),
                1_000 + i as i64,
                None,
            );
        }
        // The pot the client most needs: funded seconds ago, not yet admitted.
        let fresh = h64(0xfa);
        insert_potparty(&conn, &victim, &fresh, 0, "txFRESH", 9_999, None);

        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "potTxid",
        );
        assert!(
            got.contains(&fresh),
            "a real-but-unindexed pot must not be filtered out by the window"
        );
    }

    /// The legitimate use case: `limit` counts POTS, so a player with 100 real
    /// pots sees all 100 — even with every one of them dust-replayed.
    #[test]
    fn a_player_with_100_real_pots_still_sees_all_100_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        for i in 0..100u32 {
            let pot = format!("{:064x}", 0x0000_1000u64 + i as u64);
            insert_pot(&conn, &pot, 0, 1_000 + i as i64, true);
            insert_potparty(
                &conn,
                &victim,
                &pot,
                0,
                &format!("txM{i:03}"),
                1_000 + i as i64,
                None,
            );
            insert_potparty(
                &conn,
                &victim,
                &pot,
                0,
                &format!("txD{i:03}"),
                9_000 + i as i64,
                None,
            );
        }
        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "potTxid",
        );
        let unique: std::collections::HashSet<&String> = got.iter().collect();
        assert_eq!(
            unique.len(),
            100,
            "all 100 real pots returned, one row each"
        );
        assert_eq!(got.len(), 100, "the dust replays add no rows");
    }

    /// F4 — the PARTITION's `createdAt ASC, rowid ASC` is the thing under
    /// test, so the honest (oldest) marker is inserted PHYSICALLY LAST. Any
    /// non-discriminating partition order (or a newest-first one) returns a
    /// junk row instead, so this test cannot pass on SQLite's incidental
    /// order.
    #[test]
    fn the_oldest_marker_represents_a_pot_even_when_stored_last() {
        let conn = production_schema_db();
        let victim = victim_id();
        let pot = h64(0xaa);
        insert_pot(&conn, &pot, 0, 1_000, true);
        for i in 0..20u32 {
            insert_potparty(
                &conn,
                &victim,
                &pot,
                0,
                &format!("txJUNK{i:03}"),
                5_000 + i as i64,
                None,
            );
        }
        // Oldest by createdAt, newest by rowid — the contradiction that makes
        // this test meaningful.
        insert_potparty(&conn, &victim, &pot, 0, "txHONEST", 1_001, None);

        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "txid",
        );
        assert_eq!(
            got,
            vec!["txHONEST".to_string()],
            "oldest marker represents the pot"
        );
    }

    /// F4 — the OUTER ordering is the thing under test: pots are inserted
    /// OLDEST-POT-FIRST, so the promised newest-pot-first answer is the exact
    /// REVERSE of both insertion and rowid order.
    #[test]
    fn the_outer_order_is_pot_recency_not_storage_order() {
        let conn = production_schema_db();
        let victim = victim_id();
        let mut expect: Vec<String> = Vec::new();
        for i in 0..12u32 {
            let pot = format!("{:064x}", 0x0000_3000u64 + i as u64);
            insert_pot(&conn, &pot, 0, 1_000 + i as i64, true);
            // Marker stamps run OPPOSITE to pot stamps, so a query that
            // ordered on the marker instead of the pot also fails here.
            insert_potparty(
                &conn,
                &victim,
                &pot,
                0,
                &format!("txM{i:03}"),
                9_000 - i as i64,
                None,
            );
            expect.push(pot);
        }
        expect.reverse(); // newest POT first
        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "potTxid",
        );
        assert_eq!(got, expect, "exact newest-pot-first sequence");
    }

    /// F4 — plan independence ON TOP of the exact-sequence tests above: the
    /// answer is a function of the STORED ROWS, never of the query PLAN.
    #[test]
    fn window_is_plan_independent_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        for i in 0..40u32 {
            let pot = format!("{:064x}", 0x0000_2000u64 + i as u64);
            insert_pot(&conn, &pot, 0, 5_000 + i as i64, i % 2 == 0);
            insert_potparty(&conn, &victim, &pot, 0, &format!("txB{i:03}"), 7_000, None);
            insert_potparty(&conn, &victim, &pot, 0, &format!("txA{i:03}"), 7_000, None);
            insert_potparty(
                &conn,
                &victim,
                &format!("{:064x}", 0x0000_9000u64 + i as u64),
                0,
                &format!("txG{i:03}"),
                7_000 + i as i64,
                None,
            );
        }
        let snap = |c: &rusqlite::Connection| {
            window_col(c, &potparty_list_for_identity_sql(), &victim, 100, "txid")
        };
        let baseline = snap(&conn);
        assert_eq!(baseline, snap(&conn), "repeat runs agree");
        conn.execute_batch("ANALYZE").unwrap();
        assert_eq!(baseline, snap(&conn), "stable across ANALYZE");
        conn.execute_batch(
            "CREATE INDEX ix1 ON potparty_records(identity, createdAt DESC); \
             CREATE INDEX ix2 ON potparty_records(potTxid, createdAt ASC); \
             CREATE INDEX ix3 ON pot_records(createdAt); \
             ANALYZE",
        )
        .unwrap();
        assert_eq!(
            baseline,
            snap(&conn),
            "stable across a forced plan change — the explicit ORDER BY at \
             every level decides the answer, not SQLite"
        );
        // `txB*` was stored FIRST but `txA*`/`txB*` share a createdAt, so the
        // rowid tiebreak must pick `txB*` — deterministically, every time.
        assert!(
            baseline.iter().filter(|t| t.starts_with("txB")).count() == 40
                && !baseline.iter().any(|t| t.starts_with("txA")),
            "the rowid tiebreak is total and stable"
        );
    }

    /// `byPot` is OLDEST-first: the honest seat markers publish at funding, so
    /// a later flood naming the pot cannot bury them. Built so the honest rows
    /// are stored LAST — incidental order would fail this.
    #[test]
    fn by_pot_window_keeps_the_honest_markers_under_flood_real_sqlite() {
        let conn = production_schema_db();
        let pot = h64(0xaa);
        insert_pot(&conn, &pot, 0, 1_000, false);
        for i in 0..500u32 {
            insert_potrefund(
                &conn,
                &format!("02{:064x}", i),
                &pot,
                0,
                &format!("txFLOOD{i:03}"),
                5_000 + i as i64,
            );
        }
        // Stored last, stamped first.
        insert_potrefund(&conn, &victim_id(), &pot, 0, "txSEATA", 1_001);
        insert_potrefund(
            &conn,
            &format!("03{}", "b2".repeat(32)),
            &pot,
            0,
            "txSEATB",
            1_002,
        );

        let sql = list_for_pot_sql(POTREFUND_SELECT);
        let mut stmt = conn.prepare(&sql).unwrap();
        let got: Vec<String> = stmt
            .query_map(rusqlite::params![pot, 0u32, 100u32], |r| {
                r.get::<_, String>("txid")
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(got.len(), 100, "window full");
        assert_eq!(got[0], "txSEATA", "honest seat markers head the window");
        assert_eq!(got[1], "txSEATB");

        // RED half: the legacy NEWEST-first order buried both of them.
        let legacy = format!(
            "{POTREFUND_SELECT} WHERE potTxid = ? AND potVout = ? \
             ORDER BY createdAt DESC, rowid DESC LIMIT ?"
        );
        let mut stmt = conn.prepare(&legacy).unwrap();
        let old: Vec<String> = stmt
            .query_map(rusqlite::params![pot, 0u32, 100u32], |r| {
                r.get::<_, String>("txid")
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            !old.contains(&"txSEATA".to_string()) && !old.contains(&"txSEATB".to_string()),
            "the legacy byPot window dropped BOTH honest refund backups"
        );
    }

    /// F6 — every other key in the system is the OUTPOINT. Two genuine pots
    /// sharing a funding txid are not reachable in LOW today; partitioning on
    /// the txid alone would silently erase one if they ever were.
    #[test]
    fn two_pots_sharing_a_funding_txid_are_not_collapsed() {
        let conn = production_schema_db();
        let victim = victim_id();
        let txid = h64(0xaa);
        insert_pot(&conn, &txid, 0, 1_000, true);
        insert_pot(&conn, &txid, 1, 1_001, true);
        insert_potparty(&conn, &victim, &txid, 0, "txV0", 1_002, None);
        insert_potparty(&conn, &victim, &txid, 1, "txV1", 1_003, None);
        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "txid",
        );
        assert_eq!(got.len(), 2, "distinct outpoints are distinct pots");
    }

    /// Structural BACKSTOP for both identity windows. The behaviour these
    /// strings stand for is proven by EXECUTING them above; this only catches
    /// a future edit that quietly drops a clause.
    #[test]
    fn identity_windows_are_partitioned_tiered_and_ordered() {
        for (name, sql) in [
            ("potparty", potparty_list_for_identity_sql()),
            ("potrefund", potrefund_list_for_identity_sql()),
        ] {
            assert!(
                sql.contains("ROW_NUMBER() OVER (PARTITION BY"),
                "{name}: the window must count POTS, not rows"
            );
            assert!(
                sql.contains("potTxid, pp.potVout") || sql.contains("potTxid, pr.potVout"),
                "{name}: partition on the pot OUTPOINT, not the txid alone"
            );
            assert!(
                sql.contains("AS unknownPot") && sql.contains("potRank <= ?"),
                "{name}: existence tier with a reserved quota"
            );
            // Ordered at every level: the per-pot window, the pot ranking,
            // and the final projection — nothing left to SQLite's discretion.
            assert_eq!(
                sql.matches("ORDER BY").count(),
                4,
                "{name}: ordered throughout"
            );
            // NUMBERED binds ?1..?4 = identity, limit, quota, row_cap.
            for n in 1..=4 {
                assert!(sql.contains(&format!("?{n}")), "{name}: bind ?{n} is used");
            }
            assert!(!sql.contains("?5"), "{name}: exactly four binds");
        }
        // The potparty window must carry the v2 seat columns (the F2 fee leak).
        let pp = potparty_list_for_identity_sql();
        assert!(
            pp.matches("seatSettlePubkey").count() >= 4 && pp.contains("seatSigHex"),
            "potparty: the v2 seat columns must reach the OUTER select"
        );
    }

    /// Quota/row-cap arithmetic — the values the storage layer binds.
    #[test]
    fn window_bind_helpers_are_sane() {
        assert_eq!(unknown_pot_quota(100), 10);
        assert_eq!(unknown_pot_quota(500), 50);
        assert_eq!(
            unknown_pot_quota(1),
            1,
            "never zero — a fresh pot always fits"
        );
        assert_eq!(unknown_pot_quota(0), 1);
        // limit pots + quota unknown pots, two rows each.
        assert_eq!(identity_window_row_cap(100), 220);
        assert!(
            identity_window_row_cap(usize::MAX) > 0,
            "saturating, never a panic"
        );
    }
}
