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
use overlay_discovery::low::storage::{
    LowRecord, LowRecordType, LowStorage, LowStorageError, LOW_BY_KEY_RESULT_CAP,
    OPEN_TABLES_PER_HOST_CAP, OPEN_TABLES_RESULT_CAP,
};
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
use overlay_discovery::reveal::storage::{
    RevealRecord, RevealStorage, RevealStorageError, REVEAL_RESULT_CAP,
};
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

/// The full `low_records` column set (#290 — the decoded index IS the
/// answer, so the queries read it back instead of just the outpoint refs).
const LOW_RECORD_COLUMNS: &str =
    "recordType, txid, outputIndex, hostIdentity, gameId, stakeSats, rulesHash, \
     relayUrl, expiryHeight";

/// `ls_low findOpenTables` — full index rows (#290), newest-first, capped at
/// [`OPEN_TABLES_RESULT_CAP`] (#291), with a PER-HOST quota of
/// [`OPEN_TABLES_PER_HOST_CAP`] (#291 gate finding M3, the #281 partitioned-
/// window pattern): a flat newest-first cap let ONE identity's
/// [`OPEN_TABLES_RESULT_CAP`] byte-format-admitted junk rows blank every
/// honest table from the lobby before the client's verify-and-drop filter
/// ever saw them. With the partition, a single host occupies at most
/// [`OPEN_TABLES_PER_HOST_CAP`] window slots — blanking the lobby now takes
/// `CAP / PER_HOST` distinct identities' worth of admitted on-chain rows.
/// Residual (accepted, display-only): identities are free keypairs, so a
/// determined multi-identity flood can still displace; no money path reads
/// the lobby (rejoin is keyed byGameId/byHost, money discovery is
/// server-primary).
///
/// `where_clause` is the WhereBuilder output (leading " WHERE …" or empty).
/// Factored out so the real-SQLite tests below execute the SHIPPED string,
/// not a transcription.
pub fn low_open_tables_sql(where_clause: &str) -> String {
    format!(
        "SELECT {LOW_RECORD_COLUMNS} \
         FROM (SELECT {LOW_RECORD_COLUMNS}, createdAt, rowid AS mrowid, \
                      ROW_NUMBER() OVER (PARTITION BY hostIdentity \
                                         ORDER BY createdAt DESC, rowid DESC) AS hostRank \
               FROM low_records{where_clause}) \
         WHERE hostRank <= {OPEN_TABLES_PER_HOST_CAP} \
         ORDER BY createdAt DESC, mrowid DESC LIMIT {OPEN_TABLES_RESULT_CAP}"
    )
}

/// `ls_low byGameId` — full index rows, newest-first, capped (#290/#291).
pub fn low_by_game_id_sql() -> String {
    format!(
        "SELECT {LOW_RECORD_COLUMNS} FROM low_records WHERE gameId = ? \
         ORDER BY createdAt DESC LIMIT {LOW_BY_KEY_RESULT_CAP}"
    )
}

/// `ls_low byHost` — full index rows, newest-first, capped (#290/#291).
pub fn low_by_host_sql() -> String {
    format!(
        "SELECT {LOW_RECORD_COLUMNS} FROM low_records WHERE hostIdentity = ? \
         ORDER BY createdAt DESC LIMIT {LOW_BY_KEY_RESULT_CAP}"
    )
}

/// Row for full low_records queries. D1 returns numbers as f64 and nullable
/// columns as `Option`.
#[derive(Deserialize)]
struct LowRow {
    #[serde(rename = "recordType")]
    record_type: String,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "hostIdentity")]
    host_identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "stakeSats")]
    stake_sats: Option<f64>,
    #[serde(rename = "rulesHash")]
    rules_hash: Option<String>,
    #[serde(rename = "relayUrl")]
    relay_url: Option<String>,
    #[serde(rename = "expiryHeight")]
    expiry_height: Option<f64>,
}

impl LowRow {
    /// `None` on an unknown recordType (can't happen — the writer only ever
    /// stores the two known discriminators; defensive skip, never a panic).
    fn into_record(self) -> Option<LowRecord> {
        Some(LowRecord {
            record_type: LowRecordType::from_str_opt(&self.record_type)?,
            txid: self.txid,
            output_index: self.output_index as u32,
            host_identity: self.host_identity,
            game_id: self.game_id,
            stake_sats: self.stake_sats.map(|s| s as u64),
            rules_hash: self.rules_hash,
            relay_url: self.relay_url,
            expiry_height: self.expiry_height.map(|e| e as u32),
        })
    }
}

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
    ) -> Result<Vec<LowRecord>, LowStorageError> {
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

        // Full index rows (#290) + newest-first bound (#291) — the lobby is
        // a display surface; the cap keeps the newest tables.
        let sql = low_open_tables_sql(&where_clause);
        let mut q = Query::new(sql);
        for p in params {
            q = q.bind(p);
        }
        let rows: Vec<LowRow> = q.fetch_all(&self.db).await.map_err(low_err)?;
        Ok(rows.into_iter().filter_map(LowRow::into_record).collect())
    }

    async fn find_by_game_id(&self, game_id: &str) -> Result<Vec<LowRecord>, LowStorageError> {
        let rows: Vec<LowRow> = Query::new(low_by_game_id_sql())
        .bind(game_id)
        .fetch_all(&self.db)
        .await
        .map_err(low_err)?;
        Ok(rows.into_iter().filter_map(LowRow::into_record).collect())
    }

    async fn find_by_host(&self, identity_key: &str) -> Result<Vec<LowRecord>, LowStorageError> {
        let rows: Vec<LowRow> = Query::new(low_by_host_sql())
        .bind(identity_key)
        .fetch_all(&self.db)
        .await
        .map_err(low_err)?;
        Ok(rows.into_iter().filter_map(LowRow::into_record).collect())
    }
}

// =============================================================================
// D1RevealStorage
// =============================================================================

/// `ls_reveal byGameSeat` — full index rows (#290), newest-first, capped at
/// [`REVEAL_RESULT_CAP`] (#291). Factored out so the real-SQLite tests
/// execute the SHIPPED string.
pub fn reveal_by_game_seat_sql() -> String {
    format!(
        "SELECT txid, outputIndex, gameId, seat FROM reveal_records \
         WHERE gameId = ? AND seat = ? ORDER BY createdAt DESC \
         LIMIT {REVEAL_RESULT_CAP}"
    )
}

/// `ls_reveal byGameId` — full index rows, newest-first, capped (#290/#291).
pub fn reveal_by_game_id_sql() -> String {
    format!(
        "SELECT txid, outputIndex, gameId, seat FROM reveal_records WHERE gameId = ? \
         ORDER BY createdAt DESC LIMIT {REVEAL_RESULT_CAP}"
    )
}

/// Row for full reveal_records queries. D1 returns numbers as f64.
#[derive(Deserialize)]
struct RevealRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "gameId")]
    game_id: String,
    seat: f64,
}

impl RevealRow {
    fn into_record(self) -> RevealRecord {
        RevealRecord {
            txid: self.txid,
            output_index: self.output_index as u32,
            game_id: self.game_id,
            seat: self.seat as u8,
        }
    }
}

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
    ) -> Result<Vec<RevealRecord>, RevealStorageError> {
        let rows: Vec<RevealRow> = Query::new(reveal_by_game_seat_sql())
        .bind(game_id)
        .bind(seat as u32)
        .fetch_all(&self.db)
        .await
        .map_err(reveal_err)?;
        Ok(rows.into_iter().map(RevealRow::into_record).collect())
    }

    async fn find_by_game_id(
        &self,
        game_id: &str,
    ) -> Result<Vec<RevealRecord>, RevealStorageError> {
        let rows: Vec<RevealRow> = Query::new(reveal_by_game_id_sql())
        .bind(game_id)
        .fetch_all(&self.db)
        .await
        .map_err(reveal_err)?;
        Ok(rows.into_iter().map(RevealRow::into_record).collect())
    }
}

// =============================================================================
// D1PotStorage
// =============================================================================

/// Row for pot-spend record queries. D1 returns numbers as f64 and a
/// nullable TEXT column as `Option<String>`. The #284 decoded columns are
/// all `serde(default)` so the narrow SELECTs (the spend-chaser scans, which
/// don't need them) and a read racing the additive migrations both
/// deserialize.
#[derive(Deserialize, Default)]
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
    #[serde(rename = "lockKind", default)]
    lock_kind: Option<String>,
    #[serde(rename = "pubA", default)]
    pub_a: Option<String>,
    #[serde(rename = "pubB", default)]
    pub_b: Option<String>,
    #[serde(rename = "pubTower", default)]
    pub_tower: Option<String>,
    #[serde(rename = "payPkhA", default)]
    pay_pkh_a: Option<String>,
    #[serde(rename = "payPkhB", default)]
    pay_pkh_b: Option<String>,
    #[serde(rename = "rakePkh", default)]
    rake_pkh: Option<String>,
    #[serde(rename = "stakeA", default)]
    stake_a: Option<f64>,
    #[serde(rename = "stakeB", default)]
    stake_b: Option<f64>,
    #[serde(rename = "feeSats", default)]
    fee_sats: Option<f64>,
    #[serde(rename = "recoveryHeight", default)]
    recovery_height: Option<f64>,
    #[serde(rename = "potSats", default)]
    pot_sats: Option<f64>,
    #[serde(rename = "paramsDecoded", default)]
    params_decoded: f64,
    #[serde(rename = "verdict", default)]
    verdict: Option<String>,
    #[serde(rename = "verdictTxid", default)]
    verdict_txid: Option<String>,
    #[serde(rename = "spentHeight", default)]
    spent_height: Option<f64>,
}

impl PotRow {
    fn into_record(self) -> PotRecord {
        PotRecord {
            txid: self.txid,
            output_index: self.output_index as u32,
            spent: self.spent != 0.0,
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed != 0.0,
            lock_kind: self.lock_kind,
            pub_a: self.pub_a,
            pub_b: self.pub_b,
            pub_tower: self.pub_tower,
            pay_pkh_a: self.pay_pkh_a,
            pay_pkh_b: self.pay_pkh_b,
            rake_pkh: self.rake_pkh,
            stake_a: self.stake_a.map(|v| v as u64),
            stake_b: self.stake_b.map(|v| v as u64),
            fee_sats: self.fee_sats.map(|v| v as u64),
            recovery_height: self.recovery_height.map(|v| v as u64),
            pot_sats: self.pot_sats.map(|v| v as u64),
            params_decoded: self.params_decoded != 0.0,
            verdict: self.verdict,
            verdict_txid: self.verdict_txid,
            spent_height: self.spent_height.map(|v| v as u64),
        }
    }
}

/// The full pot_records column list (#284) for reads that need the decoded
/// fields (`get_spent_status`, the backfill candidate scan).
const POT_RECORD_COLUMNS: &str = "txid, outputIndex, spent, spendingTxid, spentConfirmed, \
     lockKind, pubA, pubB, pubTower, payPkhA, payPkhB, rakePkh, \
     stakeA, stakeB, feeSats, recoveryHeight, potSats, paramsDecoded, \
     verdict, verdictTxid, spentHeight";

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
/// doc). All four variants are UPDATE-only (nonexistent outpoint = 0 rows
/// touched) and never DELETE:
///
/// - confirmed: always writes and latches `spentConfirmed = 1`
///   (last-confirmed-wins). ONLY this branch touches `spentHeight` (a fact
///   of the verified BUMP), and the height RIDES THE POINTER exactly like
///   the verdict does (gate finding LOW-1, 2026-07-28):
///   `spentHeight = CASE WHEN spendingTxid = ?new THEN COALESCE(?h,
///   spentHeight) ELSE ?h END` — a re-confirm of the SAME pointer keeps the
///   stored height when the caller has none (COALESCE), but a write that
///   CHANGES the pointer RESETS the height to the incoming value (including
///   NULL: an unparseable-bump S2 must never inherit S1's height and serve
///   it as its own `at.height`). Column refs in SET expressions are the
///   PRE-update values, so `spendingTxid` here is the stored pointer.
/// - unconfirmed: the `AND spentConfirmed = 0` guard makes an unconfirmed
///   claim a no-op against a confirmed pointer, while preserving
///   last-writer-wins among unconfirmed claims; `spentConfirmed` untouched,
///   and `spentHeight` untouched too — safe, because a height is only ever
///   written by a confirmed write, which latches the flag, after which no
///   unconfirmed write can be accepted (so an accepted unconfirmed write
///   always meets `spentHeight IS NULL`).
///
/// # #284 verdict atomicity (`with_verdict`)
///
/// `with_verdict = true` adds `verdict = ?, verdictTxid = ?` to the SAME
/// statement — verdictTxid is bound to the spending txid, so the verdict can
/// never point at a different spender than the pointer it rode in with, and
/// the unconfirmed guard covers it identically (an unconfirmed writer can
/// never displace a confirmed pointer's verdict). `with_verdict = false`
/// leaves BOTH columns entirely out of the SET (explicitly UNCHANGED — a
/// confirm-only caller with no spender raw must not null a stored verdict);
/// a pointer change under `false` deliberately leaves a stale verdict
/// behind, neutralized by the reader's `verdictTxid == spendingTxid` check.
///
/// Bind order: `spendingTxid, [verdict, verdictTxid,] [confirmed only:
/// spendingTxid, spentHeight, spentHeight,] txid, outputIndex`.
///
/// Both branches stamp `spentAt = unixepoch()` (#228 backstop age anchor):
/// every ACCEPTED spend write resets the age, so the poll chaser's gate
/// measures from the CURRENT spend pointer (its push gets its chance first).
/// A refused unconfirmed-vs-confirmed write touches nothing (WHERE misses).
pub fn mark_spent_sql(confirmed: bool, with_verdict: bool) -> &'static str {
    match (confirmed, with_verdict) {
        (true, true) => {
            "UPDATE pot_records SET spent = 1, spendingTxid = ?, spentConfirmed = 1, \
                 spentAt = unixepoch(), verdict = ?, verdictTxid = ?, \
                 spentHeight = CASE WHEN spendingTxid = ? \
                               THEN COALESCE(?, spentHeight) ELSE ? END \
             WHERE txid = ? AND outputIndex = ?"
        }
        (true, false) => {
            "UPDATE pot_records SET spent = 1, spendingTxid = ?, spentConfirmed = 1, \
                 spentAt = unixepoch(), \
                 spentHeight = CASE WHEN spendingTxid = ? \
                               THEN COALESCE(?, spentHeight) ELSE ? END \
             WHERE txid = ? AND outputIndex = ?"
        }
        (false, true) => {
            "UPDATE pot_records SET spent = 1, spendingTxid = ?, spentAt = unixepoch(), \
                 verdict = ?, verdictTxid = ? \
             WHERE txid = ? AND outputIndex = ? AND spentConfirmed = 0"
        }
        (false, false) => {
            "UPDATE pot_records SET spent = 1, spendingTxid = ?, spentAt = unixepoch() \
             WHERE txid = ? AND outputIndex = ? AND spentConfirmed = 0"
        }
    }
}

/// The #284 backfill's verdict write (gate finding MEDIUM-2, 2026-07-28): a
/// GUARDED COMPARE-AND-SET that attaches a verdict to the pointer it was
/// computed for — and touches NOTHING else. `WHERE … AND spendingTxid = ?`
/// makes a stale candidate-read harmless: if the pointer moved (a
/// reorg-confirmed S2 landing between the backfill's read and write), the
/// WHERE misses and the write is a NO-OP — the backfill can never displace a
/// newer pointer, never flip `spentConfirmed`, never reset the #228
/// `spentAt` age anchor, and never attach a verdict to a spender it was not
/// computed from (verdictTxid is bound to the same guarded pointer).
///
/// Bind order: `verdict, spendingTxid (verdictTxid), txid, outputIndex,
/// spendingTxid (guard)`.
/// SQL for one batched spent-status chunk (bsv-low #289): a single
/// row-value `IN (VALUES …)` query replacing `n` individual
/// `get_spent_status` round trips. Factored out so the real-SQLite test
/// proves the syntax against the production schema.
pub fn pot_spent_statuses_sql(n: usize) -> String {
    let placeholders = vec!["(?, ?)"; n].join(", ");
    format!(
        "SELECT {POT_RECORD_COLUMNS} FROM pot_records \
         WHERE (txid, outputIndex) IN (VALUES {placeholders})"
    )
}

pub fn verdict_cas_sql() -> &'static str {
    "UPDATE pot_records SET verdict = ?, verdictTxid = ? \
     WHERE txid = ? AND outputIndex = ? AND spendingTxid = ?"
}

/// The #284 `store_record` upsert: insert-if-absent for the SPEND fields,
/// STORED-WINS fill for the DECODED columns. The conflict update touches
/// ONLY decoded columns — never `spent` / `spendingTxid` / `spentConfirmed`
/// / `spentAt` / `verdict` / `verdictTxid` / `spentHeight` / `createdAt`
/// (re-admission must never regress spend state).
///
/// STORED-WINS argument order (gate finding MEDIUM-1, 2026-07-28):
/// `X = COALESCE(X, excluded.X)` — an incoming value only ever FILLS an
/// absent stored one; it can never overwrite. The first ship had the
/// arguments reversed (incoming-wins), which let a later store with a
/// DIFFERENT value — including an empty string, which is not NULL —
/// silently replace an already-decoded param. Stored-wins matches the trait
/// doc, `MemoryPotStorage`, and least privilege: a decoded param is a pure
/// function of the admitted lock bytes, so the first decode is as good as
/// any and nothing may rewrite it. `paramsDecoded` only latches 0 → 1.
pub fn store_record_sql() -> &'static str {
    "INSERT INTO pot_records \
         (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt, \
          lockKind, pubA, pubB, pubTower, payPkhA, payPkhB, rakePkh, \
          stakeA, stakeB, feeSats, recoveryHeight, potSats, paramsDecoded) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
     ON CONFLICT(txid, outputIndex) DO UPDATE SET \
         lockKind = COALESCE(lockKind, excluded.lockKind), \
         pubA = COALESCE(pubA, excluded.pubA), \
         pubB = COALESCE(pubB, excluded.pubB), \
         pubTower = COALESCE(pubTower, excluded.pubTower), \
         payPkhA = COALESCE(payPkhA, excluded.payPkhA), \
         payPkhB = COALESCE(payPkhB, excluded.payPkhB), \
         rakePkh = COALESCE(rakePkh, excluded.rakePkh), \
         stakeA = COALESCE(stakeA, excluded.stakeA), \
         stakeB = COALESCE(stakeB, excluded.stakeB), \
         feeSats = COALESCE(feeSats, excluded.feeSats), \
         recoveryHeight = COALESCE(recoveryHeight, excluded.recoveryHeight), \
         potSats = COALESCE(potSats, excluded.potSats), \
         paramsDecoded = CASE WHEN excluded.paramsDecoded = 1 THEN 1 ELSE paramsDecoded END"
}

#[async_trait(?Send)]
impl PotStorage for D1PotStorage {
    async fn store_record(&self, record: &PotRecord) -> Result<(), PotStorageError> {
        // #284 upsert: insert-if-absent for SPEND state, COALESCE backfill
        // for the decoded columns (see `store_record_sql` for the contract:
        // the conflict update never touches spend state and never nulls).
        Query::new(store_record_sql())
            .bind(record.txid.as_str())
            .bind(record.output_index)
            .bind(if record.spent { 1u32 } else { 0u32 })
            .bind(record.spending_txid.as_deref())
            .bind(if record.spent_confirmed { 1u32 } else { 0u32 })
            .bind(current_unix_seconds_i64())
            .bind(record.lock_kind.as_deref())
            .bind(record.pub_a.as_deref())
            .bind(record.pub_b.as_deref())
            .bind(record.pub_tower.as_deref())
            .bind(record.pay_pkh_a.as_deref())
            .bind(record.pay_pkh_b.as_deref())
            .bind(record.rake_pkh.as_deref())
            .bind(record.stake_a)
            .bind(record.stake_b)
            .bind(record.fee_sats)
            .bind(record.recovery_height)
            .bind(record.pot_sats)
            .bind(if record.params_decoded { 1u32 } else { 0u32 })
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
        verdict: Option<&str>,
        spent_height: Option<u64>,
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
        //
        // #284: a Some(verdict) rides the SAME statement as the pointer
        // (verdictTxid bound to the spending txid — atomic); None leaves
        // verdict/verdictTxid entirely out of the SET. spentHeight (the
        // confirmed branch only) RIDES THE POINTER: same-pointer re-confirm
        // keeps-or-updates (COALESCE), a pointer change resets it to the
        // incoming value (gate LOW-1) — see mark_spent_sql for the CASE.
        let mut q = Query::new(mark_spent_sql(confirmed, verdict.is_some())).bind(spending_txid);
        if let Some(v) = verdict {
            q = q.bind(v).bind(spending_txid);
        }
        if confirmed {
            // CASE binds: the same-pointer probe, the COALESCE height, the
            // ELSE (pointer-changed) height.
            q = q.bind(spending_txid).bind(spent_height).bind(spent_height);
        }
        q.bind(txid)
            .bind(output_index)
            .execute(&self.db)
            .await
            .map_err(pot_err)
    }

    async fn mark_verdict_for_spender(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        verdict: &str,
    ) -> Result<(), PotStorageError> {
        // The backfill's CAS verdict write (gate MEDIUM-2): verdict +
        // verdictTxid only, guarded on the pointer it was computed for. A
        // moved pointer ⇒ WHERE misses ⇒ no-op (see `verdict_cas_sql`).
        Query::new(verdict_cas_sql())
            .bind(verdict)
            .bind(spending_txid)
            .bind(txid)
            .bind(output_index)
            .bind(spending_txid)
            .execute(&self.db)
            .await
            .map_err(pot_err)
    }

    async fn get_spent_status(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Option<PotRecord>, PotStorageError> {
        let row: Option<PotRow> = Query::new(format!(
            "SELECT {POT_RECORD_COLUMNS} FROM pot_records WHERE txid = ? AND outputIndex = ?"
        ))
        .bind(txid)
        .bind(output_index)
        .fetch_optional(&self.db)
        .await
        .map_err(pot_err)?;
        Ok(row.map(PotRow::into_record))
    }

    /// Batched spent-status (bsv-low #289): one row-value `IN (VALUES …)`
    /// query per 40-outpoint chunk instead of one D1 round trip per
    /// outpoint. Alignment contract (input order, `None` where absent) is
    /// preserved via an outpoint-keyed map.
    async fn get_spent_statuses(
        &self,
        outpoints: &[(String, u32)],
    ) -> Result<Vec<Option<PotRecord>>, PotStorageError> {
        if outpoints.is_empty() {
            return Ok(Vec::new());
        }
        // D1 caps bound parameters (100); 2 per outpoint → chunks of 40.
        const CHUNK: usize = 40;
        let mut by_outpoint: std::collections::HashMap<(String, u32), PotRecord> =
            std::collections::HashMap::new();
        for chunk in outpoints.chunks(CHUNK) {
            let sql = pot_spent_statuses_sql(chunk.len());
            let mut q = Query::new(sql);
            for (txid, output_index) in chunk {
                q = q.bind(txid.as_str()).bind(*output_index);
            }
            let rows: Vec<PotRow> = q.fetch_all(&self.db).await.map_err(pot_err)?;
            for row in rows {
                let record = row.into_record();
                by_outpoint.insert((record.txid.clone(), record.output_index), record);
            }
        }
        Ok(outpoints
            .iter()
            .map(|(txid, output_index)| {
                by_outpoint.get(&(txid.clone(), *output_index)).cloned()
            })
            .collect())
    }

    async fn find_params_undecoded(&self, limit: u64) -> Result<Vec<PotRecord>, PotStorageError> {
        // #284 backfill candidates: rows whose decode was never attempted,
        // RANDOM-sampled (the proof-check anti-starvation idiom — a row with
        // a permanently missing funding BEEF must not starve the tail).
        // Backed by idx_pot_params_undecoded.
        let sql = format!(
            "SELECT {POT_RECORD_COLUMNS} FROM pot_records \
             WHERE paramsDecoded = 0 ORDER BY RANDOM() LIMIT {limit}"
        );
        let rows: Vec<PotRow> = Query::new(sql).fetch_all(&self.db).await.map_err(pot_err)?;
        Ok(rows.into_iter().map(PotRow::into_record).collect())
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

/// SQL for one batched collected-marker chunk (bsv-low #289): one
/// `identity = ? AND gameId IN (…)` query replacing `n` individual
/// `get_record` round trips. Factored out so the real-SQLite test proves
/// the SHIPPED string selects per-(identity, gameId) — never a same-gameId
/// row belonging to a DIFFERENT identity.
pub fn collected_records_batch_sql(n: usize) -> String {
    let placeholders = vec!["?"; n].join(", ");
    format!(
        "SELECT identity, gameId, txid, sigHex FROM collected_markers \
         WHERE identity = ? AND gameId IN ({placeholders})"
    )
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

    /// Batched pair lookup (bsv-low #289): one `gameId IN (…)` query per
    /// chunk instead of a D1 round trip per requested game. Alignment
    /// contract (input order, `None` where no marker exists) preserved via
    /// a gameId-keyed map — (identity, gameId) is the primary key, so at
    /// most one row exists per requested gameId.
    async fn get_records(
        &self,
        identity: &str,
        game_ids: &[String],
    ) -> Result<Vec<Option<CollectedRecord>>, CollectedStorageError> {
        if game_ids.is_empty() {
            return Ok(Vec::new());
        }
        // D1 caps bound parameters (100); 1 per gameId + the identity.
        const CHUNK: usize = 90;
        let mut by_game: std::collections::HashMap<String, CollectedRecord> =
            std::collections::HashMap::new();
        for chunk in game_ids.chunks(CHUNK) {
            let sql = collected_records_batch_sql(chunk.len());
            let mut q = Query::new(sql).bind(identity);
            for game_id in chunk {
                q = q.bind(game_id.as_str());
            }
            let rows: Vec<CollectedRow> = q.fetch_all(&self.db).await.map_err(collected_err)?;
            for row in rows {
                let record = row.into_record();
                by_game.insert(record.game_id.clone(), record);
            }
        }
        Ok(game_ids
            .iter()
            .map(|game_id| by_game.get(game_id).cloned())
            .collect())
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
/// # A SUPERSET per pot, never one row — the client depends on it
///
/// `limit` counts POTS, and each pot yields up to
/// [`PARTYFOR_ROWS_PER_GROUP`] rows in EACH of two groups, partitioned by
/// `(potTxid, potVout, has-seat-key)`. Collapsing to one row per pot — or one
/// per group — is WRONG here, in three ways that each cost the honest player
/// money (the third is the architectural one, below):
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
/// # ARCHITECTURE — verification before collapse
///
/// (2026-07-28 owner steer; the principle this whole family of bugs comes
/// from, stated here because it generalises.)
///
/// **A layer that cannot verify signatures must never choose which row is
/// real.** SQL cannot verify a signature. So any ordering heuristic it uses to
/// pick "the honest row" — newest, oldest, v2-preferred — is guessable and
/// FORGEABLE by an attacker who controls `createdAt` and can file rows for
/// dust. There is no sort order that fixes this. An earlier revision of this
/// window returned `rn = 1` per group and defended it with "the honest seat
/// publishes at funding, before an attacker can know the pot txid". **That
/// claim was FALSE and has been removed**: `potPartyPending.ts` completes the
/// publish on a LATER visit and backfills HISTORICAL pots, so an attacker has
/// had the pot txid for weeks and can always land an EARLIER `createdAt`. One
/// forged marker then evicted the honest row server-side, `lookupPotParty`
/// dropped it client-side for failing its signature check, and the pot
/// vanished from recovery — strictly worse than doing nothing.
///
/// So this window no longer decides anything. It returns a BOUNDED SUPERSET —
/// up to [`PARTYFOR_ROWS_PER_GROUP`] rows per `(potTxid, potVout, group)` —
/// and the CONSUMER collapses AFTER verifying. `lookupPotParty` already
/// verifies both v2 signatures and discards failures; handed a superset it now
/// has the honest row left to keep. The contract is: **the server guarantees
/// the answer CONTAINS the honest row; the client decides which row is real.**
///
/// Where the caller already holds VERIFIED key material, the right answer is
/// stronger still — BIND it instead of ordering, so a forged row cannot enter
/// the result set at all. `low-app-layer`'s `seat_markers_sql` does exactly
/// that with each pot's COMMITTED settle keys. `partyFor` cannot: it IS the
/// discovery query, so its caller does not yet know which pots — let alone
/// which keys — to bind. Superset-plus-verify is the fallback for that case
/// only.
///
/// # The bounds, all deterministic
///
///  1. **Per-POT-OUTPOINT windowing** (within each of the v1 / v2 groups) —
///     `limit` counts POTS, not rows, so one pot can never consume the window
///     however many markers name it, and the zero-forgery replay variant dies
///     outright. Partitioning on the full OUTPOINT (not just the txid) matches
///     every other key in the system. Within a group the OLDEST rows are kept
///     — not because oldest is trustworthy (it is not, see above) but because
///     it is the only order an attacker cannot improve on by publishing MORE
///     later; it bounds cost without pretending to pick truth.
///  2. **Existence tier with a RESERVED QUOTA** — a row whose pot outpoint is
///     absent from `pot_records` normally sorts behind every row whose pot
///     exists, so markers naming INVENTED pots (free, unlimited, each its own
///     partition) cannot displace a real one. A STRICT tier, though, silently
///     becomes a FILTER once `limit` binds — a genuinely fresh pot whose
///     `tm_pot` admission is still in flight would fall off the page, which
///     is exactly the pot a recovering client most needs. So the newest
///     `quota` unknown pots are PROMOTED into the main tier and compete on
///     recency; the rest stay demoted but are still served.
///  3. Ordering is by the POT's own admission stamp (`pot_records.createdAt`
///     — an attacker cannot backdate or advance it by filing markers), then
///     the marker stamp, then `rowid` as a total order. EVERY level carries
///     an explicit `ORDER BY`.
///
/// BINDS (numbered): `?1` identity, `?2` limit (POTS), `?3` quota
/// (unknown-pot promotion slots), `?4` row cap. Rows are bounded by
/// `limit × 2 groups × PARTYFOR_ROWS_PER_GROUP`, and the cap is a BELT — the
/// filters already guarantee it, so it can never cut a pot in half.
///
/// # Residual — stated plainly, because the improvement is modest
///
/// This does NOT make the window expensive to fill. An attacker who copies
/// `limit` REAL, recently-admitted pot txids out of the very index being
/// queried — they are public — and files one marker per pot naming the victim
/// still displaces the victim's pots at the same ~`limit`-dust cost as before.
/// The honest net gain is **from "any N junk rows" to "N junk rows naming
/// real, recent pot txids"**, plus the outright death of the zero-forgery
/// replay variant and of free invented-pot flooding. And eviction WITHIN a
/// pot now costs [`PARTYFOR_ROWS_PER_GROUP`] markers per group instead of one
/// — a MITIGATION, not a closure: file one more than that and the honest row
/// is evicted again. See [`PARTYFOR_ROWS_PER_GROUP`] for the measured size,
/// and for the only two things that would actually close it (binding verified
/// key material, which discovery has none of; or making admission cost
/// something, which is an owner decision about the byte-format-only
/// doctrine).
pub fn potparty_list_for_identity_sql() -> String {
    // NUMBERED parameters (?1 identity, ?2 limit, ?3 quota, ?4 row_cap): the
    // quota appears twice and the identity sits in the INNERMOST subquery, so
    // anonymous `?` would bind in a textual order nobody could keep straight.
    format!(
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
                       WHERE rn <= {per_group}))) \
     WHERE finalRank <= ?2 \
     ORDER BY tier ASC, COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
              potTxid ASC, potVout ASC, isV2 DESC, markerRowid ASC \
     LIMIT ?4",
        per_group = PARTYFOR_ROWS_PER_GROUP,
    )
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
    format!(
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
                       WHERE rn <= {per_group}))) \
     WHERE finalRank <= ?2 \
     ORDER BY tier ASC, COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
              potTxid ASC, potVout ASC, markerRowid ASC \
     LIMIT ?4",
        per_group = PARTYFOR_ROWS_PER_GROUP,
    )
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
/// `OFFSET` (bsv-low #291 gate finding M2): the byPot window is now
/// PAGEABLE — each response stays payload-bounded (rows carry up to
/// ~200 KB of `refundRawHex` TEXT), yet no admitted row is unreachable: a
/// client that suspects burial pages `offset += limit` past junk to the
/// honest markers. The `(createdAt ASC, rowid ASC)` total order makes
/// offset pages stable — new markers only ever APPEND at the tail, so a
/// concurrent insert can never shift rows across an already-fetched page
/// boundary.
///
/// Built from the caller's own SELECT list so the tests execute the SHIPPED
/// string rather than a transcription of it.
pub fn list_for_pot_sql(select: &str) -> String {
    format!(
        "{select} WHERE potTxid = ? AND potVout = ? \
         ORDER BY createdAt ASC, rowid ASC LIMIT ? OFFSET ?"
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

/// Rows kept per `(potTxid, potVout, marker group)` by the identity-scoped
/// windows — the SUPERSET size (bsv-low #281, 2026-07-28 owner steer).
///
/// # Why this is not 1
///
/// **Verification must happen BEFORE collapse. A layer that cannot verify
/// signatures must never choose which row is real.** SQL cannot verify a
/// signature, so ANY ordering heuristic it uses to pick "the honest row" —
/// newest, oldest, v2-preferred — is guessable and forgeable by an attacker
/// who controls `createdAt` and can file rows for dust. There is no sort order
/// that fixes this; a per-group window of 1 is exactly wrong, because ONE
/// forged marker is then the whole attack.
///
/// The window's job is to BOUND COST, not to decide truth. The layer that CAN
/// verify already does: `lookupPotParty` (`overlay.ts`) checks both v2
/// signatures and DROPS a row that fails — given a superset it now has the
/// honest row left to keep. That is the CONTRACT this constant exists to
/// honour: the server hands the consumer a set that CONTAINS the honest row.
///
/// # This is a MITIGATION, not a closure — do not read it as airtight
///
/// An attacker who files `PARTYFOR_ROWS_PER_GROUP + 1` rows stamped earlier
/// than the honest one evicts it again. This raises the cost of erasing a pot
/// from ONE dust marker to four; it does NOT eliminate the erasure, and the
/// previous docblock's mistake — asserting a defence that did not hold — is
/// not to be repeated here. Two things would actually CLOSE it:
///
///  1. **Binding to verified key material.** Where the caller already holds a
///     verified key, bind it and a forged row cannot enter the result set at
///     all — no ordering, no window, no residual. That is why `/results`'
///     seat fetch (`low-app-layer`'s `seat_markers_sql`, bound to each pot's
///     COMMITTED settle keys from the hash-verified funding lock) is genuinely
///     closed and this query is not: `partyFor` IS the discovery query, so its
///     caller does not yet know which pots — let alone which keys — to bind.
///     There is nothing to bind here, which is exactly why it falls back to
///     superset-plus-verify.
///  2. **Making admission cost something** — rate limiting, or requiring a
///     verifiable signature before a marker is admitted. That cuts against
///     this overlay's byte-format-only doctrine (the INDEX is not an
///     authority; the READER verifies), so it is an OWNER decision about the
///     doctrine, not something to smuggle in behind a window size.
///
/// # Why 4 — chosen DEFENSIVELY
///
/// N is the number of forged rows an attacker must land ahead of yours to
/// evict it, per pot per group. It is picked for that threshold, NOT to dodge
/// a per-candidate cost:
///
///  - **N = 1 is the bug.** One forged marker, one dust transaction, and the
///    pot is erased. That is the attack the gate demonstrated.
///  - **N must clear honest traffic.** An honest client publishes one marker
///    per group and republishes (content-idempotent, new outpoint) whenever an
///    index read failed. N = 4 leaves three slots of headroom above a single
///    honest marker, so an honest client's own republishes can never crowd it
///    out either.
///  - **Beyond that the return is LINEAR and the attacker's cost is small in
///    absolute terms** — 4 dust transactions or 8, a determined attacker
///    targeting one pot pays either. That is precisely why this is a
///    mitigation and not a closure (above), and why the honest answer is to
///    name the real closures rather than keep buying linear increments.
///
/// SECONDARY CHECK — wire weight, measured against real SQLite over the
/// production schema with realistic field sizes (71-byte DER sigs, 258-byte
/// refund raws), serialising the record shape `ls_potparty` actually returns,
/// at the client's default `limit` of 100 pots with EVERY group filled:
///
/// | N | potparty rows | potparty bytes | potrefund rows | potrefund bytes |
/// |---|---------------|----------------|----------------|-----------------|
/// | 1 |           200 |       137 KiB  |            100 |         96 KiB  |
/// | 2 |           400 |       274 KiB  |            200 |        192 KiB  |
/// | **4** |       800 |   **548 KiB**  |            400 |    **384 KiB**  |
/// | 8 |          1600 |     1097 KiB   |            800 |        768 KiB  |
///
/// N = 8 puts the worst case at 1.07 MiB — at the practical response boundary,
/// reachable only under exactly the flood this defends against, so it would
/// trade erasure for a 503: the same denial by another name. Since the
/// defensive return from 4 to 8 is linear while the wire cost hits a cliff, 4
/// is the better point on that curve. Typical honest traffic is 1–2 rows per
/// group (~200 rows / ~140 KiB); the worst case is what an attacker must PAY
/// 800 dust transactions to produce. Pinned by
/// `worst_case_window_response_stays_in_budget`.
///
/// NOT A FACTOR, deliberately: per-candidate BLOB and classification cost.
/// NEITHER identity window joins `pot_beefs` — no covenant lock is re-parsed
/// here, so nothing about this constant scales that work. (`/results` and
/// `/leaderboard` DO re-`hex()` and re-decode both BEEFs per pot on every
/// read, recomputing an immutable answer — that is bsv-low #284, a separate
/// defect being moved to admission time, and it is why those surfaces keep
/// their BEEF joins on the OUTER select: run BLOB work on survivors, not
/// candidates. Once #284 lands, per-candidate cost drops out of this decision
/// entirely and N should be RE-EVALUATED UPWARD if the defensive case wants
/// it — re-measure the wire weight, which is the only bound that remains.)
pub const PARTYFOR_ROWS_PER_GROUP: usize = 4;

/// Row cap for an identity-scoped window: `limit` pots (the `finalRank`
/// filter), `groups` marker groups per pot, [`PARTYFOR_ROWS_PER_GROUP`] rows
/// per group. `groups` is 2 for potparty (a v1 and a v2 group) and 1 for
/// potrefund.
///
/// It is a BELT, never a truncation: the window's own filters already bound
/// the result to exactly this many rows, so the `LIMIT` can never cut a pot in
/// half (which would hand the client a pot's v1 rows without its v2 rows and
/// re-open the republish-forever fee leak).
pub fn identity_window_row_cap(limit: usize, groups: usize) -> usize {
    limit
        .saturating_mul(groups)
        .saturating_mul(PARTYFOR_ROWS_PER_GROUP)
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
            .bind(identity_window_row_cap(limit, 2) as u32)
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
        // OLDEST FIRST — see `list_for_pot_sql` (bsv-low #281). The shared
        // SQL is offset-pageable since gate M2; the potparty wire has no
        // offset (its rows are small — no payload-bound cap forcing one),
        // so this binds page 0.
        let rows: Vec<PotpartyRow> = Query::new(list_for_pot_sql(POTPARTY_SELECT))
            .bind(pot_txid)
            .bind(pot_vout)
            .bind(limit as u32)
            .bind(0u32)
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
            .bind(identity_window_row_cap(limit, 1) as u32)
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
        offset: usize,
    ) -> Result<Vec<PotrefundRecord>, PotrefundStorageError> {
        // OLDEST FIRST, offset-pageable — see `list_for_pot_sql`
        // (bsv-low #281 / #291 gate M2).
        let rows: Vec<PotrefundRow> = Query::new(list_for_pot_sql(POTREFUND_SELECT))
            .bind(pot_txid)
            .bind(pot_vout)
            .bind(limit as u32)
            .bind(offset as u32)
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
    /// Admission-time base64 of the bundle (bsv-low #289). NULL on rows
    /// admitted before the `bundleB64` column existed.
    #[serde(rename = "bundleB64")]
    bundle_b64: Option<String>,
    /// hex(bundle) — decoded in `into_record`. The SHIPPED select only
    /// hauls the blob when `bundleB64` is NULL (the pre-#289 fallback);
    /// otherwise this arrives as `''`.
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
            // hex(bundle) → bytes. Empty when bundleB64 answered instead
            // (the select skips hauling the blob then). Undecodable hex is
            // impossible (written from parse-validated bytes), but fail
            // toward an empty bundle (which no client verify ever accepts)
            // rather than a panic.
            bundle: hex::decode(&self.bundle).unwrap_or_default(),
            bundle_b64: self.bundle_b64,
            txid: self.txid,
            output_index: self.output_index as u32,
            created_at: self.created_at.unwrap_or(0.0) as i64,
        }
    }
}

/// `ls_proof proofsFor` (bsv-low #289): prefer the admission-time
/// `bundleB64` TEXT; haul `hex(bundle)` ONLY for pre-#289 rows where it is
/// NULL — never both. Factored out so the real-SQLite test executes the
/// SHIPPED string against the production schema and proves the two paths
/// answer byte-identically.
pub fn proof_list_for_game_winner_sql() -> &'static str {
    "SELECT gameId, winner, sigHex, bundleB64, \
            CASE WHEN bundleB64 IS NULL THEN hex(bundle) ELSE '' END AS bundle, \
            txid, outputIndex, createdAt \
     FROM proof_markers WHERE gameId = ? AND winner = ? \
     ORDER BY createdAt DESC, rowid DESC LIMIT ?"
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
             (gameId, winner, sigHex, bundle, bundleB64, txid, outputIndex, createdAt) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.game_id.as_str())
        .bind(record.winner.as_str())
        .bind(record.sig_hex.as_str())
        .bind(record.bundle.clone()) // BLOB bind, like pot_beefs
        // bsv-low #289: the admission-time base64 (the admit path always
        // sets it; a defensive None binds NULL → the read-time fallback).
        .bind(record.bundle_b64.as_deref())
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
        let rows: Vec<ProofRow> = Query::new(proof_list_for_game_winner_sql())
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
        // Pre-#289 row shape: bundleB64 NULL, hex(bundle) hauled and
        // decoded; numeric columns come back as f64 from D1.
        let row = ProofRow {
            game_id: "11".repeat(32),
            winner: "02aa".into(),
            sig_hex: Some("3045ab".into()),
            bundle_b64: None,
            bundle: hex::encode(b"{\"v\":1}").to_uppercase(), // SQLite hex() is uppercase
            txid: "tx1".into(),
            output_index: 2.0,
            created_at: Some(1_234.0),
        };
        let r = row.into_record();
        assert_eq!(r.bundle, b"{\"v\":1}");
        assert_eq!(r.bundle_b64, None, "legacy row: service falls back to encoding");
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
            ..Default::default()
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
            ..Default::default()
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
        for with_verdict in [false, true] {
            let sql = mark_spent_sql(true, with_verdict);
            // Chain truth: sets the pointer AND the flag…
            assert!(sql.contains("SET spent = 1, spendingTxid = ?, spentConfirmed = 1"));
            // …with no confirmation guard (last-confirmed-wins), UPDATE-only,
            // never DELETE.
            assert!(!sql.contains("spentConfirmed = 0"));
            assert!(sql.starts_with("UPDATE pot_records"));
            assert!(sql.contains("WHERE txid = ? AND outputIndex = ?"));
            assert!(!sql.to_uppercase().contains("DELETE"));
            // #284 + gate LOW-1: spentHeight rides ONLY the confirmed branch
            // AND rides the pointer — same-pointer keeps-or-updates
            // (COALESCE), a pointer change resets to the incoming value.
            assert!(sql.contains(
                "spentHeight = CASE WHEN spendingTxid = ? \
                               THEN COALESCE(?, spentHeight) ELSE ? END"
            ));
        }
    }

    #[test]
    fn mark_spent_sql_unconfirmed_guarded_and_never_touches_flag() {
        for with_verdict in [false, true] {
            let sql = mark_spent_sql(false, with_verdict);
            // The guard: an unconfirmed claim only lands while no confirmed
            // pointer exists (spentConfirmed = 0)…
            assert!(sql.contains("WHERE txid = ? AND outputIndex = ? AND spentConfirmed = 0"));
            // …and the SET clause never touches the flag (it DOES stamp
            // spentAt — the #228 backstop age anchor — on every accepted
            // write) nor spentHeight (a fact of a verified BUMP only).
            assert!(sql.contains("SET spent = 1, spendingTxid = ?, spentAt = unixepoch()"));
            assert!(!sql.contains("spentConfirmed = 1"));
            assert!(!sql.contains("spentHeight"));
            assert!(sql.starts_with("UPDATE pot_records"));
            assert!(!sql.to_uppercase().contains("DELETE"));
        }
    }

    #[test]
    fn mark_spent_sql_verdict_is_atomic_with_the_pointer_or_absent() {
        // #284: with_verdict adds BOTH columns to the same statement (the
        // verdict can never point at a different spender than the pointer it
        // rode in with)…
        for confirmed in [false, true] {
            let sql = mark_spent_sql(confirmed, true);
            assert!(sql.contains("verdict = ?, verdictTxid = ?"));
        }
        // …and WITHOUT a verdict the SET must not mention either column at
        // all (explicitly UNCHANGED — a confirm-only caller must never null
        // a stored verdict).
        for confirmed in [false, true] {
            let sql = mark_spent_sql(confirmed, false);
            assert!(!sql.contains("verdict"));
            assert!(!sql.contains("verdictTxid"));
        }
    }

    // ── #284 store/mark SQL EXECUTED against the production schema ────────
    // String pins are a backstop only (the #230 gate lesson); the contract —
    // re-admission never regresses spend state, verdict atomic with the
    // pointer, unconfirmed can never displace a confirmed verdict — is
    // proven by RUNNING the exact shipped SQL under real SQLite over the
    // exact shipped migrations.

    /// One pot_records row snapshot for the #284 SQL tests.
    #[derive(Debug, PartialEq)]
    struct SqlPotRow {
        spent: i64,
        spending_txid: Option<String>,
        spent_confirmed: i64,
        lock_kind: Option<String>,
        pub_a: Option<String>,
        stake_a: Option<i64>,
        pot_sats: Option<i64>,
        params_decoded: i64,
        verdict: Option<String>,
        verdict_txid: Option<String>,
        spent_height: Option<i64>,
        created_at: Option<i64>,
    }

    fn read_pot_row(conn: &rusqlite::Connection, txid: &str, vout: u32) -> SqlPotRow {
        conn.query_row(
            "SELECT spent, spendingTxid, spentConfirmed, lockKind, pubA, stakeA, potSats, \
                    paramsDecoded, verdict, verdictTxid, spentHeight, createdAt \
             FROM pot_records WHERE txid = ?1 AND outputIndex = ?2",
            rusqlite::params![txid, vout],
            |r| {
                Ok(SqlPotRow {
                    spent: r.get(0)?,
                    spending_txid: r.get(1)?,
                    spent_confirmed: r.get(2)?,
                    lock_kind: r.get(3)?,
                    pub_a: r.get(4)?,
                    stake_a: r.get(5)?,
                    pot_sats: r.get(6)?,
                    params_decoded: r.get(7)?,
                    verdict: r.get(8)?,
                    verdict_txid: r.get(9)?,
                    spent_height: r.get(10)?,
                    created_at: r.get(11)?,
                })
            },
        )
        .expect("pot row present")
    }

    /// Execute the shipped `store_record_sql()` with the given decoded
    /// column values (spend fields as a fresh admission: unspent).
    #[allow(clippy::too_many_arguments)]
    fn exec_store(
        conn: &rusqlite::Connection,
        txid: &str,
        vout: u32,
        created_at: i64,
        lock_kind: Option<&str>,
        pub_a: Option<&str>,
        stake_a: Option<i64>,
        pot_sats: Option<i64>,
        params_decoded: i64,
    ) {
        conn.execute(
            store_record_sql(),
            rusqlite::params![
                txid, vout, 0i64, Option::<String>::None, 0i64, created_at, lock_kind, pub_a,
                pub_a, pub_a, // pubB / pubTower ride the same fixture value
                Option::<String>::None,
                Option::<String>::None,
                Option::<String>::None,
                stake_a,
                stake_a, // stakeB
                Option::<i64>::None,
                Option::<i64>::None,
                pot_sats,
                params_decoded
            ],
        )
        .expect("store_record_sql executes");
    }

    /// Execute the shipped `mark_spent_sql(...)` with the D1 impl's exact
    /// bind order (spendingTxid, [verdict, verdictTxid,] [confirmed only:
    /// spendingTxid, spentHeight, spentHeight,] txid, outputIndex).
    fn exec_mark_spent(
        conn: &rusqlite::Connection,
        txid: &str,
        vout: u32,
        spending_txid: &str,
        confirmed: bool,
        verdict: Option<&str>,
        spent_height: Option<i64>,
    ) {
        let sql = mark_spent_sql(confirmed, verdict.is_some());
        match (confirmed, verdict) {
            (true, Some(v)) => conn.execute(
                sql,
                rusqlite::params![
                    spending_txid,
                    v,
                    spending_txid,
                    spending_txid,
                    spent_height,
                    spent_height,
                    txid,
                    vout
                ],
            ),
            (true, None) => conn.execute(
                sql,
                rusqlite::params![
                    spending_txid,
                    spending_txid,
                    spent_height,
                    spent_height,
                    txid,
                    vout
                ],
            ),
            (false, Some(v)) => conn.execute(
                sql,
                rusqlite::params![spending_txid, v, spending_txid, txid, vout],
            ),
            (false, None) => {
                conn.execute(sql, rusqlite::params![spending_txid, txid, vout])
            }
        }
        .expect("mark_spent_sql executes");
    }

    /// Execute the shipped `verdict_cas_sql()` (the backfill's guarded
    /// verdict write) with the D1 impl's exact bind order.
    fn exec_verdict_cas(
        conn: &rusqlite::Connection,
        txid: &str,
        vout: u32,
        spending_txid: &str,
        verdict: &str,
    ) {
        conn.execute(
            verdict_cas_sql(),
            rusqlite::params![verdict, spending_txid, txid, vout, spending_txid],
        )
        .expect("verdict_cas_sql executes");
    }

    #[test]
    fn sql_re_admission_backfills_decoded_columns_but_never_regresses_spend_state() {
        let conn = production_schema_db();
        // Pre-#284 admission (no decoded values), then a CONFIRMED spend
        // with a verdict + height.
        exec_store(&conn, "potA", 0, 1_000, None, None, None, None, 0);
        exec_mark_spent(&conn, "potA", 0, "settleTx", true, Some("winner-a"), Some(800_000));
        let before = read_pot_row(&conn, "potA", 0);
        assert_eq!(before.spent, 1);
        assert_eq!(before.verdict.as_deref(), Some("winner-a"));

        // Re-admission / backfill upsert with decoded columns (a LATER
        // createdAt bind, which must NOT overwrite the original stamp).
        exec_store(
            &conn,
            "potA",
            0,
            9_999,
            Some("covenant"),
            Some(&"02".repeat(33)),
            Some(2000),
            Some(4000),
            1,
        );
        let after = read_pot_row(&conn, "potA", 0);
        // Spend state + createdAt byte-identical…
        assert_eq!(after.spent, before.spent);
        assert_eq!(after.spending_txid, before.spending_txid);
        assert_eq!(after.spent_confirmed, before.spent_confirmed);
        assert_eq!(after.verdict, before.verdict);
        assert_eq!(after.verdict_txid, before.verdict_txid);
        assert_eq!(after.spent_height, before.spent_height);
        assert_eq!(after.created_at, Some(1_000), "createdAt never re-stamped");
        // …decoded columns filled + latched.
        assert_eq!(after.lock_kind.as_deref(), Some("covenant"));
        assert_eq!(after.stake_a, Some(2000));
        assert_eq!(after.pot_sats, Some(4000));
        assert_eq!(after.params_decoded, 1);

        // A NULL-bearing replay never nulls, never un-latches.
        exec_store(&conn, "potA", 0, 8_888, None, None, None, None, 0);
        assert_eq!(read_pot_row(&conn, "potA", 0), after);
    }

    #[test]
    fn sql_unconfirmed_writer_cannot_displace_a_confirmed_verdict() {
        let conn = production_schema_db();
        exec_store(&conn, "potA", 0, 1_000, None, None, None, None, 0);
        exec_mark_spent(&conn, "potA", 0, "realSettle", true, Some("winner-a"), Some(800_000));

        // The attacker's unconfirmed claim — with its own forged verdict.
        exec_mark_spent(&conn, "potA", 0, "forgedSpend", false, Some("winner-b"), None);
        let r = read_pot_row(&conn, "potA", 0);
        assert_eq!(r.spending_txid.as_deref(), Some("realSettle"));
        assert_eq!(r.verdict.as_deref(), Some("winner-a"));
        assert_eq!(r.verdict_txid.as_deref(), Some("realSettle"));
        assert_eq!(r.spent_height, Some(800_000));
        assert_eq!(r.spent_confirmed, 1);
    }

    #[test]
    fn sql_confirm_only_write_keeps_the_stored_verdict_and_stamps_height() {
        let conn = production_schema_db();
        exec_store(&conn, "potA", 0, 1_000, None, None, None, None, 0);
        // 0-conf spend with a verdict (the lookup-service shape)…
        exec_mark_spent(&conn, "potA", 0, "settleTx", false, Some("tie"), None);
        // …then the chaser's confirm-only latch (no spender raw in hand).
        exec_mark_spent(&conn, "potA", 0, "settleTx", true, None, Some(801_234));
        let r = read_pot_row(&conn, "potA", 0);
        assert_eq!(r.spent_confirmed, 1);
        assert_eq!(r.verdict.as_deref(), Some("tie"), "None leaves the verdict");
        assert_eq!(r.verdict_txid.as_deref(), Some("settleTx"));
        assert_eq!(r.spent_height, Some(801_234));
        // A later confirm with a NULL height keeps the stored one (COALESCE
        // — SAME pointer, so the height survives).
        exec_mark_spent(&conn, "potA", 0, "settleTx", true, None, None);
        assert_eq!(read_pot_row(&conn, "potA", 0).spent_height, Some(801_234));
    }

    // ── 2026-07-28 gate findings, executed against the production schema ──

    /// MEDIUM-1: the upsert is STORED-WINS. An incoming Some over a
    /// DIFFERENT stored Some must NOT overwrite (the probe that caught the
    /// reversed COALESCE: incoming stakeA=999 overwrote stored 1000), and an
    /// incoming EMPTY STRING — which is not NULL, so COALESCE alone never
    /// filters it — must not overwrite a stored key either.
    #[test]
    fn sql_upsert_is_stored_wins_never_incoming_wins() {
        let conn = production_schema_db();
        let good_key = "02".repeat(33);
        exec_store(
            &conn,
            "potA",
            0,
            1_000,
            Some("covenant"),
            Some(&good_key),
            Some(1000),
            Some(2000),
            1,
        );

        // The overwrite attempts: a different stake, an empty-string key.
        exec_store(&conn, "potA", 0, 2_000, Some("covenant"), Some(""), Some(999), Some(4), 1);
        let r = read_pot_row(&conn, "potA", 0);
        assert_eq!(r.stake_a, Some(1000), "stored 1000 survives incoming 999");
        assert_eq!(
            r.pub_a.as_deref(),
            Some(good_key.as_str()),
            "incoming '' (not NULL!) never displaces a stored key"
        );
        assert_eq!(r.pot_sats, Some(2000));

        // …and the fill direction still works: a stored NULL takes the
        // incoming value (the whole point of the backfill upsert).
        exec_store(&conn, "potB", 0, 1_000, None, None, None, None, 0);
        exec_store(
            &conn,
            "potB",
            0,
            2_000,
            Some("covenant"),
            Some(&good_key),
            Some(1000),
            Some(2000),
            1,
        );
        let r = read_pot_row(&conn, "potB", 0);
        assert_eq!(r.stake_a, Some(1000));
        assert_eq!(r.pub_a.as_deref(), Some(good_key.as_str()));
        assert_eq!(r.params_decoded, 1);
    }

    /// MEDIUM-2: the backfill's verdict write is a guarded CAS — if the
    /// spend pointer MOVED between the backfill's candidate read and its
    /// write (a reorg-confirmed S2 landing in the window), the write bound
    /// to the stale S1 is a NO-OP: S2's pointer, confirmed flag, height and
    /// spentAt all survive untouched, and no verdict is attached to a
    /// spender it was not computed from.
    #[test]
    fn sql_backfill_verdict_cas_is_a_noop_when_the_pointer_moved() {
        let conn = production_schema_db();
        exec_store(&conn, "potA", 0, 1_000, None, None, None, None, 0);
        // The backfill "reads" the row while it points at S1 (unconfirmed).
        exec_mark_spent(&conn, "potA", 0, "settleS1", false, None, None);
        // …then, before its write lands, a reorg-CONFIRMED S2 displaces S1.
        exec_mark_spent(&conn, "potA", 0, "settleS2", true, None, Some(802_000));
        let before = read_pot_row(&conn, "potA", 0);

        // The backfill's write, still bound to the stale S1 → NO-OP.
        exec_verdict_cas(&conn, "potA", 0, "settleS1", "winner-a");
        let after = read_pot_row(&conn, "potA", 0);
        assert_eq!(after, before, "a stale CAS write changes NOTHING");
        assert_eq!(after.spending_txid.as_deref(), Some("settleS2"));
        assert_eq!(after.verdict, None);
        assert_eq!(after.spent_confirmed, 1);
        assert_eq!(after.spent_height, Some(802_000));

        // The CURRENT-pointer write DOES land (the non-race case) and
        // touches only the verdict pair.
        exec_verdict_cas(&conn, "potA", 0, "settleS2", "winner-b");
        let r = read_pot_row(&conn, "potA", 0);
        assert_eq!(r.verdict.as_deref(), Some("winner-b"));
        assert_eq!(r.verdict_txid.as_deref(), Some("settleS2"));
        assert_eq!(r.spent_height, Some(802_000), "nothing else touched");
    }

    /// LOW-1 (the exact gate probe): confirmed S1 with height 800000, then a
    /// confirmed S2 whose bump yielded NO height (bind NULL) — S2 must NOT
    /// inherit S1's height (`at.height` would have served S1's height as
    /// S2's). The height rides the pointer: a pointer change RESETS it.
    #[test]
    fn sql_spent_height_rides_the_pointer_never_inherited_across_spenders() {
        let conn = production_schema_db();
        exec_store(&conn, "potA", 0, 1_000, None, None, None, None, 0);
        exec_mark_spent(&conn, "potA", 0, "settleS1", true, None, Some(800_000));
        assert_eq!(read_pot_row(&conn, "potA", 0).spent_height, Some(800_000));

        // Reorg-confirmed S2, bump unparseable → height bind NULL.
        exec_mark_spent(&conn, "potA", 0, "settleS2", true, None, None);
        let r = read_pot_row(&conn, "potA", 0);
        assert_eq!(r.spending_txid.as_deref(), Some("settleS2"));
        assert_eq!(
            r.spent_height, None,
            "S2 never inherits S1's height — reset on pointer change"
        );

        // A pointer change WITH a height carries its own.
        exec_mark_spent(&conn, "potA", 0, "settleS3", true, None, Some(803_000));
        assert_eq!(read_pot_row(&conn, "potA", 0).spent_height, Some(803_000));
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

    /// #289: the batched spent-status SQL must execute on the production
    /// schema (row-value `IN (VALUES …)` is newer SQLite surface) and select
    /// per-OUTPOINT — a txid match with a different vout must NOT be
    /// returned.
    #[test]
    fn pot_batch_sql_selects_exact_outpoints_real_sqlite() {
        let conn = production_schema_db();
        insert_pot(&conn, &h64(0xaa), 0, 1, false);
        insert_pot(&conn, &h64(0xaa), 1, 2, true);
        insert_pot(&conn, &h64(0xbb), 0, 3, false);

        let sql = pot_spent_statuses_sql(3);
        let mut stmt = conn.prepare(&sql).expect("batch SQL must parse on real SQLite");
        // Ask for (aa,1), (bb,0) and an absent (cc,0).
        let rows: Vec<(String, u32)> = stmt
            .query_map(
                rusqlite::params![h64(0xaa), 1u32, h64(0xbb), 0u32, h64(0xcc), 0u32],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?)),
            )
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let mut sorted = rows;
        sorted.sort();
        assert_eq!(
            sorted,
            vec![(h64(0xaa), 1), (h64(0xbb), 0)],
            "exactly the requested present outpoints — no txid-only matches, \
             no phantom rows"
        );
    }

    /// #289: the shipped `ls_proof` select answers the SAME `bundleBase64`
    /// for a new row (admission-time `bundleB64`) and a pre-#289 legacy row
    /// (NULL `bundleB64` → hex(bundle) fallback) — byte-identical wire
    /// either way — and never hauls the blob when the b64 column answers.
    #[test]
    fn proof_list_sql_b64_and_legacy_rows_answer_identically_real_sqlite() {
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

        let conn = production_schema_db();
        let bundle: &[u8] = b"{\"v\":1,\"proof\":true}";
        let game = h64(0x11);
        // New-path row: bundle BLOB + admission-time bundleB64 (as
        // D1ProofStorage::store_record now writes).
        conn.execute(
            "INSERT INTO proof_markers \
             (gameId, winner, sigHex, bundle, bundleB64, txid, outputIndex, createdAt) \
             VALUES (?1, '02aa', 'sig', ?2, ?3, ?4, 0, 2)",
            rusqlite::params![game, bundle, BASE64.encode(bundle), h64(0x01)],
        )
        .unwrap();
        // Legacy row: same bytes, NO bundleB64 (pre-#289 writer shape).
        conn.execute(
            "INSERT INTO proof_markers \
             (gameId, winner, sigHex, bundle, txid, outputIndex, createdAt) \
             VALUES (?1, '02aa', 'sig', ?2, ?3, 0, 1)",
            rusqlite::params![game, bundle, h64(0x02)],
        )
        .unwrap();

        let mut stmt = conn
            .prepare(proof_list_for_game_winner_sql())
            .expect("shipped proof select must parse");
        let rows: Vec<(String, Option<String>, String)> = stmt
            .query_map(rusqlite::params![game, "02aa", 10u32], |row| {
                Ok((
                    row.get::<_, String>(5)?,         // txid
                    row.get::<_, Option<String>>(3)?, // bundleB64
                    row.get::<_, String>(4)?,         // bundle (hex or '')
                ))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(rows.len(), 2);
        // Newest first: the b64 row (createdAt 2), then the legacy row.
        let (new_txid, new_b64, new_hex) = &rows[0];
        let (old_txid, old_b64, old_hex) = &rows[1];
        assert_eq!(new_txid, &h64(0x01));
        assert_eq!(old_txid, &h64(0x02));
        assert_eq!(new_hex, "", "b64 answered — the blob is NOT hauled");
        assert!(old_b64.is_none(), "legacy row has no b64");

        // The wire value each path produces (mirrors the lookup service:
        // stored b64 preferred, else encode the decoded hex) must be
        // byte-identical.
        let wire_new = new_b64.clone().unwrap();
        let wire_old = BASE64.encode(hex::decode(old_hex).unwrap());
        assert_eq!(wire_new, wire_old, "both read paths answer the same bundleBase64");
        assert_eq!(wire_new, BASE64.encode(bundle), "…and it is the admitted bytes");
    }

    // ── #290/#291: the low / reveal / collected shipped SQL ──────────────

    /// Insert a `low_records` row with an explicit TEXT `createdAt`
    /// (this table's `createdAt` is `datetime('now')` TEXT — the odd one
    /// out; every other LOW marker table stamps INTEGER unix seconds).
    fn insert_low_for_host(
        conn: &rusqlite::Connection,
        txid: &str,
        host: &str,
        created_at: &str,
    ) {
        conn.execute(
            "INSERT INTO low_records (recordType, txid, outputIndex, hostIdentity, \
             gameId, stakeSats, rulesHash, relayUrl, expiryHeight, createdAt) \
             VALUES ('table', ?1, 0, ?2, ?3, 1000, 'rh', 'https://r', 900000, ?4)",
            rusqlite::params![txid, host, h64(0x11), created_at],
        )
        .unwrap();
    }

    fn insert_low(conn: &rusqlite::Connection, txid: &str, created_at: &str) {
        insert_low_for_host(conn, txid, &victim_id(), created_at);
    }

    /// #290/#291: the shipped lobby SQL executes on the production schema,
    /// returns FULL index rows newest-first, and is LIMIT-bounded. Physical
    /// insertion order CONTRADICTS createdAt order (F4 discipline) so a
    /// green cannot come from incidental row order.
    #[test]
    fn low_open_tables_sql_newest_first_and_capped_real_sqlite() {
        let conn = production_schema_db();
        // Newest inserted FIRST (physical order opposes the promised order).
        insert_low(&conn, &h64(0x03), "2026-07-29 12:00:03");
        insert_low(&conn, &h64(0x01), "2026-07-29 12:00:01");
        insert_low(&conn, &h64(0x02), "2026-07-29 12:00:02");

        // The full lobby where-shape (type + stake range + expiry filter).
        let sql = low_open_tables_sql(
            " WHERE recordType = ? AND stakeSats >= ? AND stakeSats <= ? AND expiryHeight > ?",
        );
        let mut stmt = conn.prepare(&sql).expect("shipped lobby SQL must parse");
        let rows: Vec<(String, u64)> = stmt
            .query_map(rusqlite::params!["table", 100u64, 5000u64, 800000u32], |row| {
                Ok((row.get::<_, String>(1)?, row.get::<_, u64>(5)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows.iter().map(|(t, _)| t.clone()).collect::<Vec<_>>(),
            vec![h64(0x03), h64(0x02), h64(0x01)],
            "newest-first by createdAt, not physical order"
        );
        assert_eq!(rows[0].1, 1000, "full index row: stakeSats decoded column present");

        // LIMIT proof: cap + 1 rows (each from a DISTINCT host, so the M3
        // per-host quota is not the binding constraint) ⇒ cap rows out, and
        // the row displaced is the OLDEST (the cap keeps the newest — the
        // #291 contract).
        let conn = production_schema_db();
        for i in 0..=OPEN_TABLES_RESULT_CAP {
            insert_low_for_host(
                &conn,
                &format!("{i:064x}"),
                &format!("02{i:064x}"),
                &format!("2026-07-29 13:{:02}:{:02}", (i / 60) % 60, i % 60),
            );
        }
        let sql = low_open_tables_sql(" WHERE recordType = ?");
        let mut stmt = conn.prepare(&sql).unwrap();
        let txids: Vec<String> = stmt
            .query_map(rusqlite::params!["table"], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(txids.len(), OPEN_TABLES_RESULT_CAP, "bounded at the cap");
        assert!(
            !txids.contains(&format!("{:064x}", 0)),
            "the displaced row is the OLDEST, never a newer one"
        );
        assert!(txids.contains(&format!("{:064x}", OPEN_TABLES_RESULT_CAP)));
    }

    /// #291 gate finding M3 (the #281 partitioned-window pattern): ONE
    /// identity flooding the lobby with newest-stamped junk occupies at most
    /// OPEN_TABLES_PER_HOST_CAP window slots — every other host's honest
    /// table SURVIVES in the answer. Pre-partition, a single host's 200
    /// newest rows blanked the whole lobby. Executes the SHIPPED SQL.
    #[test]
    fn lobby_single_host_flood_cannot_blank_other_hosts_real_sqlite() {
        let conn = production_schema_db();
        // Honest tables from two hosts, stamped EARLY.
        insert_low_for_host(&conn, &h64(0x01), &victim_id(), "2026-07-29 10:00:00");
        insert_low_for_host(
            &conn,
            &h64(0x02),
            &format!("03{}", "b2".repeat(32)),
            "2026-07-29 10:00:01",
        );
        // One attacker identity floods OPEN_TABLES_RESULT_CAP newer rows.
        let attacker = format!("02{}", "ff".repeat(32));
        for i in 0..OPEN_TABLES_RESULT_CAP {
            insert_low_for_host(
                &conn,
                &format!("{i:060x}dead"),
                &attacker,
                &format!("2026-07-29 12:{:02}:{:02}", (i / 60) % 60, i % 60),
            );
        }

        let sql = low_open_tables_sql(" WHERE recordType = ?");
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<(String, String)> = stmt
            .query_map(rusqlite::params!["table"], |row| {
                Ok((row.get::<_, String>(1)?, row.get::<_, String>(3)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();

        let attacker_rows = rows.iter().filter(|(_, h)| *h == attacker).count();
        assert_eq!(
            attacker_rows, OPEN_TABLES_PER_HOST_CAP,
            "the flooding identity holds exactly its quota, never the window"
        );
        let txids: Vec<&String> = rows.iter().map(|(t, _)| t).collect();
        assert!(
            txids.contains(&&h64(0x01)) && txids.contains(&&h64(0x02)),
            "both honest hosts' tables SURVIVE the single-identity flood \
             (pre-M3 the flat newest-first cap blanked them)"
        );
    }

    /// #290/#291: byGameId / byHost — shipped SQL parses, selects per-key,
    /// LIMIT present.
    #[test]
    fn low_by_key_sql_selects_per_key_real_sqlite() {
        let conn = production_schema_db();
        insert_low(&conn, &h64(0x01), "2026-07-29 12:00:01");
        // A row for a DIFFERENT game + host.
        conn.execute(
            "INSERT INTO low_records (recordType, txid, outputIndex, hostIdentity, \
             gameId, createdAt) VALUES ('game', ?1, 0, ?2, ?3, '2026-07-29 12:00:02')",
            rusqlite::params![h64(0x02), format!("03{}", "b2".repeat(32)), h64(0x22)],
        )
        .unwrap();

        for (sql, key) in [
            (low_by_game_id_sql(), h64(0x11)),
            (low_by_host_sql(), victim_id()),
        ] {
            assert!(sql.contains("LIMIT"), "by-key query must be bounded: {sql}");
            let mut stmt = conn.prepare(&sql).expect("shipped by-key SQL must parse");
            let txids: Vec<String> = stmt
                .query_map(rusqlite::params![key], |row| row.get::<_, String>(1))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(txids, vec![h64(0x01)], "exactly the requested key's rows");
        }
    }

    /// #290/#291: the shipped reveal SQL parses on the production schema,
    /// selects per-(gameId, seat), and is LIMIT-bounded.
    #[test]
    fn reveal_sql_selects_per_key_and_is_bounded_real_sqlite() {
        let conn = production_schema_db();
        for (txid_seed, game_seed, seat) in
            [(0x01u8, 0x11u8, 0u8), (0x02, 0x11, 1), (0x03, 0x22, 0)]
        {
            conn.execute(
                "INSERT INTO reveal_records (txid, outputIndex, gameId, seat) \
                 VALUES (?1, 0, ?2, ?3)",
                rusqlite::params![h64(txid_seed), h64(game_seed), seat],
            )
            .unwrap();
        }

        for sql in [reveal_by_game_seat_sql(), reveal_by_game_id_sql()] {
            assert!(sql.contains("LIMIT"), "reveal query must be bounded: {sql}");
        }

        let mut stmt = conn.prepare(&reveal_by_game_seat_sql()).unwrap();
        let txids: Vec<String> = stmt
            .query_map(rusqlite::params![h64(0x11), 1u8], |row| row.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(txids, vec![h64(0x02)], "per-(gameId, seat) exact");

        let mut stmt = conn.prepare(&reveal_by_game_id_sql()).unwrap();
        let mut txids: Vec<String> = stmt
            .query_map(rusqlite::params![h64(0x11)], |row| row.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        txids.sort();
        assert_eq!(txids, vec![h64(0x01), h64(0x02)], "both seats of the game, no leak");
    }

    /// #289: the batched collected-marker SQL selects per-(identity, gameId)
    /// — a same-gameId row belonging to a DIFFERENT identity must never
    /// answer for the requested identity (an absent marker reads as
    /// "not collected"; a leaked one would hide a Collect card).
    #[test]
    fn collected_batch_sql_scopes_to_identity_real_sqlite() {
        let conn = production_schema_db();
        let me = victim_id();
        let other = format!("03{}", "c3".repeat(32));
        conn.execute(
            "INSERT INTO collected_markers (identity, gameId, txid, sigHex, createdAt) \
             VALUES (?1, ?2, ?3, 'sig-a', 1)",
            rusqlite::params![me, h64(0x11), h64(0x01)],
        )
        .unwrap();
        // Same gameId, DIFFERENT identity — must not leak into my answer.
        conn.execute(
            "INSERT INTO collected_markers (identity, gameId, txid, sigHex, createdAt) \
             VALUES (?1, ?2, ?3, 'sig-b', 2)",
            rusqlite::params![other, h64(0x22), h64(0x02)],
        )
        .unwrap();

        let sql = collected_records_batch_sql(2);
        let mut stmt = conn.prepare(&sql).expect("shipped batch SQL must parse");
        let rows: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![me, h64(0x11), h64(0x22)], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows,
            vec![(me.clone(), h64(0x11))],
            "only MY marker answers — the other identity's gameId 0x22 row \
             never leaks in as a phantom 'collected'"
        );
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
        window_col_groups(conn, sql, identity, limit, 2, col)
    }

    fn window_col_groups(
        conn: &rusqlite::Connection,
        sql: &str,
        identity: &str,
        limit: usize,
        groups: usize,
        col: &str,
    ) -> Vec<String> {
        let mut stmt = conn.prepare(sql).expect("prepare");
        stmt.query_map(
            rusqlite::params![
                identity,
                limit as u32,
                unknown_pot_quota(limit) as u32,
                identity_window_row_cap(limit, groups) as u32
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

    /// Distinct pot txids in the order the window returned them (the window
    /// is a per-pot SUPERSET now, so several rows can share a pot).
    fn distinct_pots(rows: &[String]) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for r in rows {
            if !out.contains(r) {
                out.push(r.clone());
            }
        }
        out
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
        assert!(
            got.contains(&honest_pot),
            "the real pot is back — the 1 + DUST_REPLAYS marker rows naming it \
             consume ONE pot slot, not DUST_REPLAYS of them"
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
        let got = window_col_groups(
            &conn,
            &potrefund_list_for_identity_sql(),
            &victim,
            100,
            1,
            "potTxid",
        );
        let pots = distinct_pots(&got);
        let at = pots.iter().position(|t| *t == honest_pot).unwrap();
        assert!(
            at <= unknown_pot_quota(100),
            "the refund backup is back, within one quota of the top (was: absent)"
        );
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
        assert!(
            got.len() <= 2 * PARTYFOR_ROWS_PER_GROUP,
            "one pot ⇒ at most 2 groups × N rows, however much junk: {}",
            got.len()
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
        assert_eq!(unique.len(), 100, "all 100 real pots returned");
        assert!(
            got.len() <= 100 * 2 * PARTYFOR_ROWS_PER_GROUP,
            "and the superset stays inside the row cap"
        );
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
            got.len(),
            PARTYFOR_ROWS_PER_GROUP,
            "the group window keeps exactly N candidates"
        );
        assert!(
            got.contains(&"txHONEST".to_string()),
            "the OLDEST rows are the candidates, so the honest marker is among \
             them however many NEWER junk rows exist: {got:?}"
        );
        assert!(
            !got.contains(&"txJUNK019".to_string()),
            "and the newest junk is outside the window"
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
        // `txA*`/`txB*` share a createdAt, so only the rowid tiebreak orders
        // them — and the SUPERSET keeps BOTH, deterministically, every time.
        // The window bounds cost; it never picks which one is real.
        assert_eq!(
            baseline.iter().filter(|t| t.starts_with("txB")).count(),
            40,
            "every stored candidate is returned as a candidate"
        );
        assert_eq!(baseline.iter().filter(|t| t.starts_with("txA")).count(), 40);
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
            .query_map(rusqlite::params![pot, 0u32, 100u32, 0u32], |r| {
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

    /// bsv-low #291 gate finding M2: the byPot window is offset-PAGEABLE —
    /// a row buried behind more than MAX_LIMIT older rows (the pre-funding
    /// front-run) is still REACHABLE, pages are disjoint and cover the set,
    /// and each response stays LIMIT-bounded. Executes the SHIPPED SQL.
    #[test]
    fn by_pot_offset_pages_reach_every_row_real_sqlite() {
        let conn = production_schema_db();
        let pot = h64(0xbb);
        // 130 junk rows admitted BEFORE funding (older stamps)…
        for i in 0..130u32 {
            insert_potrefund(
                &conn,
                &format!("02{:064x}", i),
                &pot,
                0,
                &format!("txJUNK{i:03}"),
                100 + i as i64,
            );
        }
        // …then the honest backup, landed at funding — beyond ANY single
        // page at MAX_LIMIT 100.
        insert_potrefund(&conn, &victim_id(), &pot, 0, "txHONEST", 10_000);

        let sql = list_for_pot_sql(POTREFUND_SELECT);
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut page = |limit: u32, offset: u32| -> Vec<String> {
            stmt.query_map(rusqlite::params![pot, 0u32, limit, offset], |r| {
                r.get::<_, String>("txid")
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
        };

        let p1 = page(100, 0);
        let p2 = page(100, 100);
        assert_eq!(p1.len(), 100, "page 1 LIMIT-bounded");
        assert_eq!(p2.len(), 31, "page 2 = the remaining 30 junk + the honest row");
        assert!(
            !p1.contains(&"txHONEST".to_string()),
            "sanity: the buried row is NOT on page 1 (it needs paging)"
        );
        assert_eq!(
            p2.last().unwrap(),
            "txHONEST",
            "the row behind >MAX_LIMIT junk is REACHABLE via offset — the \
             cap bounds a response, never the reachable set"
        );
        // Disjoint + covering: pages partition the oldest-first total order.
        let mut all: Vec<String> = p1.iter().chain(p2.iter()).cloned().collect();
        let n = all.len();
        all.dedup();
        assert_eq!(all.len(), n, "pages are disjoint (stable total order)");
        assert_eq!(n, 131, "pages cover every admitted row");
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

    // ── FIX B (2026-07-28 owner steer) — SUPERSET, not `rn = 1` ──────────
    //
    // PRINCIPLE: verification happens BEFORE collapse. SQL cannot verify a
    // signature, so it must not choose which row is real; it returns a bounded
    // superset and `lookupPotParty` — which DOES verify, and DROPS a failing
    // row — collapses. Under `rn = 1` a single forged marker stamped earlier
    // than the victim's own won the slot server-side, was discarded
    // client-side, and the pot VANISHED from recovery: strictly worse than
    // main, which returned [honest, forged] and kept the honest one.

    /// The gate's exact scenario: ONE forged marker, older than the honest
    /// one, in each group.
    #[test]
    fn one_forged_marker_cannot_erase_the_pot_from_partyfor() {
        let conn = production_schema_db();
        let victim = victim_id();
        let pot = h64(0xaa);
        insert_pot(&conn, &pot, 0, 1_000, false);
        // The attacker HAS the pot txid — it has been public for weeks (the
        // #252 backfill publishes honest markers long after funding), so it
        // files EARLIER than the honest seat, in BOTH groups.
        insert_potparty(&conn, &victim, &pot, 0, "txFORGEDV1", 100, None);
        insert_potparty(
            &conn,
            &victim,
            &pot,
            0,
            "txFORGEDV2",
            101,
            Some(&format!("03{}", "f0".repeat(32))),
        );
        insert_potparty(&conn, &victim, &pot, 0, "txHONESTV1", 5_000, None);
        insert_potparty(
            &conn,
            &victim,
            &pot,
            0,
            "txHONESTV2",
            5_001,
            Some(&format!("02{}", "5e".repeat(32))),
        );

        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "txid",
        );
        assert!(
            got.contains(&"txHONESTV1".to_string()),
            "the honest v1 row must SURVIVE alongside the forged one — the \
             client drops the forgery and keeps this: {got:?}"
        );
        assert!(
            got.contains(&"txHONESTV2".to_string()),
            "and the honest v2 row too (the seat proof / v2Indexed latch): {got:?}"
        );
    }

    /// The eviction bar, at N-2 / N-1 / N forged rows per group. The window
    /// keeps the OLDEST `PARTYFOR_ROWS_PER_GROUP`, so the honest row survives
    /// while FEWER than that many forgeries precede it — and is evicted at
    /// exactly N, which is the documented residual, pinned so it cannot drift
    /// silently.
    #[test]
    fn the_eviction_bar_is_exactly_partyfor_rows_per_group() {
        let n = PARTYFOR_ROWS_PER_GROUP;
        for forged in [n - 2, n - 1, n] {
            let conn = production_schema_db();
            let victim = victim_id();
            let pot = h64(0xaa);
            insert_pot(&conn, &pot, 0, 1_000, false);
            for i in 0..forged {
                insert_potparty(
                    &conn,
                    &victim,
                    &pot,
                    0,
                    &format!("txFORGED{i:03}"),
                    100 + i as i64,
                    None,
                );
            }
            insert_potparty(&conn, &victim, &pot, 0, "txHONEST", 5_000, None);
            let got = window_col(
                &conn,
                &potparty_list_for_identity_sql(),
                &victim,
                100,
                "txid",
            );
            let survived = got.contains(&"txHONEST".to_string());
            if forged < n {
                assert!(
                    survived,
                    "{forged} forged (< {n}) must NOT evict the honest row"
                );
            } else {
                assert!(
                    !survived,
                    "{forged} forged (= {n}) evicts it — a COST BAR, not a proof"
                );
            }
        }
    }

    /// The same superset guarantee on the refund index, where the row carries
    /// `refundRawHex` — the pre-signed refund that brings the ante home.
    #[test]
    fn one_forged_marker_cannot_erase_a_refund_backup() {
        let conn = production_schema_db();
        let victim = victim_id();
        let pot = h64(0xaa);
        insert_pot(&conn, &pot, 0, 1_000, false);
        insert_potrefund(&conn, &victim, &pot, 0, "txFORGED", 100);
        insert_potrefund(&conn, &victim, &pot, 0, "txHONEST", 5_000);
        let got = window_col_groups(
            &conn,
            &potrefund_list_for_identity_sql(),
            &victim,
            100,
            1,
            "txid",
        );
        assert!(
            got.contains(&"txHONEST".to_string()),
            "the honest refund backup must survive a forged older row: {got:?}"
        );
    }

    /// MEASURED BUDGET (owner steer): pin the worst-case response weight so a
    /// future bump to `PARTYFOR_ROWS_PER_GROUP` trips a test, not production.
    #[test]
    fn worst_case_window_response_stays_in_budget() {
        let conn = production_schema_db();
        let victim = victim_id();
        // 100 pots (the client's default limit), EVERY group filled — the
        // state an attacker must pay 800 dust transactions to produce.
        for i in 0..100u32 {
            let pot = format!("{:064x}", 0x0000_1000u64 + i as u64);
            insert_pot(&conn, &pot, 0, 1_000 + i as i64, true);
            for k in 0..PARTYFOR_ROWS_PER_GROUP {
                insert_potparty(
                    &conn,
                    &victim,
                    &pot,
                    0,
                    &format!("a{i:03}{k}"),
                    100 + k as i64,
                    None,
                );
                insert_potparty(
                    &conn,
                    &victim,
                    &pot,
                    0,
                    &format!("b{i:03}{k}"),
                    200 + k as i64,
                    Some(&format!("02{}", "5e".repeat(32))),
                );
            }
        }
        let sql = potparty_list_for_identity_sql();
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<PotpartyRecord> = stmt
            .query_map(
                rusqlite::params![
                    victim,
                    100u32,
                    unknown_pot_quota(100) as u32,
                    identity_window_row_cap(100, 2) as u32
                ],
                |r| {
                    Ok(PotpartyRecord {
                        identity: r.get("identity")?,
                        opponent_identity: r.get("opponentIdentity")?,
                        game_id: r.get("gameId")?,
                        pot_txid: r.get("potTxid")?,
                        pot_vout: r.get::<_, i64>("potVout")? as u32,
                        recovery_height: r.get::<_, i64>("recoveryHeight")? as u32,
                        sig_hex: r.get::<_, Option<String>>("sigHex")?.unwrap_or_default(),
                        seat_settle_pubkey: r.get("seatSettlePubkey")?,
                        seat_sig_hex: r.get("seatSigHex")?,
                        txid: r.get("txid")?,
                        output_index: r.get::<_, i64>("outputIndex")? as u32,
                        created_at: r.get::<_, i64>("createdAt")?,
                    })
                },
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows.len(),
            100 * 2 * PARTYFOR_ROWS_PER_GROUP,
            "the row cap is exact, so the LIMIT never cuts a pot in half"
        );
        let bytes = serde_json::to_string(&rows).unwrap().len();
        // 640 KiB: the measured 548 KiB at N=4 plus headroom. N=8 measured
        // 1.07 MiB and would fail here — which is the point.
        assert!(
            bytes < 640 * 1024,
            "worst-case ls_potparty response {bytes} bytes exceeds the budget \
             — re-measure before raising PARTYFOR_ROWS_PER_GROUP"
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
        // limit pots × groups × PARTYFOR_ROWS_PER_GROUP.
        assert_eq!(
            identity_window_row_cap(100, 2),
            100 * 2 * PARTYFOR_ROWS_PER_GROUP
        );
        assert_eq!(
            identity_window_row_cap(100, 1),
            100 * PARTYFOR_ROWS_PER_GROUP
        );
        assert!(
            identity_window_row_cap(usize::MAX, 2) > 0,
            "saturating, never a panic"
        );
    }
}
