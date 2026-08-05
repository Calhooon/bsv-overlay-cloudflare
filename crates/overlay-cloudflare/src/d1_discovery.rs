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
use overlay_discovery::hopparty::storage::{
    HoppartyRecord, HoppartyStorage, HoppartyStorageError, HOPSFOR_ROWS_PER_OUTPOINT,
};
use overlay_discovery::low::storage::{
    LowRecord, LowRecordType, LowStorage, LowStorageError, LOW_BY_KEY_RESULT_CAP,
    OPEN_TABLES_PER_HOST_CAP, OPEN_TABLES_RESULT_CAP,
};
use overlay_discovery::pot::storage::{pot_beef_has_proof, PotRecord, PotStorage, PotStorageError};
use overlay_discovery::potparty::storage::{PotpartyRecord, PotpartyStorage, PotpartyStorageError};
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

/// `find_tables_for_spend_check` (bsv-low #309) — bounded RANDOM sample of
/// TABLE rows for the cron's advert spend-confirmation pass. RANDOM defeats
/// head-of-queue starvation (the same anti-starvation shape as
/// `pot_records.find_spent_unconfirmed`): a probe-resistant head cannot
/// starve the tail, and every row is eventually visited across ticks.
/// `limit` is interpolated (a code constant, never user input) to match the
/// sibling idiom. Factored out so the real-SQLite tests execute the SHIPPED
/// string.
pub fn low_tables_for_spend_check_sql(limit: u64) -> String {
    format!(
        "SELECT {LOW_RECORD_COLUMNS} FROM low_records \
         WHERE recordType = 'table' ORDER BY RANDOM() LIMIT {limit}"
    )
}

/// `find_tables_expired_at_or_before` (bsv-low #309) — TABLE rows with a
/// NON-NULL `expiryHeight <= ?` (the caller-computed `tip - margin` cutoff),
/// OLDEST-expiry-first (deterministic: deletion removes rows from the set,
/// so a fixed order drains the backlog front-to-back without starvation),
/// bounded. `expiryHeight IS NOT NULL` is belt-and-braces (`NULL <= ?` is
/// already never true) and documents that a NULL-expiry row is NEVER reaped.
/// Backed by `idx_low_expiry (recordType, expiryHeight)`.
pub fn low_tables_expired_sql(limit: u64) -> String {
    format!(
        "SELECT {LOW_RECORD_COLUMNS} FROM low_records \
         WHERE recordType = 'table' AND expiryHeight IS NOT NULL AND expiryHeight <= ? \
         ORDER BY expiryHeight ASC LIMIT {limit}"
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
    /// `None` on an unknown recordType — possible only under version skew
    /// (a discriminator written by a NEWER deploy read back after a
    /// rollback). Callers go through [`low_records_from_rows`], which makes
    /// the skip LOUD (gate finding L3) — never a silent vanish.
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

/// Convert `low_records` rows, LOUDLY skipping any with an unknown
/// `recordType` discriminator (gate finding L3): a silent `filter_map` let
/// a discriminator added writer-side before reader-side (deploy rollback /
/// version skew) vanish rows from lobby and rejoin answers with zero
/// signal. Good rows always survive; each skip is console-warned with its
/// outpoint so the skew is diagnosable from logs.
fn low_records_from_rows(rows: Vec<LowRow>) -> Vec<LowRecord> {
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let outpoint = format!("{}:{}", row.txid, row.output_index);
        let record_type = row.record_type.clone();
        match row.into_record() {
            Some(record) => records.push(record),
            None => {
                let msg = format!(
                    "low_records: SKIPPING row {outpoint} with unknown recordType \
                     '{record_type}' (reader older than writer? deploy skew) — \
                     the row is preserved in D1, only this answer omits it"
                );
                // worker::console_warn! requires the JS host; native (unit
                // tests) goes to stderr.
                #[cfg(target_arch = "wasm32")]
                worker::console_warn!("{msg}");
                #[cfg(not(target_arch = "wasm32"))]
                eprintln!("{msg}");
            }
        }
    }
    records
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
        Ok(low_records_from_rows(rows))
    }

    async fn find_by_game_id(&self, game_id: &str) -> Result<Vec<LowRecord>, LowStorageError> {
        let rows: Vec<LowRow> = Query::new(low_by_game_id_sql())
            .bind(game_id)
            .fetch_all(&self.db)
            .await
            .map_err(low_err)?;
        Ok(low_records_from_rows(rows))
    }

    async fn find_by_host(&self, identity_key: &str) -> Result<Vec<LowRecord>, LowStorageError> {
        let rows: Vec<LowRow> = Query::new(low_by_host_sql())
            .bind(identity_key)
            .fetch_all(&self.db)
            .await
            .map_err(low_err)?;
        Ok(low_records_from_rows(rows))
    }

    async fn find_tables_for_spend_check(
        &self,
        limit: u64,
    ) -> Result<Vec<LowRecord>, LowStorageError> {
        let rows: Vec<LowRow> = Query::new(low_tables_for_spend_check_sql(limit))
            .fetch_all(&self.db)
            .await
            .map_err(low_err)?;
        Ok(low_records_from_rows(rows))
    }

    async fn find_tables_expired_at_or_before(
        &self,
        cutoff_height: u32,
        limit: u64,
    ) -> Result<Vec<LowRecord>, LowStorageError> {
        let rows: Vec<LowRow> = Query::new(low_tables_expired_sql(limit))
            .bind(cutoff_height)
            .fetch_all(&self.db)
            .await
            .map_err(low_err)?;
        Ok(low_records_from_rows(rows))
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

/// Row for the `pot_beefs` length + verified-latch probe
/// (`length(beef) AS len, proof_verified`). D1 returns numbers as f64;
/// `proof_verified` is Option-tolerant (defensive for a read racing the
/// additive bsv-low#304 migration — NULL/absent = 0 = unverified).
#[derive(Deserialize)]
struct BeefLenRow {
    len: f64,
    #[serde(default)]
    proof_verified: Option<f64>,
}

/// SHIPPED `pot_beefs` probe for [`D1PotStorage::store_beef`] (bsv-low#304)
/// — a const so the real-SQLite test executes the production string.
pub(crate) const POT_BEEF_PROBE_SQL: &str =
    "SELECT length(beef) AS len, proof_verified FROM pot_beefs WHERE txid = ?";

/// SHIPPED admit-path write: `proof_verified` FORCED to 0 (an admit bump is
/// never a verified fact — bsv-low#304); `has_proof` records structure;
/// createdAt is preserve-or-stamp (#228).
pub(crate) const POT_BEEF_ADMIT_WRITE_SQL: &str =
    "INSERT OR REPLACE INTO pot_beefs (txid, beef, createdAt, has_proof, proof_verified) \
     VALUES (?, ?, COALESCE((SELECT createdAt FROM pot_beefs WHERE txid = ?), ?), ?, 0)";

/// SHIPPED verifying write ([`D1PotStorage::compact_pot_beef`]): both
/// latches set — the caller chaintracks-verified the bump.
pub(crate) const POT_BEEF_VERIFIED_WRITE_SQL: &str =
    "INSERT OR REPLACE INTO pot_beefs (txid, beef, createdAt, has_proof, proof_verified) \
     VALUES (?, ?, ?, 1, 1)";

/// SHIPPED verified-latch flip ([`D1PotStorage::mark_pot_beef_proven`]) —
/// no byte rewrite, no createdAt touch.
pub(crate) const POT_BEEF_MARK_PROVEN_SQL: &str =
    "UPDATE pot_beefs SET proof_verified = 1, has_proof = 1 WHERE txid = ?";

/// Chunk size for the batched verified-latch flip (bsv-low#304 gate M-4) —
/// one D1 statement per up-to-100 latched rows instead of one per row,
/// comfortably under SQLite's bind-parameter ceiling.
pub(crate) const POT_BEEF_MARK_PROVEN_CHUNK: usize = 100;

/// SHIPPED batched verified-latch flip for `n` txids
/// ([`D1PotStorage::mark_pot_beefs_proven`]) — the `IN (?, …)` form of
/// [`POT_BEEF_MARK_PROVEN_SQL`], identical semantics per row.
pub(crate) fn pot_beef_mark_proven_batch_sql(n: usize) -> String {
    debug_assert!(n >= 1);
    let placeholders = vec!["?"; n].join(", ");
    format!("UPDATE pot_beefs SET proof_verified = 1, has_proof = 1 WHERE txid IN ({placeholders})")
}

/// SHIPPED completion-pass candidate scan (bsv-low#304: gated on the
/// VERIFIED latch, not the structural flag).
pub(crate) fn pot_beef_candidates_sql(limit: u64, min_age_secs: u64) -> String {
    format!(
        "SELECT txid, hex(beef) AS beef FROM pot_beefs \
         WHERE proof_verified = 0 \
           AND (createdAt IS NULL OR createdAt <= unixepoch() - {min_age_secs}) \
         ORDER BY RANDOM() LIMIT {limit}"
    )
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
pub fn verdict_cas_sql() -> &'static str {
    "UPDATE pot_records SET verdict = ?, verdictTxid = ? \
     WHERE txid = ? AND outputIndex = ? AND spendingTxid = ?"
}

/// The #301 spend-confirmation CAS (the [`verdict_cas_sql`] sibling): the
/// #186 chaser's confirmed latch, GUARDED on the spender the SPV proof was
/// verified FOR. If the pointer moved between the chaser's candidate read
/// and this write (a reorg-confirmed S2 landing in the await window), the
/// WHERE misses and the write is a NO-OP — the pre-#301 unguarded
/// `mark_spent(confirmed)` write RESET the pointer back to the stale S1,
/// and nothing ever re-chased it (the candidate query only surfaces
/// `spentConfirmed = 0` rows).
///
/// SET is the guard-hit subset of `mark_spent_sql(true, false)`: the
/// pointer already equals the bound spender, so only `spent`/
/// `spentConfirmed` latch and `spentHeight` keeps-or-updates
/// (`COALESCE(?, spentHeight)` — the same-pointer branch of the LOW-1
/// CASE). `spentAt` is deliberately NOT restamped (the verdict-CAS
/// touches-nothing-else idiom: a confirmed row leaves the candidate set,
/// so the #228 age anchor is moot — and a missed row keeps its true age).
/// `RETURNING txid` makes the hit/miss observable through the ordinary
/// row-read path (`fetch_optional`): a row back = the guard HIT.
///
/// Bind order: `spentHeight, txid, outputIndex, spendingTxid (guard)`.
pub fn confirm_spend_cas_sql() -> &'static str {
    "UPDATE pot_records SET spent = 1, spentConfirmed = 1, \
         spentHeight = COALESCE(?, spentHeight) \
     WHERE txid = ? AND outputIndex = ? AND spendingTxid = ? \
     RETURNING txid"
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

    async fn mark_confirmed_for_spender(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        spent_height: Option<u64>,
    ) -> Result<bool, PotStorageError> {
        // The #301 CAS confirm (see `confirm_spend_cas_sql`): guarded on
        // the spender the proof was verified for; a moved pointer ⇒ WHERE
        // misses ⇒ no-op. RETURNING txid turns the hit into a row, so
        // `fetch_optional` answers the hit/miss the caller counts.
        let hit: Option<serde_json::Value> = Query::new(confirm_spend_cas_sql())
            .bind(spent_height)
            .bind(txid)
            .bind(output_index)
            .bind(spending_txid)
            .fetch_optional(&self.db)
            .await
            .map_err(pot_err)?;
        Ok(hit.is_some())
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
            .map(|(txid, output_index)| by_outpoint.get(&(txid.clone(), *output_index)).cloned())
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
        // Probe the existing row's length + verified latch first; write only
        // when absent or strictly longer ([`beef_write_allowed`] — never
        // clobber a good row with a shorter/empty one) AND the existing row
        // is not VERIFIED-proven (bsv-low#304: an admit-path write is
        // untrusted submitter bytes; a chaintracks-verified row is
        // authoritative and only the verifying writers may rewrite it —
        // "never weaken existing verified answers").
        let existing: Option<BeefLenRow> = Query::new(POT_BEEF_PROBE_SQL)
            .bind(txid)
            .fetch_optional(&self.db)
            .await
            .map_err(pot_err)?;
        if existing
            .as_ref()
            .is_some_and(|r| r.proof_verified.unwrap_or(0.0) != 0.0)
        {
            return Ok(());
        }
        if !beef_write_allowed(existing.map(|r| r.len as usize), beef.len()) {
            return Ok(());
        }

        // OR REPLACE + BLOB bind — the same idiom as the engine's
        // transactions upsert (`d1_storage.rs::insert_output`): the guard
        // above means we only ever replace with a strictly longer beef.
        // has_proof (#192/#193) records whether this beef STRUCTURALLY
        // carries a BUMP for its own txid; proof_verified is FORCED to 0 —
        // an admit-path bump is never a verified fact (bsv-low#304), so the
        // row stays a completion-pass candidate until the pass
        // chaintracks-verifies (or replaces) its proof.
        let has_proof = i64::from(pot_beef_has_proof(txid, beef));
        // createdAt is preserve-or-stamp (#228 backstop age anchor): a
        // longer-beef rewrite keeps the original first-store time so the
        // push-primary backstop's age gate measures real age.
        Query::new(POT_BEEF_ADMIT_WRITE_SQL)
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
        // ONLY not-yet-VERIFIED rows (#192/#193, re-based on the verified
        // latch by bsv-low#304 — a structurally-bumped admit row must stay
        // a candidate until its proof is chaintracks-verified or replaced),
        // RANDOM-sampled so a never-mineable head cannot starve the tail
        // (zanaadu prod incident). Reaches the whole historical backlog
        // (rows written before the proof_verified column default to 0 and
        // re-latch via the pass's stored-bump re-verify fast path). Bytes
        // are read back as hex (the pot_beefs idiom).
        //
        // Push-primary backstop age gate (#228): young rows wait for their
        // /arc-ingest push; NULL createdAt (pre-migration) = eligible.
        let sql = pot_beef_candidates_sql(limit, min_age_secs);
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
        // SHORTER (its proven ancestry has been trimmed away). This is a
        // VERIFYING writer (every caller chaintracks-verified the bump before
        // stitching), so BOTH has_proof and the bsv-low#304 proof_verified
        // latch are set — the row drops out of the completion candidate set
        // and its confirmed/height become index-servable.
        if !pot_beef_has_proof(txid, new_beef) {
            return Ok(());
        }
        Query::new(POT_BEEF_VERIFIED_WRITE_SQL)
            .bind(txid)
            .bind(new_beef)
            .bind(current_unix_seconds_i64())
            .execute(&self.db)
            .await
            .map_err(pot_err)
    }

    async fn mark_pot_beef_proven(&self, txid: &str) -> Result<(), PotStorageError> {
        // Lightweight verified-latch flip (bsv-low#304) — NO byte rewrite,
        // first-store age anchor untouched. Called ONLY after the completion
        // pass chaintracks-re-verified the STORED bump (the honest-backlog
        // fast path). has_proof is latched alongside (the bytes demonstrably
        // carry the bump that just verified).
        Query::new(POT_BEEF_MARK_PROVEN_SQL)
            .bind(txid)
            .execute(&self.db)
            .await
            .map_err(pot_err)
    }

    async fn mark_pot_beefs_proven(&self, txids: &[String]) -> Result<(), PotStorageError> {
        // One statement per POT_BEEF_MARK_PROVEN_CHUNK rows (bsv-low#304
        // gate M-4) — the per-row round trip was the fast path's dominant
        // subrequest cost. A failed chunk propagates; its rows simply stay
        // candidates for the next tick (fail-safe).
        for chunk in txids.chunks(POT_BEEF_MARK_PROVEN_CHUNK) {
            let mut q = Query::new(pot_beef_mark_proven_batch_sql(chunk.len()));
            for txid in chunk {
                q = q.bind(txid.as_str());
            }
            q.execute(&self.db).await.map_err(pot_err)?;
        }
        Ok(())
    }

    async fn pot_beef_proof_verified(&self, txid: &str) -> Result<bool, PotStorageError> {
        let row: Option<BeefLenRow> = Query::new(POT_BEEF_PROBE_SQL)
            .bind(txid)
            .fetch_optional(&self.db)
            .await
            .map_err(pot_err)?;
        Ok(row.is_some_and(|r| r.proof_verified.unwrap_or(0.0) != 0.0))
    }
}

// =============================================================================
// D1CollectedStorage
// =============================================================================

/// Row for collected-marker queries. `txid` is NOT NULL in the v2 schema (it
/// is half the primary key); `sigHex` remains nullable. `outputIndex` is an
/// INTEGER column that D1 hands back as a number.
#[derive(Deserialize)]
struct CollectedRow {
    identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: u32,
    #[serde(rename = "sigHex")]
    sig_hex: Option<String>,
}

impl CollectedRow {
    fn into_record(self) -> CollectedRecord {
        CollectedRecord {
            identity: self.identity,
            game_id: self.game_id,
            txid: self.txid,
            output_index: self.output_index,
            sig_hex: self.sig_hex,
        }
    }
}

/// Rows kept per `(identity, gameId)` in the batched collected-marker read.
///
/// The #327 S8 re-key removed EXCLUSIVITY, which was the bug — but removing a
/// slot without adding a bound just trades a squat for unbounded growth
/// (gate finding H6): `tm_collected` admits EVERY matching output, so one
/// transaction with N marker OP_RETURNs mints N permanent rows for one
/// victim's pair, and rows are never deleted. The sibling this supersede
/// pattern was cloned from (`result_markers_v2`) carries a per-key window for
/// exactly this reason; inherit the bound along with the pattern.
///
/// **Rule 6 — compare the failure modes, because a window can re-create the
/// censorship the re-key removed.** Unbounded: the honest row is always
/// returned, but the response grows without limit. Windowed: the response is
/// bounded, and an attacker who fills the window can push the honest row out.
/// That trade is acceptable HERE, and only here, because eviction on this
/// surface is FAIL-SAFE: a missing marker reads as "not collected", so the
/// Collect card stays VISIBLE and a re-collect is idempotent (the money-safe
/// direction). On a recovery-enumeration surface the same trade would be a
/// money bug, which is why `ls_potparty`/`ls_potrefund` need the full
/// existence-tier treatment rather than a plain cap.
const COLLECTED_ROWS_PER_PAIR: usize = 8;

/// SQL for one batched collected-marker chunk (bsv-low #289): one
/// `identity = ? AND gameId IN (…)` query replacing `n` individual round
/// trips. Factored out so the real-SQLite test proves the SHIPPED string
/// selects per-(identity, gameId) — never a same-gameId row belonging to a
/// DIFFERENT identity.
///
/// #327 S8: reads `collected_markers_v2` and returns every marker row per pair
/// (the old table is write-frozen and its rows were carried over by the
/// one-time migration), bounded to [`COLLECTED_ROWS_PER_PAIR`] per pair.
///
/// **The ordering is chosen against the documented attack, not by habit.** The
/// squat is PRE-EMPTIVE — filed at deal time, long before the victim ever
/// collects — so `createdAt ASC` (oldest-first) would hand the squatter the
/// whole window by construction and evict the genuine marker that arrives
/// later. `DESC` keeps the most recent rows, so a pre-filed squat can never
/// displace the victim's genuine marker; an attacker must instead out-file it
/// AFTERWARDS.
///
/// **Scope that post-hoc cost honestly:** out-filing costs a real fee-bearing
/// transaction per row only once `SUBMIT_ENFORCE=true`. In the shipping
/// default (lenient) it is still FREE — verified live. So today this ordering
/// buys the PRE-EMPTIVE case only, which is the documented attack; the
/// post-hoc case is bought by the strict flip, not by this `ORDER BY`.
/// `txid` breaks ties so the window is deterministic.
pub fn collected_records_batch_sql(n: usize) -> String {
    let placeholders = vec!["?"; n].join(", ");
    format!(
        "SELECT identity, gameId, txid, outputIndex, sigHex FROM \
           (SELECT identity, gameId, txid, outputIndex, sigHex, \
                   ROW_NUMBER() OVER (PARTITION BY identity, gameId \
                                      ORDER BY createdAt DESC, txid DESC, \
                                               outputIndex DESC) AS rn \
            FROM collected_markers_v2 \
            WHERE identity = ? AND gameId IN ({placeholders})) \
         WHERE rn <= {per_pair} \
         ORDER BY gameId ASC, rn ASC",
        per_pair = COLLECTED_ROWS_PER_PAIR,
    )
}

/// Cloudflare D1 implementation of the CollectedStorage trait
/// (tm_collected / ls_collected, bsv-low #161).
///
/// Schema: `collected_markers_v2` in `d1::OVERLAY_MIGRATIONS`. Keyed by the
/// marker OUTPOINT `(txid, outputIndex)`; `INSERT OR IGNORE` makes a replay of
/// the same output a no-op while markers for one `(identity, gameId)` from
/// DIFFERENT txs all coexist, and rows are NEVER deleted (a collected fact is
/// permanent, like a reveal; the lookup service's spend/eviction hooks are
/// no-ops).
///
/// The superseded `collected_markers` table is write-frozen — nothing here
/// reads or writes it (#327 S8).
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
        // INSERT OR IGNORE on the (txid, outputIndex) primary key: a replayed
        // submit of the same output is a no-op; a marker for the same
        // (identity, gameId) from another tx is a NEW row. Never overwrite,
        // never delete.
        Query::new(
            "INSERT OR IGNORE INTO collected_markers_v2 \
             (identity, gameId, txid, outputIndex, sigHex, createdAt) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(record.identity.as_str())
        .bind(record.game_id.as_str())
        .bind(record.txid.as_str())
        .bind(record.output_index)
        .bind(record.sig_hex.as_deref())
        .bind(current_unix_seconds_i64())
        .execute(&self.db)
        .await
        .map_err(collected_err)
    }

    async fn get_records_for(
        &self,
        identity: &str,
        game_id: &str,
    ) -> Result<Vec<CollectedRecord>, CollectedStorageError> {
        // Same bound and same newest-first order as the batched read — two
        // reads of one store that disagreed on either would be a boundary with
        // no pin (Rule 16).
        let rows: Vec<CollectedRow> = Query::new(format!(
            "SELECT identity, gameId, txid, outputIndex, sigHex FROM collected_markers_v2 \
             WHERE identity = ? AND gameId = ? \
             ORDER BY createdAt DESC, txid DESC, outputIndex DESC \
             LIMIT {COLLECTED_ROWS_PER_PAIR}"
        ))
        .bind(identity)
        .bind(game_id)
        .fetch_all(&self.db)
        .await
        .map_err(collected_err)?;
        Ok(rows.into_iter().map(CollectedRow::into_record).collect())
    }

    /// Batched pair lookup (bsv-low #289): one `gameId IN (…)` query per chunk
    /// instead of a D1 round trip per requested game. Alignment contract (input
    /// order, an EMPTY vec where no marker exists) preserved via a gameId-keyed
    /// map of LISTS — under the #327 S8 outpoint key a pair can hold many rows,
    /// so collapsing to one would silently re-create the censorship the re-key
    /// removes.
    async fn get_records(
        &self,
        identity: &str,
        game_ids: &[String],
    ) -> Result<Vec<Vec<CollectedRecord>>, CollectedStorageError> {
        if game_ids.is_empty() {
            return Ok(Vec::new());
        }
        // D1 caps bound parameters (100); 1 per gameId + the identity.
        const CHUNK: usize = 90;
        let mut by_game: std::collections::HashMap<String, Vec<CollectedRecord>> =
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
                by_game
                    .entry(record.game_id.clone())
                    .or_default()
                    .push(record);
            }
        }
        Ok(game_ids
            .iter()
            .map(|game_id| by_game.get(game_id).cloned().unwrap_or_default())
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

/// The result-marker columns, threaded through every window level.
const RESULT_COLS: &str = "gameId, winner, loser, potTxid, settleTxid, \
     winnerSigHex, loserSigHex, cardsHex, txid, outputIndex, createdAt";

/// `ls_result resultsFor` / `recentResults` — the leaderboard/history read
/// windows over `result_markers_v2` (bsv-low #282, the #281 class).
///
/// `tm_result` admission is BYTE-FORMAT-ONLY, so anyone can file a marker
/// naming any (gameId, winner, potTxid) for one dust `OP_RETURN`; under the
/// legacy flat `ORDER BY createdAt DESC LIMIT n` window, `n` junk rows
/// displaced every honest result — erasing a player's record from the
/// leaderboard read (and #276 established that a win RECORD is a product
/// promise). The #281 pattern applies:
///
///  - **Per-POT windowing**: `limit` counts POTS (a settled hand ≙ one pot),
///    and each pot yields up to [`PARTYFOR_ROWS_PER_GROUP`] rows as a
///    SUPERSET — verification-before-collapse: SQL cannot verify
///    `winnerSigHex`, so it must never pick "the real row"; the consumer
///    (the client / low-app-layer verify pass) checks sigs and keeps the
///    genuine one. Within a pot the OLDEST rows are kept — not because
///    oldest is honest (an attacker who pre-files during the hand beats it;
///    a post-hoc flood — the CHEAP variant — does not), but because it is
///    the one order later spam cannot improve on.
///  - **Pot-existence tier with the age-bounded oldest-first quota**
///    (#283a): markers naming invented pots are demoted behind every row
///    whose pot is indexed in `pot_records`; up to `quota` FRESH unknown
///    pots are promoted (a genuinely just-settled pot whose admission is
///    in flight must not be filtered), allocated oldest-first inside the
///    freshness window.
///  - **Explicit ORDER BY at every level**; pots newest-first by the pot's
///    own admission stamp (an attacker cannot move it by filing markers) —
///    "recent results" stays recent.
///
/// BINDS (numbered): winner-scoped — `?1` winner, `?2` limit (POTS), `?3`
/// quota, `?4` row cap; recent — `?1` limit, `?2` quota, `?3` row cap.
/// Residual, same as the family: markers naming REAL recent pots still
/// displace at ~limit dust cost; eviction WITHIN a pot costs
/// [`PARTYFOR_ROWS_PER_GROUP`] pre-filed rows.
pub fn result_window_sql(winner_scoped: bool) -> String {
    let (where_winner, b_limit, b_quota, b_cap) = if winner_scoped {
        ("WHERE rm.winner = ?1", "?2", "?3", "?4")
    } else {
        ("", "?1", "?2", "?3")
    };
    format!(
        "SELECT {cols} \
     FROM (SELECT {cols}, markerRowid, potCreatedAt, potFirstMarkerAt, tier, \
                  DENSE_RANK() OVER (ORDER BY tier ASC, \
                                              COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
                                              potTxid ASC) AS finalRank \
           FROM (SELECT {cols}, markerRowid, potCreatedAt, potFirstMarkerAt, \
                        CASE WHEN unknownPot = 0 \
                             OR (freshUnknown = 1 AND potRank <= {b_quota}) \
                             THEN 0 ELSE 1 END AS tier \
                 FROM (SELECT {cols}, markerRowid, potCreatedAt, potFirstMarkerAt, \
                              unknownPot, \
                              {fresh} AS freshUnknown, \
                              DENSE_RANK() OVER (PARTITION BY unknownPot, {fresh} \
                                                 ORDER BY COALESCE(potFirstMarkerAt, 0) ASC, \
                                                          potTxid ASC) AS potRank \
                       FROM (SELECT rm.gameId AS gameId, rm.winner AS winner, \
                                    rm.loser AS loser, rm.potTxid AS potTxid, \
                                    rm.settleTxid AS settleTxid, \
                                    rm.winnerSigHex AS winnerSigHex, \
                                    rm.loserSigHex AS loserSigHex, \
                                    rm.cardsHex AS cardsHex, \
                                    rm.txid AS txid, rm.outputIndex AS outputIndex, \
                                    rm.createdAt AS createdAt, rm.rowid AS markerRowid, \
                                    r.potCreatedAt AS potCreatedAt, \
                                    MIN(rm.createdAt) OVER (PARTITION BY rm.potTxid) \
                                        AS potFirstMarkerAt, \
                                    CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot, \
                                    ROW_NUMBER() OVER (PARTITION BY rm.potTxid \
                                                       ORDER BY rm.createdAt ASC, \
                                                                rm.rowid ASC) AS rn \
                             FROM result_markers_v2 rm \
                             LEFT JOIN (SELECT txid, MIN(createdAt) AS potCreatedAt \
                                        FROM pot_records GROUP BY txid) r \
                                    ON r.txid = rm.potTxid \
                             {where_winner}) \
                       WHERE rn <= {per_group}))) \
     WHERE finalRank <= {b_limit} \
     ORDER BY tier ASC, COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
              potTxid ASC, markerRowid ASC \
     LIMIT {b_cap}",
        cols = RESULT_COLS,
        per_group = overlay_discovery::result::storage::RESULT_ROWS_PER_POT,
        fresh = fresh_unknown_expr(),
    )
}

/// Row cap for a result window: `limit` pots x the per-pot superset. A
/// BELT, never a truncation (the rn filter already bounds it) — same
/// contract as `identity_window_row_cap`.
pub fn result_window_row_cap(limit: usize) -> usize {
    limit.saturating_mul(overlay_discovery::result::storage::RESULT_ROWS_PER_POT)
}

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
        // Per-pot superset window — see `result_window_sql` (bsv-low #282;
        // the flat newest-first window was dust-displaceable).
        let rows: Vec<ResultRow> = Query::new(result_window_sql(true))
            .bind(winner)
            .bind(limit as u32)
            .bind(unknown_pot_quota(limit) as u32)
            .bind(result_window_row_cap(limit) as u32)
            .fetch_all(&self.db)
            .await
            .map_err(result_err)?;
        Ok(rows.into_iter().map(ResultRow::into_record).collect())
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<ResultRecord>, ResultStorageError> {
        let rows: Vec<ResultRow> = Query::new(result_window_sql(false))
            .bind(limit as u32)
            .bind(unknown_pot_quota(limit) as u32)
            .bind(result_window_row_cap(limit) as u32)
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
/// The handle is a [`PotpartyDb`], NOT a bare `D1Database` — see
/// [`potparty_write`] for why that type exists and what it makes impossible.
pub struct D1PotpartyStorage {
    db: PotpartyDb,
}

impl D1PotpartyStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self {
            db: PotpartyDb::new(db),
        }
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
/// # ARCHITECTURE — verification before collapse, and then a STORED verdict
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
/// **What changed in bsv-low #283.** "SQL cannot verify a signature" is
/// still true, and "no sort order fixes this" was true only because nobody
/// had STORED the answer. `sigValid` is decoded once at admission
/// (`overlay_discovery::potparty::validity`, the #284 decode-at-write
/// pattern applied to a predicate) and
/// [`sig_rank_expr`](overlay_discovery::potparty::validity::sig_rank_expr)
/// is now the FIRST ordering term at every level of this window:
/// within-pot (`rn`), quota allocation (`potRank`), promotion (`tier`), and
/// the page itself (`finalRank`).
///
/// The architecture is unchanged where it matters: this query still does not
/// DECIDE anything (the latch is a sort key, never a `WHERE`; a 0-latched row
/// is still served, still in the superset, and the consumer still verifies
/// before collapsing). What it no longer does is hand the ORDER to the
/// attacker. And the reason the latch is not itself forgeable HERE is that
/// this window is scoped `WHERE pp.identity = ?1`: to appear in the victim's
/// answer at all a row must NAME the victim, and the identity signature is
/// over a challenge that binds that identity — so an attacker's row is
/// `sigValid = 0` by construction, whatever it does with stamps, volume, or
/// `pot_records`.
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
///     is exactly the pot a recovering client most needs. So up to `quota`
///     unknown pots are PROMOTED into the main tier — allocated AGE-BOUNDED
///     and OLDEST-FIRST, never by recency (bsv-low #283a: recency slots were
///     attacker-jumpable, since a ghost can always be newer but can never
///     backdate a server stamp) — see [`unknown_pot_quota`] /
///     [`UNKNOWN_POT_PROMOTION_MAX_AGE_SECS`]; the rest stay demoted but are
///     still served.
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
/// # Residual — and a CORRECTION to what the #281 revision claimed here
///
/// The paragraph this replaces said the #281 shape achieved "the outright
/// death of free invented-pot flooding" and priced the remaining attack at
/// "`limit` dust markers". **Both were wrong, and the second was wrong in the
/// direction that made the first look true.** bsv-low#347: `/submit` has no
/// auth and no rate limit, and its SEEN-gate is selected by a caller-supplied
/// `x-submit-mode` header — so filing a marker costs nothing, and so does
/// filing the `pot_records` row that makes a ghost pot read `unknownPot = 0`
/// and skip the quota path entirely (`tm_pot` admits any structurally
/// covenant-shaped output with no signature). The #281 window's ordering was
/// therefore free to defeat, not dust-priced, and the quota it introduced was
/// never on the attacker's path at all.
///
/// **What actually bounds it now is not a price and not a quota — it is that
/// the attacker cannot produce the victim's identity signature.** Every row
/// in this window names the victim (`WHERE pp.identity = ?1`), the identity
/// signature binds that name, and `sigValid` is latched from it at admission
/// and sorted on first. Executed:
/// `free_ghost_pot_records_cannot_erase_the_victims_pots_real_sqlite` — 200
/// free ghosts, each with its own fabricated `pot_records` row stamped NOW,
/// against a 100-pot page: **zero honest pots displaced** (the same 200 erase
/// an all-legacy page, executed as the control).
///
/// Residuals that remain, both bounded to the LEGACY tier (rows admitted
/// before the latch migration). That population **cannot grow, but it does
/// NOT drain on its own** — an earlier revision of this note claimed the
/// #252 republish sweep would land a latched row for any pot the honest
/// client still sees, and it will not: `decidePartyStep` stops the moment an
/// indexed row exists for the pot, and a legacy row is an indexed row. It is
/// permanent until the lazy RE-LATCH pass lands (bsv-low#355 — every row, not
/// just the `NULL` ones; see [`potparty_write::potparty_insert_query`] for
/// why a rank-0 row is equally unrecoverable):
///  - a legacy-vs-legacy contest is decided exactly as it was before, so an
///    attacker who filed junk BEFORE the migration keeps whatever advantage
///    that junk already had (`free_ghost_pot_records_do_erase_legacy_
///    unlatched_rows_real_sqlite`, `sustained_fresh_older_ghost_flood_
///    still_displaces_residual_real_sqlite`);
///  - within a pot, [`PARTYFOR_ROWS_PER_GROUP`] LEGACY junk rows still evict
///    a LEGACY honest row. A latched honest row is never evicted by junk at
///    any volume (`a_latched_marker_is_never_evicted_within_its_pot_
///    real_sqlite`, executed at 4x the group cap).
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
                  potCreatedAt, potFirstMarkerAt, potBestSigRank, tier, \
                  DENSE_RANK() OVER (ORDER BY potBestSigRank DESC, tier ASC, \
                                              COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
                                              potTxid ASC, potVout ASC) AS finalRank \
           FROM (SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
                        recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
                        txid, outputIndex, createdAt, isV2, markerRowid, \
                        potCreatedAt, potFirstMarkerAt, potBestSigRank, \
                        CASE WHEN unknownPot = 0 \
                             OR (freshUnknown = 1 AND potRank <= ?3) \
                             THEN 0 ELSE 1 END AS tier \
                 FROM (SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
                              recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
                              txid, outputIndex, createdAt, isV2, markerRowid, \
                              potCreatedAt, potFirstMarkerAt, unknownPot, \
                              potBestSigRank, \
                              {fresh} AS freshUnknown, \
                              DENSE_RANK() OVER (PARTITION BY unknownPot, {fresh} \
                                                 ORDER BY potBestSigRank DESC, \
                                                          COALESCE(potFirstMarkerAt, 0) ASC, \
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
                                    MAX({rank}) OVER (PARTITION BY pp.potTxid, pp.potVout) \
                                        AS potBestSigRank, \
                                    ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout, \
                                                                    CASE WHEN \
                                                                      pp.seatSettlePubkey IS NULL \
                                                                      THEN 0 ELSE 1 END \
                                                       ORDER BY {rank} DESC, \
                                                                pp.createdAt ASC, \
                                                                pp.rowid ASC) AS rn \
                             FROM potparty_records pp \
                             LEFT JOIN pot_records r \
                                    ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
                             WHERE pp.identity = ?1) \
                       WHERE rn <= {per_group}))) \
     WHERE finalRank <= ?2 \
     ORDER BY potBestSigRank DESC, tier ASC, \
              COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
              potTxid ASC, potVout ASC, isV2 DESC, markerRowid ASC \
     LIMIT ?4",
        per_group = PARTYFOR_ROWS_PER_GROUP,
        fresh = fresh_unknown_expr(),
        rank = overlay_discovery::potparty::validity::sig_rank_expr("pp."),
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
/// # UNRANKED — and it carries the highest-stakes rows (gate round 2, LOW-3)
///
/// **WARNING to a first consumer.** Unlike its potparty sibling this window
/// has NO validity rank: `potrefund_records` has no `sigValid` column, so the
/// ordering is `tier ASC, potCreatedAt DESC` and nothing else. Victim-named
/// potrefund markers and fabricated `pot_records` rows are BOTH free
/// (bsv-low#347: `/submit` admission is byte-format-only and its SEEN-gate is
/// caller-selected), so an attacker lands in tier 0, newest-first, ahead of
/// the honest backups — the #283 displacement this family's other windows
/// close, still open here.
///
/// It is not live today: no client consumes `ls_potrefund partyFor`
/// (`app/src/lib/overlay.ts` reaches `ls_potrefund` only via `byPot`). That
/// is the ONLY reason this is a note and not a defect. **Do not wire a
/// consumer to it without closing the ordering first** — latch and rank the
/// refund markers the way #283 ranked the potparty ones, or bind the pot's
/// committed keys. Whoever wires it inherits the gap silently otherwise.
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
                        CASE WHEN unknownPot = 0 \
                             OR (freshUnknown = 1 AND potRank <= ?3) \
                             THEN 0 ELSE 1 END AS tier \
                 FROM (SELECT identity, gameId, potTxid, potVout, refundRawHex, sigHex, \
                              txid, outputIndex, createdAt, markerRowid, \
                              potCreatedAt, potFirstMarkerAt, unknownPot, \
                              {fresh} AS freshUnknown, \
                              DENSE_RANK() OVER (PARTITION BY unknownPot, {fresh} \
                                                 ORDER BY COALESCE(potFirstMarkerAt, 0) ASC, \
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
        fresh = fresh_unknown_expr(),
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
/// # DELIBERATELY NOT RANK-ORDERED — and that is the FIX, not the residual
/// (bsv-low#354 / #356)
///
/// #283 made `potparty_records.sigValid` the leading `ORDER BY` term in SEVEN
/// queries. This is the eighth reader and it stays out of that family. State
/// the reasoning here rather than let a reader infer the family property from
/// the siblings (epoch Rule 8), because "the eighth one was forgotten" and
/// "the eighth one must not have it" look identical from outside:
///
///  1. **The rank is FORGEABLE in this window, so it would be a bar that does
///     not bar.** `sigValid = 1` means the marker's signatures verify under
///     the marker's OWN claimed identity and OWN claimed settle key. The
///     seven ranked windows are identity-scoped or committed-key-scoped, so a
///     junk row there must carry a signature from a key the attacker does not
///     hold. Here the only scope is `potTxid`/`potVout` — payload CLAIMS, not
///     this row's own outpoint — so a stranger names ITS OWN identity, signs
///     with ITS OWN key, and latches rank 2 for free, 100 times. Adding the
///     term would move nothing except what a reviewer believes is covered,
///     which is worse than not adding it (epoch Rule 1 / the "cited as
///     coverage" failure in Rule 9).
///  2. **A mutable rank would break the page stability that IS the fix.** The
///     `OFFSET` below is only safe because `(createdAt ASC, rowid ASC)` is
///     append-only: new markers land at the tail and can never shift a row
///     across a boundary the caller already fetched. `sigValid` is no longer
///     immutable — the #355/#367 re-latch sweep (`crate::relatch`) rewrites
///     it — so a rank-leading order would silently move rows BETWEEN pages
///     mid-enumeration, which is exactly how a paging client loses the row it
///     was paging toward.
///  3. The SQL is **shared with `POTREFUND_SELECT`**, and `potrefund_records`
///     has no `sigValid` column, so the term could not be added without a
///     second spelling of one query — a cost worth paying for a real bar and
///     not for one that is neither.
///
/// # What DOES close it: the caller can now page (bsv-low#354)
///
/// The window has been `LIMIT ? OFFSET ?` since #291 gate M2, and until now
/// **no caller could reach the OFFSET**: `PotpartyQuery::ByPot` declared no
/// such field and `list_for_pot` hard-bound 0. So the accepted mitigation
/// ("a client that suspects burial pages `offset += limit`") was advice with
/// no mechanism behind it — the Rule 13 failure where a truthful answer
/// leaves its consumer no way to reach the right data. ~100 free rows
/// (bsv-low#347: a marker is free, not dust-priced) stamped before the honest
/// seats evicted BOTH of them from `lookupPotPartyByPot`'s `attribute_seats`
/// fold, permanently, because rows are never deleted and `createdAt` is
/// server-assigned.
///
/// `byPot` now takes `offset`, so every admitted row is reachable and the
/// bound on a flood is the attacker's willingness to keep filing rather than
/// the caller's page size. The fail direction was already the good one
/// (attribution OMITTED, never wrong), which is why this was a display
/// surface rather than a money path — but "the honest row is unreachable" is
/// not a state to leave standing on a recovery-adjacent index.
///
/// The strictly stronger design remains available and is NOT this change:
/// bind the pot's COMMITTED settle keys (read from its own funding lock) and
/// serve this route through `seat_markers_sql`, which prefilters on keys an
/// attacker cannot name. That is a different wire and a different question;
/// it is worth doing when a consumer needs the overlay to have an opinion.
/// This consumer verifies every row itself, so REACHABILITY is what it was
/// missing.
///
/// Built from the caller's own SELECT list so the tests execute the SHIPPED
/// string rather than a transcription of it.
pub fn list_for_pot_sql(select: &str) -> String {
    format!(
        "{select} WHERE potTxid = ? AND potVout = ? \
         ORDER BY createdAt ASC, rowid ASC LIMIT ? OFFSET ?"
    )
}

/// THE `byPot` query — statement AND bind list — as a pure value, for both
/// callers (`ls_potparty` and `ls_potrefund`).
///
/// This exists because of a MEASURED gap, not for tidiness. `fetch_all` needs
/// a live `D1Database`, so a native test cannot watch the storage impl bind
/// anything (epoch Rule 22 — the same unreachability that let a latch column
/// be silently bound `NULL` in #283 with the whole suite green). Executing
/// [`list_for_pot_sql`] against real SQLite with hand-written params proves
/// the STATEMENT and says nothing about the BINDS: replacing the page-start
/// bind with a computed `0` in `list_for_pot` left every cell passing, which
/// is #354's fix silently inoperative. RED-verified, and this builder is the
/// remedy — the built value is inspectable (`Query::sql` / `Query::params`),
/// so `the_by_pot_query_binds_the_page_start_for_both_callers` observes the
/// exact four binds production sends.
///
/// One builder for both tables, so the pin covers both call sites rather than
/// one of them (epoch Rule 10: the durable fix for "these two must agree" is
/// deleting one of the copies).
pub fn by_pot_query(
    select: &str,
    pot_txid: &str,
    pot_vout: u32,
    limit: usize,
    offset: usize,
) -> Query {
    Query::new(list_for_pot_sql(select))
        .bind(pot_txid)
        .bind(pot_vout)
        .bind(limit as u32)
        .bind(offset as u32)
}

/// How many pots ABSENT from `pot_records` are promoted into the main tier
/// instead of being demoted behind every indexed pot (bsv-low #281 F3): a
/// STRICT existence tier silently becomes a FILTER once `limit` binds,
/// dropping exactly the fresh pot a recovering client most needs. One tenth
/// of the page, at least one slot.
///
/// # Quota ALLOCATION is age-bounded and oldest-first (bsv-low #283a)
///
/// The #281 revision allocated the quota slots by MARKER RECENCY — and an
/// attacker can ALWAYS be newer: 10 ghost markers filed after the victim's
/// funding demoted the victim's genuinely fresh pot (tm_pot admission still
/// in flight) out of the promoted set, executed in the #283 gate. Two facts
/// fix the allocation:
///
///  1. `createdAt` is SERVER-STAMPED at admission (`store_record` ignores
///     the record's value) — an attacker can be arbitrarily NEW but can
///     never BACKDATE;
///  2. an honest pot is "unknown" only for the in-flight window between its
///     marker admit and its `tm_pot` admit (the SAME gated submit family —
///     seconds to minutes; [`UNKNOWN_POT_PROMOTION_MAX_AGE_SECS`] bounds it
///     generously). A ghost pot stays unknown FOREVER.
///
/// So promotion is restricted to unknown pots whose FIRST marker is younger
/// than the freshness window (a stale unknown is a ghost with probability
/// →1 and is only ever demoted-not-dropped), and slots inside the window go
/// OLDEST-first — the one order an attacker cannot jump by filing MORE
/// markers after seeing the victim's funding.
///
/// **That allocation is now a BACKSTOP, not the bar.** It is a heuristic
/// over stamps, and a heuristic over stamps left a residual (a rolling
/// flood of ghosts kept continuously inside the freshness window and older
/// than the victim's funding moment still took the slots — and per
/// bsv-low#347 that flood is FREE, so the "O(sustained) cost" this note used
/// to claim was never a cost at all). What bounds it now is that
/// `potBestSigRank DESC` LEADS the page ordering, so a ghost sorts behind
/// every honest pot whether it was promoted or not — promotion stopped
/// being the decision. The stamp heuristic is kept because it still orders
/// the LEGACY tier, where no latch exists;
/// `sustained_fresh_older_ghost_flood_still_displaces_residual_real_sqlite`
/// pins that legacy behaviour and
/// `latched_ghosts_take_no_promoted_slot_real_sqlite` pins the closure for
/// admitted rows.
///
/// An earlier revision of this change ALSO gated promotion on
/// `potBestSigRank > 0`. It was deleted, not covered: the adversarial gate
/// removed the conjunct from the shipped SQL and measured identical results
/// at 0/50/200 ghosts with the honest row latching 1 or 0, because the
/// leading rank term dominates it. Epoch Rule 1 — a check proven unreachable
/// is re-derivation.
///
/// # The guaranteed ~10% ghost share — CLOSED for latched rows (#283b)
///
/// The quota ITSELF used to hand an invented-pot flood up to `limit/10`
/// promoted slots: 50 ghost markers displaced exactly `quota` real pots
/// (executed in the #281 gate; pre-#281 main displaced 50). The note that
/// stood here called that "the price of never filtering a genuinely fresh
/// pot" and said to revisit it only with priced admission. Priced admission
/// was never available — bsv-low#347 established that filing a marker is
/// free, not dust-priced — so the guarantee was being handed to an attacker
/// who paid nothing for it.
///
/// It is closed without pricing anything, and without filtering anything —
/// and NOT by anything done to the quota itself. `potBestSigRank DESC` is
/// the LEADING term of both the quota allocation and the page ordering, so a
/// promoted ghost still sorts behind every honest pot and promotion stopped
/// being the decision. A ghost naming the victim cannot carry the victim's
/// identity signature, so it cannot outrank one:
/// `latched_ghosts_take_no_promoted_slot_real_sqlite` re-measures the #281
/// case at 50 ghosts (5x the quota, all inside the freshness window and all
/// older than the honest marker) and displaces **zero** real pots.
///
/// This is an ORDERING, not a filter — the fail direction is unchanged. A pot
/// whose only marker latches `false` is DEMOTED, never dropped; it still
/// appears behind every indexed pot, which is exactly what a stale unknown
/// already did (`a_false_latch_never_removes_a_row_real_sqlite`).
pub fn unknown_pot_quota(limit: usize) -> usize {
    (limit / 10).max(1)
}

/// Freshness window for unknown-pot quota PROMOTION (bsv-low #283a): an
/// unknown pot only competes for promoted slots while its first marker is
/// younger than this. An honest pot's unknown phase is the marker-vs-tm_pot
/// admission race (seconds–minutes; both ride the same submit family), so
/// one hour is generous headroom for provider outages/retries while
/// guaranteeing every ghost ages OUT of the promoted set. Stale unknowns
/// are DEMOTED, never dropped — the pot stays reachable, just behind every
/// indexed pot.
pub const UNKNOWN_POT_PROMOTION_MAX_AGE_SECS: u64 = 3600;

/// The shared SQL expression for "this row's pot is a FRESH unknown"
/// (see [`UNKNOWN_POT_PROMOTION_MAX_AGE_SECS`]). `COALESCE(…, 0)`: a
/// NULL-stamped (pre-migration) marker reads as ancient → never promoted
/// (fail direction: demoted-but-served).
fn fresh_unknown_expr() -> String {
    format!(
        "CASE WHEN unknownPot = 1 AND COALESCE(potFirstMarkerAt, 0) >= unixepoch() - {UNKNOWN_POT_PROMOTION_MAX_AGE_SECS} THEN 1 ELSE 0 END"
    )
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

pub use potparty_write::{
    potparty_insert_query, potparty_relatch_query, LatchedPotpartyInsert, LatchedPotpartyRelatch,
    PotpartyDb,
};

/// The potparty write path as a CAPABILITY rather than a convention
/// (bsv-low #283, gate round 2 MED-2).
///
/// # What the first round got wrong
///
/// Round 1 split the INSERT into a pure producer (`potparty_insert_query`)
/// and gave it a real replay pin. That pin is genuine — and the SECOND gate
/// showed it pinned a function nobody was obliged to call: replacing
/// `store_record`'s body with an inline `INSERT … sigValid) VALUES (…, NULL)`
/// — same columns, same binds, latch dropped — left **293 passed, 0 failed**.
/// Every new production row would have landed in the legacy tier with #283
/// inoperative and the whole suite green. That is epoch Rule 22 verbatim: the
/// producer is unreachable natively (`execute` needs a live `D1Database`), so
/// the harness pins a seam and the seam's USE stays unobservable.
///
/// # The shape that closes it
///
/// A source-scanning pin would only have counted needles. Instead this module
/// removes the CAPABILITY to express the write any other way, which is how
/// the sibling bsv-low#347 lane closed the identical class (an exhaustive
/// match: "reading a struct field is optional; an enum arm is not"):
///
///  - [`LatchedPotpartyInsert`] has PRIVATE fields and exactly one
///    constructor, [`potparty_insert_query`], which binds the latch itself.
///  - [`PotpartyDb`] owns the `D1Database` in a PRIVATE field and exposes no
///    way to run an arbitrary write: `insert` takes a `LatchedPotpartyInsert`
///    and nothing else.
///  - `D1PotpartyStorage.db` is a `PotpartyDb`, so `store_record` — which
///    lives outside this module — cannot reach a `D1Database` at all.
///
/// So the gate's injection no longer compiles: an inline `Query` has no way
/// to be executed, and deleting the `potparty_insert_query` call leaves
/// nothing that can construct the only value `insert` accepts. A compile
/// error is strictly stronger than a failing test.
///
/// # The boundary, stated (epoch Rule 22)
///
/// This makes the CALL structurally mandatory. It does not make the D1
/// round-trip observable natively — nothing here can. Two residuals, both
/// deliberate and both louder than the hole they replace:
///
///  - `fetch_all` is generic over the SELECT list, so an author could try to
///    smuggle an INSERT through the READ method. Round 2 answered that with a
///    source-scanning count of the INSERT literal, and the round-3 gate broke
///    it by changing ONE keyword (`INSERT INTO` for `INSERT OR IGNORE INTO`)
///    — 294 passed, 0 failed, every new row binding NULL. A needle is one
///    keyword wide. The bar is now a CAPABILITY like everything else here:
///    [`potparty_write::is_select_only`], enforced in `fetch_all`, refuses
///    every non-`SELECT` regardless of spelling. The count pin stays behind
///    it as the belt.
///  - The predicate's verdict flowing into the bound column is pinned by
///    `the_admission_write_latches_sig_valid_through_the_real_writer`, which
///    replays this module's own SQL and bind list against real SQLite.
pub mod potparty_write {
    use super::{PotpartyRecord, Query};
    use serde::de::DeserializeOwned;
    use std::rc::Rc;
    use worker::D1Database;

    /// A potparty INSERT that PROVABLY carries the `sigValid` latch.
    ///
    /// Both fields are private to this module and there is exactly one
    /// constructor, so a value of this type is a proof that
    /// [`potparty_insert_query`] ran. [`PotpartyDb::insert`] accepts nothing
    /// else.
    pub struct LatchedPotpartyInsert {
        query: Query,
        sig_valid: bool,
    }

    impl LatchedPotpartyInsert {
        /// The verdict this insert BINDS — the same evaluation, not a second
        /// one (gate round 2 LOW-2: `record_sig_valid` used to run twice per
        /// admitted marker, once for telemetry and once for the bind, while
        /// two comments claimed "once"). Telemetry that reads this is
        /// reporting the value actually written, so a future single-derivation
        /// bug corrupts the signal too instead of hiding behind it.
        pub fn sig_valid(&self) -> bool {
            self.sig_valid
        }

        /// Read-only view of the built query, for the replay pin. Cannot be
        /// executed (`Query::execute` consumes `self`) and cannot be mutated.
        pub fn query(&self) -> &Query {
            &self.query
        }
    }

    /// THE potparty admission WRITE, as a pure value.
    ///
    /// `store_record` is `potparty_insert_query(...)` handed to
    /// [`PotpartyDb::insert`] and nothing else, because `execute` needs a
    /// live `D1Database` and is therefore unreachable in a native test —
    /// which is precisely how a write path gets silently neutered while the
    /// whole suite stays green. The #283 adversarial gate demonstrated
    /// exactly that: binding `None` for `sigValid` here left every test
    /// passing and made the entire change inoperative (every new production
    /// row would land in the legacy tier). Splitting the query out gives the
    /// writer a BEHAVIOURAL pin —
    /// `the_admission_write_latches_sig_valid_through_the_real_writer`
    /// replays this query's own SQL and bind list against real SQLite and
    /// reads the column back — and the module's private fields make the call
    /// itself unskippable (see the module doc).
    ///
    /// INSERT OR IGNORE on the `(txid, outputIndex)` primary key — a replayed
    /// submit of the same output is a no-op; markers for the same identity
    /// from different txs are ALL kept; never overwrite, never delete.
    ///
    /// `sigValid` is DECODED ONCE HERE — the #284 decode-at-write pattern
    /// applied to a predicate, and "once" is now literally true: the verdict
    /// is computed in this function and carried on
    /// [`LatchedPotpartyInsert::sig_valid`] for every other reader. It is an
    /// ORDERING HINT, not an admission decision: this cannot refuse a marker,
    /// a 0-latched row is stored and served exactly as before, and every
    /// consumer that concludes anything from a marker re-verifies. It exists
    /// because every downstream window is a slot allocated by an ordering an
    /// attacker controls, and "does this verify" is the one ordering an
    /// attacker can neither out-stamp nor out-number.
    ///
    /// # A latched `0` is a VERDICT, not "not yet checked" (bsv-low#355)
    ///
    /// A TRANSIENT predicate fault — a `bsv-rs` DER/`to_der` behaviour
    /// change, a wallet emitting a non-canonical signature during a rollout,
    /// a partial deploy — demotes every honest row admitted in that window to
    /// rank **0**, which sorts BELOW the legacy (`NULL`) tier. Its victims
    /// are wiped-device users seeing a silently short enumeration, i.e. the
    /// population least able to report it (Rule 14).
    ///
    /// This write is `INSERT OR IGNORE`, so it never revisits a row. Until
    /// bsv-low#355 nothing else did either, and the demotion was PERMANENT
    /// (the epoch Rule 6 trade: a self-healing failure swapped for a
    /// permanent one). The repair now exists and is the ONE other statement
    /// in this module that touches the column:
    /// [`potparty_relatch_query`], swept over EVERY row by
    /// `crate::relatch` — never a backfill of the `NULL` ones, because a
    /// criterion of "zero rows with `sigValid IS NULL`" structurally skips
    /// the 0s, which are the rows a fault would have created. Read a `0` as
    /// "this row's verdict is as old as the last sweep", never as
    /// "unchecked".
    pub fn potparty_insert_query(
        record: &PotpartyRecord,
        created_at: i64,
    ) -> LatchedPotpartyInsert {
        let sig_valid = overlay_discovery::potparty::validity::record_sig_valid(record);
        LatchedPotpartyInsert {
            query: Query::new(
                "INSERT OR IGNORE INTO potparty_records \
                 (identity, opponentIdentity, gameId, potTxid, potVout, \
                  recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
                  txid, outputIndex, createdAt, sigValid) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            .bind(created_at)
            .bind(i64::from(sig_valid)),
            sig_valid,
        }
    }

    /// A potparty RE-LATCH that PROVABLY carries a freshly recomputed
    /// `sigValid` (bsv-low#355).
    ///
    /// Same capability shape as [`LatchedPotpartyInsert`], for the same
    /// reason: private fields, exactly one constructor, and
    /// [`PotpartyDb::relatch`] accepts nothing else. The pass therefore
    /// cannot be handed a verdict to write — it can only ask for the row's
    /// verdict to be RE-DERIVED (epoch Rule 15: don't hand a call site a
    /// decision it can get wrong).
    pub struct LatchedPotpartyRelatch {
        query: Query,
        sig_valid: bool,
    }

    impl LatchedPotpartyRelatch {
        /// The verdict this UPDATE binds — the same evaluation, not a second
        /// one.
        pub fn sig_valid(&self) -> bool {
            self.sig_valid
        }

        /// Read-only view of the built query, for the replay pin.
        pub fn query(&self) -> &Query {
            &self.query
        }
    }

    /// THE potparty RE-LATCH write, as a pure value (bsv-low#355).
    ///
    /// `UPDATE`, never `INSERT OR REPLACE`: rows in this table are never
    /// rewritten, and only this one column may move. The row is addressed by
    /// its OUTPOINT — the primary key — so this cannot touch a second row
    /// however stale the cursor that produced the record is.
    ///
    /// The verdict is recomputed HERE, from the record as stored, by the same
    /// [`overlay_discovery::potparty::validity::record_sig_valid`] the
    /// admission write uses. "The pass's own predicate version" in #355's
    /// closure criterion is therefore literally this call.
    pub fn potparty_relatch_query(record: &PotpartyRecord) -> LatchedPotpartyRelatch {
        let sig_valid = overlay_discovery::potparty::validity::record_sig_valid(record);
        LatchedPotpartyRelatch {
            query: Query::new(
                "UPDATE potparty_records SET sigValid = ? \
                 WHERE txid = ? AND outputIndex = ?",
            )
            .bind(i64::from(sig_valid))
            .bind(record.txid.as_str())
            .bind(record.output_index),
            sig_valid,
        }
    }

    /// What [`PotpartyDb::fetch_all`] answers when handed a write.
    pub const NON_SELECT_ON_READ_PATH: &str = "potparty read path accepts SELECT only";

    /// Is this statement a READ? The bar [`PotpartyDb::fetch_all`] enforces.
    ///
    /// Kept pure and public so the bar is testable without a `D1Database` —
    /// the same reason [`potparty_insert_query`] exists. Deliberately
    /// keyword-blind rather than spelling-specific: it admits `SELECT` and
    /// refuses everything else, so `INSERT`, `INSERT OR IGNORE`,
    /// `INSERT OR REPLACE`, `REPLACE`, `UPDATE` and `DELETE` are all refused
    /// by the same clause and no future write spelling slips past a needle.
    ///
    /// BOUNDARY: a read that legitimately needs a leading `WITH` (a CTE) or a
    /// leading comment would be refused here. That is intended — widening
    /// this predicate should be a deliberate edit with its own cell, not a
    /// thing that happens by accident to a query builder.
    pub fn is_select_only(sql: &str) -> bool {
        sql.trim_start().to_ascii_uppercase().starts_with("SELECT")
    }

    /// The ONLY database handle [`super::D1PotpartyStorage`] holds.
    ///
    /// The inner `D1Database` is private to this module, so the storage impl
    /// — which lives outside it — has no way to run a write it did not build
    /// through [`potparty_insert_query`]. See the module doc.
    pub struct PotpartyDb(Rc<D1Database>);

    impl PotpartyDb {
        pub fn new(db: Rc<D1Database>) -> Self {
            Self(db)
        }

        /// Run a read. Generic over the row type, never over the write shape.
        ///
        /// GUARDED (bsv-low #283, gate round 3). The round-2 remediation left
        /// this door open and covered it with a source-scanning count of the
        /// INSERT literal — which pins a STRING, not a property. The gate
        /// changed exactly one keyword (`INSERT INTO` instead of
        /// `INSERT OR IGNORE INTO`), routed a NULL-binding write through here
        /// with `potparty_insert_query` still called and `record_sig_valid`
        /// still evaluated once, and got 294 passed / 0 failed with #283
        /// inoperative in production — round 1's exact defect, reconstituted
        /// inside its own remediation. My RED-verification had reused the
        /// injection's own spelling, which is epoch Rule 12a exactly: a pin
        /// verified only against the injection it was written for pins that
        /// injection.
        ///
        /// So the residual is now a CAPABILITY bar like the rest of this
        /// module, not a needle: a non-`SELECT` never reaches D1 from here,
        /// whatever it is spelled. [`is_select_only`] is pure, so the bar is
        /// unit-testable without a `D1Database` — which is the whole problem
        /// this module exists to solve.
        pub async fn fetch_all<T: DeserializeOwned>(&self, q: Query) -> Result<Vec<T>, String> {
            if !is_select_only(q.sql()) {
                return Err(NON_SELECT_ON_READ_PATH.to_string());
            }
            q.fetch_all(&self.0).await
        }

        /// Run THE potparty admission write. Accepts nothing that did not
        /// come from [`potparty_insert_query`].
        pub async fn insert(&self, insert: LatchedPotpartyInsert) -> Result<(), String> {
            insert.query.execute(&self.0).await
        }

        /// Run THE potparty re-latch write (bsv-low#355). Accepts nothing that
        /// did not come from [`potparty_relatch_query`], so this second write
        /// path cannot become the door the capability bar was built to close.
        pub async fn relatch(&self, update: LatchedPotpartyRelatch) -> Result<(), String> {
            update.query.execute(&self.0).await
        }
    }
}

#[async_trait(?Send)]
impl PotpartyStorage for D1PotpartyStorage {
    async fn store_record(&self, record: &PotpartyRecord) -> Result<(), PotpartyStorageError> {
        let insert = potparty_insert_query(record, current_unix_seconds_i64());
        // TELEMETRY, not a decision (bsv-low #283, gate M5). The golden cells
        // make a client/server crypto disagreement UNLIKELY; they do not make
        // it DETECTABLE once deployed, and that class fails toward refusing
        // HONEST work all at once (epoch Rule 16). A 0-latch is normal under
        // a marker flood and abnormal on a quiet topic, so the RATE is the
        // detector: a sustained stream of these with no flood in the logs
        // means our predicate and the client's signer have drifted, and the
        // right response is to look, not to change any behaviour here.
        // Surfaced rather than consumed (epoch Rule 13).
        //
        // Read off the insert rather than re-evaluated (gate round 2 LOW-2):
        // ONE `record_sig_valid` per admitted marker, and the log now reports
        // the value that is actually being written.
        if !insert.sig_valid() {
            worker::console_log!(
                "[potparty:siginvalid] txid={} vout={} v2={} identity={}",
                record.txid,
                record.output_index,
                record.seat_settle_pubkey.is_some(),
                record.identity
            );
        }
        self.db.insert(insert).await.map_err(potparty_err)
    }

    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError> {
        // Per-pot + existence-tiered window — see
        // `potparty_list_for_identity_sql` for the dust-DoS this shape closes
        // (bsv-low #281).
        let rows: Vec<PotpartyRow> = self
            .db
            .fetch_all(
                Query::new(potparty_list_for_identity_sql())
                    .bind(identity)
                    .bind(limit as u32)
                    .bind(unknown_pot_quota(limit) as u32)
                    .bind(identity_window_row_cap(limit, 2) as u32),
            )
            .await
            .map_err(potparty_err)?;
        Ok(rows.into_iter().map(PotpartyRow::into_record).collect())
    }

    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError> {
        // OLDEST FIRST, offset-pageable — see `list_for_pot_sql` for why
        // paging rather than a rank is the answer for THIS window
        // (bsv-low #281, #291 gate M2, #354/#356). Until #354 this bound a
        // literal 0 and no caller could ask for anything else.
        let rows: Vec<PotpartyRow> = self
            .db
            .fetch_all(by_pot_query(
                POTPARTY_SELECT,
                pot_txid,
                pot_vout,
                limit,
                offset,
            ))
            .await
            .map_err(potparty_err)?;
        Ok(rows.into_iter().map(PotpartyRow::into_record).collect())
    }
}

// ── #355 RE-LATCH: potparty_records ─────────────────────────────────────────

/// The table name `relatch_cursors` keys this sweep by, and the log label.
pub const POTPARTY_TABLE: &str = "potparty_records";

/// The re-latch SCAN — every column [`record_sig_valid`] needs, plus the
/// `rowid` the cursor rides on and the STORED verdict the pass compares
/// against.
///
/// [`overlay_discovery::potparty::validity::record_sig_valid`]: the predicate
///
/// **`WHERE sigValid IS NULL` is deliberately absent and must stay absent.**
/// A NULL-only filter skips every row a transient predicate fault latched `0`
/// — the exact population #355 exists to repair, and the population that
/// sorts BELOW even the legacy tier. The verdict column appears in the SELECT
/// list and NOWHERE else in this statement.
fn potparty_relatch_scan_sql() -> String {
    use overlay_discovery::potparty::validity::SIG_VALID_COLUMN;
    format!(
        "SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
            recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
            txid, outputIndex, createdAt, rowid, {SIG_VALID_COLUMN} \
         FROM potparty_records WHERE rowid > ? ORDER BY rowid ASC LIMIT ?"
    )
}

/// Both convergence readouts in one aggregate: rows left in this sweep, and
/// the size of the legacy tier. Reported, never used as a filter.
fn potparty_relatch_census_sql() -> String {
    use overlay_discovery::potparty::validity::SIG_VALID_COLUMN;
    format!(
        "SELECT COALESCE(SUM(CASE WHEN rowid > ?1 THEN 1 ELSE 0 END), 0) AS remaining, \
                COALESCE(SUM(CASE WHEN {SIG_VALID_COLUMN} IS NULL THEN 1 ELSE 0 END), 0) \
                    AS stillNull \
         FROM potparty_records"
    )
}

/// A scanned potparty row: the record, its `rowid`, and its STORED verdict.
#[derive(Deserialize)]
struct PotpartyRelatchDbRow {
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
    #[serde(rename = "seatSettlePubkey", default)]
    seat_settle_pubkey: Option<String>,
    #[serde(rename = "seatSigHex", default)]
    seat_sig_hex: Option<String>,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "createdAt")]
    created_at: Option<f64>,
    rowid: f64,
    #[serde(rename = "sigValid", default)]
    sig_valid: Option<f64>,
}

/// The pass's row shape for `potparty_records`.
pub struct PotpartyRelatchRow {
    rowid: i64,
    stored: Option<bool>,
    record: PotpartyRecord,
}

impl PotpartyRelatchDbRow {
    fn into_row(self) -> PotpartyRelatchRow {
        PotpartyRelatchRow {
            rowid: self.rowid as i64,
            stored: self.sig_valid.map(|v| v != 0.0),
            record: PotpartyRecord {
                identity: self.identity,
                opponent_identity: self.opponent_identity,
                game_id: self.game_id,
                pot_txid: self.pot_txid,
                pot_vout: self.pot_vout as u32,
                recovery_height: self.recovery_height as u32,
                sig_hex: self.sig_hex.unwrap_or_default(),
                seat_settle_pubkey: self.seat_settle_pubkey,
                seat_sig_hex: self.seat_sig_hex,
                txid: self.txid,
                output_index: self.output_index as u32,
                created_at: self.created_at.unwrap_or(0.0) as i64,
            },
        }
    }
}

#[derive(Deserialize)]
struct RelatchCensusRow {
    remaining: f64,
    #[serde(rename = "stillNull")]
    still_null: f64,
}

#[async_trait(?Send)]
impl crate::relatch::RelatchTable for D1PotpartyStorage {
    type Row = PotpartyRelatchRow;

    fn table(&self) -> &'static str {
        POTPARTY_TABLE
    }
    fn rowid(row: &Self::Row) -> i64 {
        row.rowid
    }
    fn stored(row: &Self::Row) -> Option<bool> {
        row.stored
    }

    async fn scan(&self, after_rowid: i64, limit: u64) -> Result<Vec<Self::Row>, String> {
        let rows: Vec<PotpartyRelatchDbRow> = self
            .db
            .fetch_all(
                Query::new(potparty_relatch_scan_sql())
                    .bind(after_rowid)
                    .bind(limit as u32),
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(PotpartyRelatchDbRow::into_row)
            .collect())
    }

    async fn relatch_if_changed(&self, row: &Self::Row) -> Result<Option<bool>, String> {
        // ONE evaluation of the predicate, inside the capability-typed UPDATE,
        // and the compare reads it off that value — never a second derivation
        // (the #283 gate's LOW-2 finding, avoided rather than repeated).
        let update = potparty_relatch_query(&row.record);
        let verdict = update.sig_valid();
        if row.stored == Some(verdict) {
            return Ok(None);
        }
        self.db.relatch(update).await?;
        Ok(Some(verdict))
    }

    async fn census(&self, after_rowid: i64) -> Result<crate::relatch::RelatchCensus, String> {
        let rows: Vec<RelatchCensusRow> = self
            .db
            .fetch_all(Query::new(potparty_relatch_census_sql()).bind(after_rowid))
            .await?;
        let r = rows.into_iter().next().ok_or("census returned no row")?;
        Ok(crate::relatch::RelatchCensus {
            remaining: r.remaining as u64,
            still_null: r.still_null as u64,
        })
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
        // (bsv-low #281 / #291 gate M2). Statement AND binds come from the
        // SHARED `by_pot_query`, so the pin on that builder covers this call
        // site too (bsv-low#354: a native test cannot watch a `fetch_all`
        // bind anything).
        let rows: Vec<PotrefundRow> =
            by_pot_query(POTREFUND_SELECT, pot_txid, pot_vout, limit, offset)
                .fetch_all(&self.db)
                .await
                .map_err(potrefund_err)?;
        Ok(rows.into_iter().map(PotrefundRow::into_record).collect())
    }
}

// =============================================================================
// D1HoppartyStorage (bsv-low #315)
// =============================================================================

/// Row for hopparty-marker queries. TEXT columns arrive as `String`;
/// `hopVout` / `hopSats` / `outputIndex` / `createdAt` are INTEGER columns
/// but D1 returns numbers as f64. (`hopSats` as f64 is exact for every
/// value below 2^53 — far beyond any real hop; the same tolerance every
/// sats-bearing row type in this file accepts.)
#[derive(Deserialize)]
struct HoppartyRow {
    identity: String,
    #[serde(rename = "opponentIdentity")]
    opponent_identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "hopVout")]
    hop_vout: f64,
    #[serde(rename = "hopSats")]
    hop_sats: f64,
    #[serde(rename = "seatSettlePubkey")]
    seat_settle_pubkey: String,
    #[serde(rename = "seatSigHex")]
    seat_sig_hex: String,
    #[serde(rename = "identitySigHex")]
    identity_sig_hex: String,
    /// The CONTAINER's decoded facts (#310 decode-at-write).
    #[serde(rename = "hopLockHex")]
    hop_lock_hex: Option<String>,
    #[serde(rename = "hopSatsOnChain")]
    hop_sats_on_chain: Option<f64>,
    #[serde(rename = "containerOutputs")]
    container_outputs: f64,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "createdAt")]
    created_at: Option<f64>,
}

impl HoppartyRow {
    fn into_record(self) -> HoppartyRecord {
        HoppartyRecord {
            identity: self.identity,
            opponent_identity: self.opponent_identity,
            game_id: self.game_id,
            hop_vout: self.hop_vout as u32,
            hop_sats: self.hop_sats as u64,
            seat_settle_pubkey: self.seat_settle_pubkey,
            seat_sig_hex: self.seat_sig_hex,
            identity_sig_hex: self.identity_sig_hex,
            hop_lock_hex: self.hop_lock_hex,
            hop_sats_on_chain: self.hop_sats_on_chain.map(|v| v as u64),
            container_outputs: self.container_outputs as u32,
            txid: self.txid,
            output_index: self.output_index as u32,
            created_at: self.created_at.unwrap_or(0.0) as i64,
        }
    }
}

pub use hopparty_write::{
    hopparty_insert_query, hopparty_relatch_query, HoppartyDb, LatchedHoppartyInsert,
    LatchedHoppartyRelatch,
};

/// The hopparty write path as a CAPABILITY rather than a convention
/// (bsv-low #362) — the same shape [`potparty_write`] landed for #283, for
/// the same measured reason.
///
/// `store_record` needs a live `D1Database`, so it is unreachable in a
/// native test. That is precisely how a write path gets silently neutered
/// while the whole suite stays green: #283's adversarial gate replaced
/// `store_record`'s body with an inline `INSERT … VALUES (…, NULL)` — same
/// columns, same binds, latch dropped — and got **293 passed, 0 failed**,
/// with every new production row landing in the legacy tier. A source-scan
/// remediation was then defeated twice, once by a KEYWORD and once by CASE
/// (epoch Rule 12a): you cannot enumerate your way to a property.
///
/// So this module removes the CAPABILITY to express the write any other way:
///
///  - [`LatchedHoppartyInsert`] has PRIVATE fields and exactly one
///    constructor, [`hopparty_insert_query`], which binds the latch itself.
///  - [`HoppartyDb`] owns the `D1Database` in a PRIVATE field and exposes no
///    way to run an arbitrary write: `insert` takes a
///    [`LatchedHoppartyInsert`] and nothing else, and the read method is
///    barred to `SELECT` by the shared runtime predicate
///    [`potparty_write::is_select_only`] — ONE bar for both tables, phrased
///    as a property of the input rather than a list of forbidden spellings.
///  - `D1HoppartyStorage.db` is a [`HoppartyDb`], so `store_record` — which
///    lives outside this module — cannot reach a `D1Database` at all.
///
/// The gate's injection therefore no longer compiles: an inline `Query` has
/// no way to be executed, and deleting the `hopparty_insert_query` call
/// leaves nothing that can construct the only value `insert` accepts.
///
/// # The boundary, stated (epoch Rule 22)
///
/// This makes the CALL structurally mandatory. It does not make the D1
/// round-trip observable natively — nothing here can. The predicate's
/// verdict flowing into the bound column is pinned instead by
/// `the_hopparty_admission_write_latches_marker_valid_through_the_real_writer`,
/// which replays this module's own SQL and bind list against real SQLite and
/// reads the column back.
pub mod hopparty_write {
    use super::{potparty_write::is_select_only, HoppartyRecord, Query};
    use serde::de::DeserializeOwned;
    use std::rc::Rc;
    use worker::D1Database;

    /// A hopparty INSERT that PROVABLY carries the `markerValid` latch.
    ///
    /// Both fields are private to this module and there is exactly one
    /// constructor, so a value of this type is a proof that
    /// [`hopparty_insert_query`] ran. [`HoppartyDb::insert`] accepts nothing
    /// else.
    pub struct LatchedHoppartyInsert {
        query: Query,
        marker_valid: bool,
    }

    impl LatchedHoppartyInsert {
        /// The verdict this insert BINDS — the same evaluation, not a second
        /// one. Telemetry reading this is reporting the value actually
        /// written, so a future single-derivation bug corrupts the signal too
        /// instead of hiding behind it (the #283 round-2 LOW-2 lesson).
        pub fn marker_valid(&self) -> bool {
            self.marker_valid
        }

        /// Read-only view of the built query, for the replay pin. Cannot be
        /// executed (`Query::execute` consumes `self`) and cannot be mutated.
        pub fn query(&self) -> &Query {
            &self.query
        }
    }

    /// THE hopparty admission WRITE, as a pure value.
    ///
    /// `INSERT OR IGNORE` on the `(txid, outputIndex)` primary key — a
    /// replayed submit of the same output is a no-op; markers for the same
    /// identity from different txs are ALL kept; never overwrite, never
    /// delete. `createdAt` is the SERVER's stamp (the record's own value is
    /// ignored by the caller).
    ///
    /// `markerValid` is DECODED ONCE HERE, from facts already in hand: the
    /// container's decoded output at `hopVout` (#310) and the two signatures
    /// the marker carries. It is an ORDERING HINT, not an admission
    /// decision: this cannot refuse a marker, a 0-latched row is stored and
    /// served exactly as before, and `/hops-view` returns both signatures so
    /// the client re-verifies.
    ///
    /// # A latched `0` is a VERDICT, not "not yet checked" (bsv-low#367)
    ///
    /// A TRANSIENT predicate fault demotes every honest row admitted in that
    /// window to rank **0**, below even the legacy `NULL` tier — the epoch
    /// Rule 6 trade, whose victims are wiped-device users seeing a silently
    /// short enumeration (Rule 14). This write is `INSERT OR IGNORE` and
    /// never revisits a row; until bsv-low#367 nothing else did either, and
    /// for THIS table there was not even a republish that could (a hop marker
    /// rides a transaction already on chain). The repair is
    /// [`hopparty_relatch_query`], swept over EVERY row by `crate::relatch` —
    /// never a backfill of the `NULL` ones.
    pub fn hopparty_insert_query(
        record: &HoppartyRecord,
        created_at: i64,
    ) -> LatchedHoppartyInsert {
        let marker_valid = overlay_discovery::hopparty::validity::record_marker_valid(record);
        LatchedHoppartyInsert {
            query: Query::new(
                "INSERT OR IGNORE INTO hopparty_records \
                 (identity, opponentIdentity, gameId, hopVout, hopSats, \
                  seatSettlePubkey, seatSigHex, identitySigHex, hopLockHex, \
                  hopSatsOnChain, containerOutputs, txid, outputIndex, createdAt, \
                  markerValid) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(record.identity.as_str())
            .bind(record.opponent_identity.as_str())
            .bind(record.game_id.as_str())
            .bind(record.hop_vout)
            .bind(record.hop_sats)
            .bind(record.seat_settle_pubkey.as_str())
            .bind(record.seat_sig_hex.as_str())
            .bind(record.identity_sig_hex.as_str())
            .bind(record.hop_lock_hex.as_deref())
            .bind(record.hop_sats_on_chain)
            .bind(record.container_outputs)
            .bind(record.txid.as_str())
            .bind(record.output_index)
            .bind(created_at)
            .bind(i64::from(marker_valid)),
            marker_valid,
        }
    }

    /// A hopparty RE-LATCH that PROVABLY carries a freshly recomputed
    /// `markerValid` (bsv-low#367) — the [`LatchedPotpartyRelatch`] shape,
    /// for the same reason and with the same guarantees.
    pub struct LatchedHoppartyRelatch {
        query: Query,
        marker_valid: bool,
    }

    impl LatchedHoppartyRelatch {
        /// The verdict this UPDATE binds — the same evaluation, not a second
        /// one.
        pub fn marker_valid(&self) -> bool {
            self.marker_valid
        }

        /// Read-only view of the built query, for the replay pin.
        pub fn query(&self) -> &Query {
            &self.query
        }
    }

    /// THE hopparty RE-LATCH write, as a pure value (bsv-low#367).
    ///
    /// `UPDATE` on the OUTPOINT primary key; only `markerValid` moves. The
    /// verdict is recomputed here by
    /// [`overlay_discovery::hopparty::validity::record_marker_valid`] from
    /// facts already in the row — including the container's decoded output
    /// (#310) — so the pass needs no BEEF re-parse and no chain read.
    ///
    /// This is the ONLY repair path this table can ever have: a hopparty
    /// marker rides the hop transaction, which is already on chain, so no
    /// republish can re-latch a legacy row.
    pub fn hopparty_relatch_query(record: &HoppartyRecord) -> LatchedHoppartyRelatch {
        let marker_valid = overlay_discovery::hopparty::validity::record_marker_valid(record);
        LatchedHoppartyRelatch {
            query: Query::new(
                "UPDATE hopparty_records SET markerValid = ? \
                 WHERE txid = ? AND outputIndex = ?",
            )
            .bind(i64::from(marker_valid))
            .bind(record.txid.as_str())
            .bind(record.output_index),
            marker_valid,
        }
    }

    /// The ONLY database handle [`super::D1HoppartyStorage`] holds.
    pub struct HoppartyDb(Rc<D1Database>);

    impl HoppartyDb {
        pub fn new(db: Rc<D1Database>) -> Self {
            Self(db)
        }

        /// Run a read. Generic over the row type, never over the write shape
        /// — and guarded by the SHARED [`is_select_only`] capability bar, so
        /// no write spelling can be smuggled through the read method (the
        /// #283 round-3 finding: a needle is one keyword wide).
        pub async fn fetch_all<T: DeserializeOwned>(&self, q: Query) -> Result<Vec<T>, String> {
            if !is_select_only(q.sql()) {
                return Err(super::potparty_write::NON_SELECT_ON_READ_PATH.to_string());
            }
            q.fetch_all(&self.0).await
        }

        /// Run THE hopparty admission write. Accepts nothing that did not
        /// come from [`hopparty_insert_query`].
        pub async fn insert(&self, insert: LatchedHoppartyInsert) -> Result<(), String> {
            insert.query.execute(&self.0).await
        }

        /// Run THE hopparty re-latch write (bsv-low#367). Accepts nothing
        /// that did not come from [`hopparty_relatch_query`].
        pub async fn relatch(&self, update: LatchedHoppartyRelatch) -> Result<(), String> {
            update.query.execute(&self.0).await
        }
    }
}

/// Cloudflare D1 implementation of the HoppartyStorage trait
/// (tm_hopparty / ls_hopparty, bsv-low #315).
///
/// Schema: `hopparty_records` in `d1::OVERLAY_MIGRATIONS` — every WIRE field
/// typed + indexed from the FIRST migration (#310 decode-at-write), plus the
/// #362 `markerValid` verdict latch. Keyed by the marker OUTPOINT (txid,
/// outputIndex); `INSERT OR IGNORE` makes a replayed submit of the same
/// output a no-op, while markers for the same identity from DIFFERENT txs
/// are ALL kept (the censorship-front-run fix). Rows are NEVER deleted.
/// `createdAt` is stamped here at insert (the record's value is ignored).
pub struct D1HoppartyStorage {
    /// A [`HoppartyDb`], NOT a `D1Database` — so this impl cannot express a
    /// write that skips the latch (see [`hopparty_write`]).
    db: HoppartyDb,
}

impl D1HoppartyStorage {
    pub fn new(db: Rc<D1Database>) -> Self {
        Self {
            db: HoppartyDb::new(db),
        }
    }
}

fn hopparty_err(e: String) -> HoppartyStorageError {
    HoppartyStorageError::Database(e)
}

const HOPPARTY_SELECT: &str = "SELECT identity, opponentIdentity, gameId, hopVout, \
     hopSats, seatSettlePubkey, seatSigHex, identitySigHex, hopLockHex, hopSatsOnChain, \
     containerOutputs, txid, outputIndex, createdAt \
     FROM hopparty_records";

/// `ls_hopparty hopsFor` — the identity-scoped hops-in-flight window: the
/// EXACT `potrefund_list_for_identity_sql` shape (per-OUTPOINT collapse to
/// a bounded superset, hop-existence tier against `pot_records` — hops ARE
/// indexed there via `tm_lowfund` — with a reserved unknown quota, fully
/// explicit ORDER BY at every level, `limit` counting OUTPOINTS) with two
/// differences:
///
///  - the hop OUTPOINT is `(txid, hopVout)` — the marker rides the hop tx,
///    so the containing txid IS the hop txid — and each outpoint yields up
///    to [`HOPSFOR_ROWS_PER_OUTPOINT`] OLDEST rows (a SUPERSET, not a
///    representative: a layer that picks "the real row" before anything
///    verifies hands an attacker the eviction; see the constant's doc in
///    `overlay_discovery::hopparty`);
///  - there is no v1/v2 group split (one version), so the row cap is
///    `limit × HOPSFOR_ROWS_PER_OUTPOINT`.
///
/// Read `potparty_list_for_identity_sql`'s doc for the dust-DoS these
/// bounds close and the residual they do not.
///
/// # The LATCHED VERDICT leads (bsv-low #362)
///
/// Same sweep as `/hops-view`, for the same reason: this window is a
/// fixed-size slot over attacker-writable rows, and the one ordering an
/// attacker can neither out-stamp nor out-number is *does this marker
/// verify*. `markerValid` is decided at admission, so the window can lead on
/// it — aggregated per outpoint via `MAX`, because `finalRank` counts
/// OUTPOINTS and a key that differs between two rows of one outpoint would
/// split it across ranks.
///
/// **Sort key, never a `WHERE`.** A refuted or legacy row is deprioritised,
/// served, and carried back with both signatures verbatim so the caller
/// re-verifies. Hiding it would recreate the invisible-money class (#358).
///
/// BINDS (numbered): `?1` identity, `?2` limit (OUTPOINTS), `?3` quota
/// (unknown-hop promotion slots), `?4` row cap.
pub fn hopparty_list_for_identity_sql() -> String {
    format!(
        "SELECT identity, opponentIdentity, gameId, hopVout, hopSats, \
            seatSettlePubkey, seatSigHex, identitySigHex, hopLockHex, hopSatsOnChain, \
            containerOutputs, txid, outputIndex, createdAt \
     FROM (SELECT identity, opponentIdentity, gameId, hopVout, hopSats, \
                  seatSettlePubkey, seatSigHex, identitySigHex, hopLockHex, hopSatsOnChain, \
                  containerOutputs, txid, outputIndex, createdAt, markerRowid, \
                  potCreatedAt, potFirstMarkerAt, markerRank, outpointMarkerRank, tier, \
                  DENSE_RANK() OVER (ORDER BY outpointMarkerRank DESC, tier ASC, \
                                              COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
                                              txid ASC, hopVout ASC) AS finalRank \
           FROM (SELECT identity, opponentIdentity, gameId, hopVout, hopSats, \
                        seatSettlePubkey, seatSigHex, identitySigHex, hopLockHex, \
                        hopSatsOnChain, containerOutputs, txid, outputIndex, createdAt, \
                        markerRowid, potCreatedAt, potFirstMarkerAt, \
                        markerRank, outpointMarkerRank, \
                        CASE WHEN unknownPot = 0 \
                             OR (freshUnknown = 1 AND potRank <= ?3) \
                             THEN 0 ELSE 1 END AS tier \
                 FROM (SELECT identity, opponentIdentity, gameId, hopVout, hopSats, \
                              seatSettlePubkey, seatSigHex, identitySigHex, hopLockHex, \
                              hopSatsOnChain, containerOutputs, txid, outputIndex, createdAt, \
                              markerRowid, potCreatedAt, potFirstMarkerAt, unknownPot, \
                              markerRank, \
                              MAX(markerRank) OVER (PARTITION BY txid, hopVout) \
                                  AS outpointMarkerRank, \
                              {fresh} AS freshUnknown, \
                              DENSE_RANK() OVER (PARTITION BY unknownPot, {fresh} \
                                                 ORDER BY COALESCE(potFirstMarkerAt, 0) ASC, \
                                                          txid ASC, hopVout ASC) AS potRank \
                       FROM (SELECT hp.identity AS identity, \
                                    hp.opponentIdentity AS opponentIdentity, \
                                    hp.gameId AS gameId, hp.hopVout AS hopVout, \
                                    hp.hopSats AS hopSats, \
                                    hp.seatSettlePubkey AS seatSettlePubkey, \
                                    hp.seatSigHex AS seatSigHex, \
                                    hp.identitySigHex AS identitySigHex, \
                                    hp.hopLockHex AS hopLockHex, \
                                    hp.hopSatsOnChain AS hopSatsOnChain, \
                                    hp.containerOutputs AS containerOutputs, \
                                    hp.txid AS txid, hp.outputIndex AS outputIndex, \
                                    hp.createdAt AS createdAt, hp.rowid AS markerRowid, \
                                    {marker_rank} AS markerRank, \
                                    r.createdAt AS potCreatedAt, \
                                    MIN(hp.createdAt) OVER (PARTITION BY hp.txid, \
                                                                         hp.hopVout) \
                                        AS potFirstMarkerAt, \
                                    CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot, \
                                    ROW_NUMBER() OVER (PARTITION BY hp.txid, hp.hopVout \
                                                       ORDER BY hp.createdAt ASC, \
                                                                hp.rowid ASC) AS rn \
                             FROM hopparty_records hp \
                             LEFT JOIN pot_records r \
                                    ON r.txid = hp.txid AND r.outputIndex = hp.hopVout \
                             WHERE hp.identity = ?1) \
                       WHERE rn <= {per_outpoint}))) \
     WHERE finalRank <= ?2 \
     ORDER BY outpointMarkerRank DESC, tier ASC, \
              COALESCE(potCreatedAt, potFirstMarkerAt) DESC, \
              txid ASC, hopVout ASC, markerRank DESC, createdAt ASC, markerRowid ASC \
     LIMIT ?4",
        per_outpoint = HOPSFOR_ROWS_PER_OUTPOINT,
        fresh = fresh_unknown_expr(),
        // The SHARED rank expression — the column name and its three tiers
        // live in the crate that WRITES them (epoch Rule 16).
        marker_rank = overlay_discovery::hopparty::validity::marker_rank_expr("hp."),
    )
}

/// `ls_hopparty byHop` — OLDEST first (the honest marker rides the hop tx
/// itself, so it is permanently at the head of the window — the #281 byPot
/// rationale), one page, no cursor. Built from the shared select so tests
/// execute the SHIPPED string.
///
/// # Why this window is safe UNRANKED and UNPAGED, while `byPot` is not
///
/// The #362 gate accepted this as the one `hopparty_records` reader that does
/// not lead on `markerValid`. That acceptance turned on an argument nobody
/// had written down, so write it down (epoch Rule 8) — "we decided this is
/// fine" and "we forgot this one" are indistinguishable from outside, and the
/// sibling potparty window reached the OPPOSITE conclusion on the same
/// question (bsv-low#354/#356).
///
/// **The partition is UNCROWDABLE by a third party.** `txid` here is the
/// marker's OWN container transaction — the primary key's first half, a
/// hash-bound fact — not a claim inside the payload. Every row in
/// `WHERE txid = ? AND hopVout = ?` is therefore an output of ONE
/// transaction, and the primary key `(txid, outputIndex)` admits at most one
/// row per output. A stranger cannot add an output to a transaction that
/// already exists, so the partition's size is fixed by whoever built the hop
/// transaction, and a flood is not representable. Neither a rank nor a cursor
/// buys anything against an adversary who cannot put a row in the set.
///
/// `list_for_pot_sql`'s partition is the opposite: `potTxid`/`potVout` are
/// CLAIMS in the marker payload, so anyone can file unlimited rows naming a
/// victim's pot from their own transactions. That window needs the cursor,
/// and the rank would not have helped it. The distinction is exactly "is this
/// key the row's own outpoint, or something the row asserts?" — worth asking
/// of every partition in this file.
pub fn hopparty_list_for_hop_sql() -> String {
    format!(
        "{HOPPARTY_SELECT} WHERE txid = ? AND hopVout = ? \
         ORDER BY createdAt ASC, rowid ASC LIMIT ?"
    )
}

#[async_trait(?Send)]
impl HoppartyStorage for D1HoppartyStorage {
    async fn store_record(&self, record: &HoppartyRecord) -> Result<(), HoppartyStorageError> {
        let insert = hopparty_insert_query(record, current_unix_seconds_i64());
        // TELEMETRY, not a decision (the #283 gate-M5 posture). The frozen
        // cross-repo golden makes a client/server crypto disagreement
        // UNLIKELY; it does not make it DETECTABLE once deployed, and that
        // class fails toward refusing HONEST work all at once (epoch
        // Rule 16). A 0-latch is normal under a marker flood and abnormal on
        // a quiet topic, so the RATE is the detector: a sustained stream of
        // these with no flood in the logs means our predicate and the
        // client's signer have drifted, and the right response is to look,
        // not to change any behaviour here (epoch Rule 13).
        //
        // Read off the insert rather than re-evaluated: ONE
        // `record_marker_valid` per admitted marker, and the log reports the
        // value that is actually being written.
        if !insert.marker_valid() {
            worker::console_log!(
                "[hopparty:markerinvalid] txid={} vout={} hopVout={} identity={}",
                record.txid,
                record.output_index,
                record.hop_vout,
                record.identity
            );
        }
        self.db.insert(insert).await.map_err(hopparty_err)
    }

    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<HoppartyRecord>, HoppartyStorageError> {
        let rows: Vec<HoppartyRow> = self
            .db
            .fetch_all(
                Query::new(hopparty_list_for_identity_sql())
                    .bind(identity)
                    .bind(limit as u32)
                    .bind(unknown_pot_quota(limit) as u32)
                    .bind(limit.saturating_mul(HOPSFOR_ROWS_PER_OUTPOINT) as u32),
            )
            .await
            .map_err(hopparty_err)?;
        Ok(rows.into_iter().map(HoppartyRow::into_record).collect())
    }

    async fn list_for_hop(
        &self,
        hop_txid: &str,
        hop_vout: u32,
        limit: usize,
    ) -> Result<Vec<HoppartyRecord>, HoppartyStorageError> {
        let rows: Vec<HoppartyRow> = self
            .db
            .fetch_all(
                Query::new(hopparty_list_for_hop_sql())
                    .bind(hop_txid)
                    .bind(hop_vout)
                    .bind(limit as u32),
            )
            .await
            .map_err(hopparty_err)?;
        Ok(rows.into_iter().map(HoppartyRow::into_record).collect())
    }
}

// ── #367 RE-LATCH: hopparty_records ─────────────────────────────────────────

/// The table name `relatch_cursors` keys this sweep by, and the log label.
pub const HOPPARTY_TABLE: &str = "hopparty_records";

/// The re-latch SCAN — every column
/// [`overlay_discovery::hopparty::validity::record_marker_valid`] needs
/// (including the container's decoded facts, `hopLockHex` / `hopSatsOnChain`,
/// which #310's decode-at-write already put on the row), plus the `rowid` the
/// cursor rides on and the STORED verdict.
///
/// **`WHERE markerValid IS NULL` is deliberately absent and must stay
/// absent** — see [`potparty_relatch_scan_sql`]. The verdict column appears in
/// the SELECT list and nowhere else.
fn hopparty_relatch_scan_sql() -> String {
    use overlay_discovery::hopparty::validity::MARKER_VALID_COLUMN;
    format!(
        "SELECT identity, opponentIdentity, gameId, hopVout, hopSats, \
            seatSettlePubkey, seatSigHex, identitySigHex, hopLockHex, \
            hopSatsOnChain, containerOutputs, txid, outputIndex, createdAt, \
            rowid, {MARKER_VALID_COLUMN} \
         FROM hopparty_records WHERE rowid > ? ORDER BY rowid ASC LIMIT ?"
    )
}

/// Both convergence readouts in one aggregate — see
/// [`potparty_relatch_census_sql`].
fn hopparty_relatch_census_sql() -> String {
    use overlay_discovery::hopparty::validity::MARKER_VALID_COLUMN;
    format!(
        "SELECT COALESCE(SUM(CASE WHEN rowid > ?1 THEN 1 ELSE 0 END), 0) AS remaining, \
                COALESCE(SUM(CASE WHEN {MARKER_VALID_COLUMN} IS NULL THEN 1 ELSE 0 END), 0) \
                    AS stillNull \
         FROM hopparty_records"
    )
}

/// A scanned hopparty row: the record, its `rowid`, and its STORED verdict.
#[derive(Deserialize)]
struct HoppartyRelatchDbRow {
    identity: String,
    #[serde(rename = "opponentIdentity")]
    opponent_identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "hopVout")]
    hop_vout: f64,
    #[serde(rename = "hopSats")]
    hop_sats: f64,
    #[serde(rename = "seatSettlePubkey")]
    seat_settle_pubkey: String,
    #[serde(rename = "seatSigHex")]
    seat_sig_hex: String,
    #[serde(rename = "identitySigHex")]
    identity_sig_hex: String,
    #[serde(rename = "hopLockHex", default)]
    hop_lock_hex: Option<String>,
    #[serde(rename = "hopSatsOnChain", default)]
    hop_sats_on_chain: Option<f64>,
    #[serde(rename = "containerOutputs")]
    container_outputs: f64,
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    #[serde(rename = "createdAt")]
    created_at: Option<f64>,
    rowid: f64,
    #[serde(rename = "markerValid", default)]
    marker_valid: Option<f64>,
}

/// The pass's row shape for `hopparty_records`.
pub struct HoppartyRelatchRow {
    rowid: i64,
    stored: Option<bool>,
    record: HoppartyRecord,
}

impl HoppartyRelatchDbRow {
    fn into_row(self) -> HoppartyRelatchRow {
        HoppartyRelatchRow {
            rowid: self.rowid as i64,
            stored: self.marker_valid.map(|v| v != 0.0),
            record: HoppartyRecord {
                identity: self.identity,
                opponent_identity: self.opponent_identity,
                game_id: self.game_id,
                hop_vout: self.hop_vout as u32,
                hop_sats: self.hop_sats as u64,
                seat_settle_pubkey: self.seat_settle_pubkey,
                seat_sig_hex: self.seat_sig_hex,
                identity_sig_hex: self.identity_sig_hex,
                hop_lock_hex: self.hop_lock_hex,
                hop_sats_on_chain: self.hop_sats_on_chain.map(|v| v as u64),
                container_outputs: self.container_outputs as u32,
                txid: self.txid,
                output_index: self.output_index as u32,
                created_at: self.created_at.unwrap_or(0.0) as i64,
            },
        }
    }
}

#[async_trait(?Send)]
impl crate::relatch::RelatchTable for D1HoppartyStorage {
    type Row = HoppartyRelatchRow;

    fn table(&self) -> &'static str {
        HOPPARTY_TABLE
    }
    fn rowid(row: &Self::Row) -> i64 {
        row.rowid
    }
    fn stored(row: &Self::Row) -> Option<bool> {
        row.stored
    }

    async fn scan(&self, after_rowid: i64, limit: u64) -> Result<Vec<Self::Row>, String> {
        let rows: Vec<HoppartyRelatchDbRow> = self
            .db
            .fetch_all(
                Query::new(hopparty_relatch_scan_sql())
                    .bind(after_rowid)
                    .bind(limit as u32),
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(HoppartyRelatchDbRow::into_row)
            .collect())
    }

    async fn relatch_if_changed(&self, row: &Self::Row) -> Result<Option<bool>, String> {
        // ONE evaluation — see the potparty twin.
        let update = hopparty_relatch_query(&row.record);
        let verdict = update.marker_valid();
        if row.stored == Some(verdict) {
            return Ok(None);
        }
        self.db.relatch(update).await?;
        Ok(Some(verdict))
    }

    async fn census(&self, after_rowid: i64) -> Result<crate::relatch::RelatchCensus, String> {
        let rows: Vec<RelatchCensusRow> = self
            .db
            .fetch_all(Query::new(hopparty_relatch_census_sql()).bind(after_rowid))
            .await?;
        let r = rows.into_iter().next().ok_or("census returned no row")?;
        Ok(crate::relatch::RelatchCensus {
            remaining: r.remaining as u64,
            still_null: r.still_null as u64,
        })
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
///
/// OLDEST-first (bsv-low #282): `tm_proof` admission is byte-format-only
/// and the window is tiny (DEFAULT_LIMIT 3), so under newest-first THREE
/// junk bundles filed after the honest one hid the real proof — the
/// cheapest attack in the dust-displacement family. The honest bundle is
/// published at settle; a post-hoc flood can never get in front of it
/// oldest-first. Residual (stated plainly): an attacker who PRE-files
/// during the hand — (gameId, winner) are guessable from the two seats —
/// still buries it; the closure is the client verifying each bundle
/// (which it does: a junk bundle never validates) plus paging, not order.
pub fn proof_list_for_game_winner_sql() -> &'static str {
    "SELECT gameId, winner, sigHex, bundleB64, \
            CASE WHEN bundleB64 IS NULL THEN hex(bundle) ELSE '' END AS bundle, \
            txid, outputIndex, createdAt \
     FROM proof_markers WHERE gameId = ? AND winner = ? \
     ORDER BY createdAt ASC, rowid ASC LIMIT ?"
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

    /// bsv-low#304: the SHIPPED pot_beefs SQL on the production schema —
    /// the completion-pass candidate set is gated on the VERIFIED latch
    /// (`proof_verified`), NOT the structural `has_proof` flag; the admit
    /// write forces `proof_verified = 0`; only the verifying writes latch
    /// it. Executes the exact production strings under real SQLite.
    #[test]
    fn pot_beef_verified_latch_real_sqlite() {
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
        let candidates = |min_age: u64| -> Vec<String> {
            let sql = pot_beef_candidates_sql(16, min_age);
            let mut stmt = conn
                .prepare(&sql)
                .expect("shipped candidate SQL must parse");
            let mut got: Vec<String> = stmt
                .query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            got.sort();
            got
        };

        // ADMIT writes (the store_beef path): a fake-bumped submission is
        // modeled by has_proof = 1 in the bind — proof_verified is FORCED
        // to 0 by the SQL itself, so the row STAYS a candidate.
        conn.execute(
            POT_BEEF_ADMIT_WRITE_SQL,
            rusqlite::params!["fakebumped", vec![0xbeu8, 0xef], "fakebumped", 100i64, 1i64],
        )
        .unwrap();
        conn.execute(
            POT_BEEF_ADMIT_WRITE_SQL,
            rusqlite::params!["proofless", vec![0xbeu8, 0xef], "proofless", 100i64, 0i64],
        )
        .unwrap();
        assert_eq!(
            candidates(0),
            vec!["fakebumped".to_string(), "proofless".to_string()],
            "a structurally-bumped ADMIT row must stay a completion candidate (the #304 hole: \
             WHERE has_proof = 0 excluded it forever)"
        );

        // The verifying compact write latches BOTH flags → drops out.
        conn.execute(
            POT_BEEF_VERIFIED_WRITE_SQL,
            rusqlite::params!["fakebumped", vec![0xbeu8, 0xef, 0x01], 200i64],
        )
        .unwrap();
        assert_eq!(candidates(0), vec!["proofless".to_string()]);

        // The lightweight mark-proven flip latches without a byte rewrite.
        conn.execute(POT_BEEF_MARK_PROVEN_SQL, rusqlite::params!["proofless"])
            .unwrap();
        assert!(candidates(0).is_empty());

        // The probe SQL surfaces the latch the store_beef guard consumes —
        // a verified row answers proof_verified = 1 (the Rust side then
        // refuses the admit overwrite: never weaken a verified answer).
        let (len, verified): (i64, i64) = conn
            .query_row(POT_BEEF_PROBE_SQL, rusqlite::params!["fakebumped"], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(len, 3, "the verifying write replaced the bytes");
        assert_eq!(verified, 1);

        // gate M-4: the BATCHED latch flip — one statement latches many rows
        // (identical per-row semantics to POT_BEEF_MARK_PROVEN_SQL).
        for t in ["b1", "b2", "b3"] {
            conn.execute(
                POT_BEEF_ADMIT_WRITE_SQL,
                rusqlite::params![t, vec![0xbeu8, 0xef], t, 100i64, 1i64],
            )
            .unwrap();
        }
        assert_eq!(
            candidates(0),
            vec!["b1".to_string(), "b2".into(), "b3".into()]
        );
        conn.execute(
            &pot_beef_mark_proven_batch_sql(3),
            rusqlite::params!["b1", "b2", "b3"],
        )
        .unwrap();
        assert!(
            candidates(0).is_empty(),
            "one batched statement latched all three"
        );
    }

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
        assert_eq!(
            r.bundle_b64, None,
            "legacy row: service falls back to encoding"
        );
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
                txid,
                vout,
                0i64,
                Option::<String>::None,
                0i64,
                created_at,
                lock_kind,
                pub_a,
                pub_a,
                pub_a, // pubB / pubTower ride the same fixture value
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
            (false, None) => conn.execute(sql, rusqlite::params![spending_txid, txid, vout]),
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
        exec_mark_spent(
            &conn,
            "potA",
            0,
            "settleTx",
            true,
            Some("winner-a"),
            Some(800_000),
        );
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
        exec_mark_spent(
            &conn,
            "potA",
            0,
            "realSettle",
            true,
            Some("winner-a"),
            Some(800_000),
        );

        // The attacker's unconfirmed claim — with its own forged verdict.
        exec_mark_spent(
            &conn,
            "potA",
            0,
            "forgedSpend",
            false,
            Some("winner-b"),
            None,
        );
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
        exec_store(
            &conn,
            "potA",
            0,
            2_000,
            Some("covenant"),
            Some(""),
            Some(999),
            Some(4),
            1,
        );
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

    /// Execute the shipped `confirm_spend_cas_sql()` (the #301 chaser
    /// confirm) with the D1 impl's exact bind order; returns whether the
    /// guard HIT (a RETURNING row came back).
    fn exec_confirm_cas(
        conn: &rusqlite::Connection,
        txid: &str,
        vout: u32,
        spending_txid: &str,
        spent_height: Option<i64>,
    ) -> bool {
        match conn.query_row(
            confirm_spend_cas_sql(),
            rusqlite::params![spent_height, txid, vout, spending_txid],
            |r| r.get::<_, String>(0),
        ) {
            Ok(_) => true,
            Err(rusqlite::Error::QueryReturnedNoRows) => false,
            Err(e) => panic!("confirm_spend_cas_sql executes: {e}"),
        }
    }

    /// bsv-low#301 (the MEDIUM-2 sibling, executed against the production
    /// schema): the #186 chaser's confirmed write is a guarded CAS — bound
    /// to the stale S1 after a reorg-confirmed S2 displaced it, the write
    /// is a NO-OP (miss) and S2's pointer/flag/height/spentAt all survive;
    /// bound to the CURRENT pointer it lands (hit), latching the flag and
    /// height while leaving the pointer, the verdict pair, and the #228
    /// spentAt age anchor untouched.
    #[test]
    fn sql_confirm_cas_is_a_noop_when_the_pointer_moved() {
        let read_spent_at = |conn: &rusqlite::Connection, txid: &str| -> Option<i64> {
            conn.query_row(
                "SELECT spentAt FROM pot_records WHERE txid = ?1 AND outputIndex = 0",
                rusqlite::params![txid],
                |r| r.get(0),
            )
            .expect("row present")
        };

        let conn = production_schema_db();
        // The RACE case: the chaser reads S1 (unconfirmed)…
        exec_store(&conn, "potA", 0, 1_000, None, None, None, None, 0);
        exec_mark_spent(&conn, "potA", 0, "settleS1", false, None, None);
        // …then a reorg-CONFIRMED S2 displaces S1 before the write lands.
        exec_mark_spent(&conn, "potA", 0, "settleS2", true, None, Some(802_000));
        let before = read_pot_row(&conn, "potA", 0);
        let before_at = read_spent_at(&conn, "potA");

        assert!(
            !exec_confirm_cas(&conn, "potA", 0, "settleS1", Some(800_000)),
            "a moved pointer is a CAS MISS"
        );
        let after = read_pot_row(&conn, "potA", 0);
        assert_eq!(after, before, "a stale CAS confirm changes NOTHING");
        assert_eq!(after.spending_txid.as_deref(), Some("settleS2"));
        assert_eq!(after.spent_confirmed, 1);
        assert_eq!(
            after.spent_height,
            Some(802_000),
            "S2 never regains S1's height"
        );
        assert_eq!(read_spent_at(&conn, "potA"), before_at, "spentAt untouched");

        // The HIT case: pointer still the proof's spender. Sentinel the age
        // anchor first so "not restamped" is provable (the CAS idiom: only
        // spent/spentConfirmed/spentHeight move).
        exec_store(&conn, "potB", 0, 1_000, None, None, None, None, 0);
        exec_mark_spent(&conn, "potB", 0, "settleS1", false, Some("tie"), None);
        conn.execute(
            "UPDATE pot_records SET spentAt = 12345 WHERE txid = 'potB'",
            [],
        )
        .unwrap();
        assert!(exec_confirm_cas(
            &conn,
            "potB",
            0,
            "settleS1",
            Some(800_000)
        ));
        let r = read_pot_row(&conn, "potB", 0);
        assert_eq!(r.spent_confirmed, 1);
        assert_eq!(r.spent, 1);
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("settleS1"),
            "pointer untouched"
        );
        assert_eq!(r.spent_height, Some(800_000));
        assert_eq!(r.verdict.as_deref(), Some("tie"), "verdict pair untouched");
        assert_eq!(r.verdict_txid.as_deref(), Some("settleS1"));
        assert_eq!(
            read_spent_at(&conn, "potB"),
            Some(12345),
            "spentAt not restamped"
        );

        // Same-pointer re-confirm with a NULL height keeps the stored one
        // (COALESCE — the mark_spent same-pointer semantics).
        assert!(exec_confirm_cas(&conn, "potB", 0, "settleS1", None));
        assert_eq!(read_pot_row(&conn, "potB", 0).spent_height, Some(800_000));

        // An absent outpoint: miss, never an error / phantom row.
        assert!(!exec_confirm_cas(&conn, "ghost", 0, "settleS1", None));
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
        let row = BeefLenRow {
            len: 1234.0,
            proof_verified: None,
        };
        assert_eq!(row.len as usize, 1234);
        // NULL/absent proof_verified (a read racing the additive #304
        // migration) is UNVERIFIED — the admit write stays allowed.
        assert!(row.proof_verified.unwrap_or(0.0) == 0.0);
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
        let mut stmt = conn
            .prepare(&sql)
            .expect("batch SQL must parse on real SQLite");
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

    // ── #282: result_markers_v2 windows + proof oldest-first ─────────────

    fn insert_result(
        conn: &rusqlite::Connection,
        winner: &str,
        pot_txid: &str,
        marker_txid: &str,
        created_at: i64,
    ) {
        conn.execute(
            "INSERT OR IGNORE INTO result_markers_v2 \
             (gameId, winner, loser, potTxid, settleTxid, winnerSigHex, \
              loserSigHex, cardsHex, txid, outputIndex, createdAt) \
             VALUES (?1, ?2, ?3, ?4, ?5, '3045ab', NULL, NULL, ?6, 0, ?7)",
            rusqlite::params![
                h64(0x11),
                winner,
                "03bb",
                pot_txid,
                h64(0x33),
                marker_txid,
                created_at
            ],
        )
        .expect("insert result_markers_v2");
    }

    /// Project one column from the shipped result window.
    fn result_window_col(
        conn: &rusqlite::Connection,
        winner: Option<&str>,
        limit: usize,
        col: &str,
    ) -> Vec<String> {
        let sql = result_window_sql(winner.is_some());
        let mut stmt = conn.prepare(&sql).expect("prepare shipped result window");
        let map = |r: &rusqlite::Row<'_>| r.get::<_, String>(col);
        let rows = match winner {
            Some(w) => stmt
                .query_map(
                    rusqlite::params![
                        w,
                        limit as u32,
                        unknown_pot_quota(limit) as u32,
                        result_window_row_cap(limit) as u32
                    ],
                    map,
                )
                .expect("query")
                .collect::<Result<Vec<_>, _>>(),
            None => stmt
                .query_map(
                    rusqlite::params![
                        limit as u32,
                        unknown_pot_quota(limit) as u32,
                        result_window_row_cap(limit) as u32
                    ],
                    map,
                )
                .expect("query")
                .collect::<Result<Vec<_>, _>>(),
        };
        rows.expect("rows")
    }

    /// RED-class scenario (the #281 family, executed for tm_result): junk
    /// markers flooding BOTH invented pots and the victim's real pot bury
    /// the honest result under a flat newest-first window; the shipped
    /// per-pot window keeps the honest row reachable.
    #[test]
    fn result_window_survives_dust_flood_real_sqlite() {
        let conn = production_schema_db();
        let winner = "02".to_string() + &"a1".repeat(32);
        let honest_pot = h64(0xaa);
        insert_pot(&conn, &honest_pot, 0, 1_000, true);
        // The honest result at settle…
        insert_result(&conn, &winner, &honest_pot, "txHONEST", 1_001);
        // …then a post-hoc flood: replays on the real pot + ghost pots.
        for i in 0..60u32 {
            insert_result(
                &conn,
                &winner,
                &honest_pot,
                &format!("txJUNK{i:03}"),
                2_000 + i as i64,
            );
        }
        for i in 0..120u32 {
            insert_result(
                &conn,
                &winner,
                &format!("{:064x}", 0xdead_0000u64 + i as u64),
                &format!("txGHOST{i:03}"),
                3_000 + i as i64,
            );
        }
        for (scope, got) in [
            (
                "resultsFor",
                result_window_col(&conn, Some(&winner), 100, "txid"),
            ),
            ("recentResults", result_window_col(&conn, None, 100, "txid")),
        ] {
            assert!(
                got.contains(&"txHONEST".to_string()),
                "{scope}: the honest result survives the flood (oldest-in-pot \
                 superset + existence tier): {}",
                got.len()
            );
        }
        // And the real pot contributes at most the superset, never 61 rows.
        let per_pot = result_window_col(&conn, Some(&winner), 100, "potTxid")
            .iter()
            .filter(|p| **p == honest_pot)
            .count();
        assert!(
            per_pot <= overlay_discovery::result::storage::RESULT_ROWS_PER_POT,
            "per-pot superset bounded, got {per_pot}"
        );
    }

    /// The windows stay windows: `limit` counts POTS newest-first (pot
    /// admission stamp — unmovable by marker spam), and every real pot's
    /// result stays reachable at the page size.
    #[test]
    fn result_window_limit_counts_pots_newest_first_real_sqlite() {
        let conn = production_schema_db();
        let winner = "02".to_string() + &"a1".repeat(32);
        for i in 0..5u32 {
            let pot = format!("{:064x}", 0x0000_2000u64 + i as u64);
            insert_pot(&conn, &pot, 0, 1_000 + i as i64, true);
            insert_result(&conn, &winner, &pot, &format!("txR{i}"), 1_000 + i as i64);
        }
        let got = result_window_col(&conn, None, 3, "txid");
        assert_eq!(
            got,
            vec!["txR4", "txR3", "txR2"],
            "3 newest pots, newest first"
        );
        let got = result_window_col(&conn, Some(&winner), 100, "txid");
        assert_eq!(got.len(), 5, "all real results at the page size");
    }

    /// #282: `ls_proof` answers OLDEST-first — the honest bundle lands at
    /// settle, so a post-hoc junk flood (3 rows beat the old DEFAULT_LIMIT 3
    /// newest-first window) can never get in front of it.
    #[test]
    fn proof_window_is_oldest_first_real_sqlite() {
        let conn = production_schema_db();
        let game = h64(0x11);
        let winner = "02".to_string() + &"a1".repeat(32);
        let insert = |txid: &str, at: i64| {
            conn.execute(
                "INSERT OR IGNORE INTO proof_markers \
                 (gameId, winner, sigHex, bundle, bundleB64, txid, outputIndex, createdAt) \
                 VALUES (?1, ?2, '3045ab', X'7B7D', 'e30=', ?3, 0, ?4)",
                rusqlite::params![game, winner, txid, at],
            )
            .expect("insert proof_markers");
        };
        insert("txREAL", 1_000);
        for i in 0..10u32 {
            insert(&format!("txJUNK{i:02}"), 2_000 + i as i64);
        }
        let mut stmt = conn.prepare(proof_list_for_game_winner_sql()).unwrap();
        let got: Vec<String> = stmt
            .query_map(rusqlite::params![game, winner, 3u32], |r| {
                r.get::<_, String>("txid")
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            got.first().map(String::as_str),
            Some("txREAL"),
            "the settle-time bundle heads the window; junk filed later never fronts it"
        );
    }

    /// Gate finding L3: an unknown recordType discriminator (version skew —
    /// written by a newer deploy, read after a rollback) is an EXPLICIT
    /// logged skip, and it can never take neighboring good rows with it.
    #[test]
    fn unknown_low_record_type_skips_loudly_never_the_good_rows() {
        let row = |record_type: &str, txid: &str| LowRow {
            record_type: record_type.into(),
            txid: txid.into(),
            output_index: 0.0,
            host_identity: "02aa".into(),
            game_id: "11".repeat(32),
            stake_sats: Some(1000.0),
            rules_hash: None,
            relay_url: None,
            expiry_height: None,
        };
        let records = low_records_from_rows(vec![
            row("table", "txGOOD1"),
            row("fromTheFuture", "txSKEW"), // v-next discriminator
            row("gameutxo", "txGOOD2"),
        ]);
        assert_eq!(
            records.iter().map(|r| r.txid.as_str()).collect::<Vec<_>>(),
            vec!["txGOOD1", "txGOOD2"],
            "the skewed row is skipped (and console-warned with its \
             outpoint); every convertible row survives in order"
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
        // OLDEST first (#282): the legacy row (createdAt 1), then the b64 row.
        let (old_txid, old_b64, old_hex) = &rows[0];
        let (new_txid, new_b64, new_hex) = &rows[1];
        assert_eq!(new_txid, &h64(0x01));
        assert_eq!(old_txid, &h64(0x02));
        assert_eq!(new_hex, "", "b64 answered — the blob is NOT hauled");
        assert!(old_b64.is_none(), "legacy row has no b64");

        // The wire value each path produces (mirrors the lookup service:
        // stored b64 preferred, else encode the decoded hex) must be
        // byte-identical.
        let wire_new = new_b64.clone().unwrap();
        let wire_old = BASE64.encode(hex::decode(old_hex).unwrap());
        assert_eq!(
            wire_new, wire_old,
            "both read paths answer the same bundleBase64"
        );
        assert_eq!(
            wire_new,
            BASE64.encode(bundle),
            "…and it is the admitted bytes"
        );
    }

    // ── #290/#291: the low / reveal / collected shipped SQL ──────────────

    /// Insert a `low_records` row with an explicit TEXT `createdAt`
    /// (this table's `createdAt` is `datetime('now')` TEXT — the odd one
    /// out; every other LOW marker table stamps INTEGER unix seconds).
    fn insert_low_for_host(conn: &rusqlite::Connection, txid: &str, host: &str, created_at: &str) {
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
            .query_map(
                rusqlite::params!["table", 100u64, 5000u64, 800000u32],
                |row| Ok((row.get::<_, String>(1)?, row.get::<_, u64>(5)?)),
            )
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows.iter().map(|(t, _)| t.clone()).collect::<Vec<_>>(),
            vec![h64(0x03), h64(0x02), h64(0x01)],
            "newest-first by createdAt, not physical order"
        );
        assert_eq!(
            rows[0].1, 1000,
            "full index row: stakeSats decoded column present"
        );

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

    /// bsv-low#309: the shipped advert-lifecycle candidate scans execute on
    /// the production schema. Spend-check: TABLE rows only, LIMIT-bounded.
    /// Reap: cutoff-INCLUSIVE (`<=`), NULL-expiry rows never surface,
    /// oldest-expiry-first, LIMIT-bounded.
    #[test]
    fn low_advert_lifecycle_scan_sql_real_sqlite() {
        let conn = production_schema_db();
        // Three table rows with distinct expiries + one NULL-expiry table
        // row + one gameutxo pointer row.
        for (txid_seed, expiry) in [(0x01u8, 899_990i64), (0x02, 900_000), (0x03, 900_001)] {
            conn.execute(
                "INSERT INTO low_records (recordType, txid, outputIndex, hostIdentity, \
                 gameId, stakeSats, expiryHeight) VALUES ('table', ?1, 0, ?2, ?3, 1000, ?4)",
                rusqlite::params![h64(txid_seed), victim_id(), h64(0x11), expiry],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO low_records (recordType, txid, outputIndex, hostIdentity, gameId) \
             VALUES ('table', ?1, 0, ?2, ?3)",
            rusqlite::params![h64(0x04), victim_id(), h64(0x11)],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO low_records (recordType, txid, outputIndex, hostIdentity, gameId) \
             VALUES ('gameutxo', ?1, 1, ?2, ?3)",
            rusqlite::params![h64(0x05), victim_id(), h64(0x11)],
        )
        .unwrap();

        // Spend-check scan: every TABLE row (incl. the NULL-expiry one — a
        // confirmed spend is the ONLY thing that may remove it), never the
        // gameutxo pointer; LIMIT bounds the batch.
        let mut stmt = conn
            .prepare(&low_tables_for_spend_check_sql(10))
            .expect("shipped spend-check SQL must parse");
        let mut txids: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        txids.sort();
        assert_eq!(
            txids,
            vec![h64(0x01), h64(0x02), h64(0x03), h64(0x04)],
            "all table rows, never the pointer"
        );
        let mut stmt = conn.prepare(&low_tables_for_spend_check_sql(2)).unwrap();
        assert_eq!(
            stmt.query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .count(),
            2,
            "LIMIT bounds the batch"
        );

        // Reap scan at cutoff 900_000: the <= boundary row and the older row,
        // OLDEST first; the above-cutoff and NULL-expiry rows never surface.
        let mut stmt = conn
            .prepare(&low_tables_expired_sql(10))
            .expect("shipped reap SQL must parse");
        let txids: Vec<String> = stmt
            .query_map(rusqlite::params![900_000i64], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            txids,
            vec![h64(0x01), h64(0x02)],
            "cutoff-inclusive, oldest-expiry-first; NULL expiry is NEVER a candidate"
        );
        let mut stmt = conn.prepare(&low_tables_expired_sql(1)).unwrap();
        let txids: Vec<String> = stmt
            .query_map(rusqlite::params![900_000i64], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(txids, vec![h64(0x01)], "the bound keeps the oldest");
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
            .query_map(rusqlite::params![h64(0x11), 1u8], |row| {
                row.get::<_, String>(0)
            })
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
        assert_eq!(
            txids,
            vec![h64(0x01), h64(0x02)],
            "both seats of the game, no leak"
        );
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
            "INSERT INTO collected_markers_v2 \
             (identity, gameId, txid, outputIndex, sigHex, createdAt) \
             VALUES (?1, ?2, ?3, 0, 'sig-a', 1)",
            rusqlite::params![me, h64(0x11), h64(0x01)],
        )
        .unwrap();
        // Same gameId, DIFFERENT identity — must not leak into my answer.
        conn.execute(
            "INSERT INTO collected_markers_v2 \
             (identity, gameId, txid, outputIndex, sigHex, createdAt) \
             VALUES (?1, ?2, ?3, 0, 'sig-b', 2)",
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

    /// #327 S8, through the SHIPPED SQL against the REAL production schema:
    /// a pre-emptive squat can no longer censor the victim's genuine marker.
    ///
    /// This is the SQL-level counterpart of the storage-layer cell. It matters
    /// separately because the censorship lived in the PRIMARY KEY, not in Rust:
    /// under the superseded `(identity, gameId)` key the second INSERT was
    /// silently dropped by the engine, so no amount of Rust-side testing could
    /// have surfaced it (Rule 16 — the property spans the boundary, so it must
    /// be pinned against the real database).
    #[test]
    fn a_squat_cannot_censor_the_genuine_marker_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        let game = h64(0x11);

        // 1. The SQUATTER files first, at deal time, naming the VICTIM — free,
        //    byte-format-only admission, junk signature.
        let squat = conn
            .execute(
                "INSERT OR IGNORE INTO collected_markers_v2 \
                 (identity, gameId, txid, outputIndex, sigHex, createdAt) \
                 VALUES (?1, ?2, ?3, 0, 'sigJUNK', 1)",
                rusqlite::params![victim, game, h64(0xaa)],
            )
            .unwrap();
        assert_eq!(squat, 1);

        // 2. The victim's GENUINE marker lands later, from a DIFFERENT tx.
        //    Under the old (identity, gameId) key this INSERT OR IGNORE was a
        //    silent no-op — that was the defect.
        let genuine = conn
            .execute(
                "INSERT OR IGNORE INTO collected_markers_v2 \
                 (identity, gameId, txid, outputIndex, sigHex, createdAt) \
                 VALUES (?1, ?2, ?3, 0, 'sigREAL', 2)",
                rusqlite::params![victim, game, h64(0xbb)],
            )
            .unwrap();
        assert_eq!(genuine, 1, "the genuine marker must be STORED, not ignored");

        // 3. Both rows answer the shipped batched read, so the client can
        //    verify the sigs and select its own.
        let sql = collected_records_batch_sql(1);
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![victim, game], |row| {
                Ok((row.get::<_, String>(2)?, row.get::<_, String>(4)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(rows.len(), 2, "both rows coexist — exclusivity WAS the bug");
        assert!(
            rows.iter()
                .any(|(txid, sig)| *txid == h64(0xbb) && sig == "sigREAL"),
            "the victim's genuine marker survives the squat: {rows:?}"
        );

        // 4. …while a REPLAY of the squatter's own outpoint is still a no-op,
        //    so the set cannot be inflated for free by resubmitting one output.
        let replay = conn
            .execute(
                "INSERT OR IGNORE INTO collected_markers_v2 \
                 (identity, gameId, txid, outputIndex, sigHex, createdAt) \
                 VALUES (?1, ?2, ?3, 0, 'sigOTHER', 3)",
                rusqlite::params![victim, game, h64(0xaa)],
            )
            .unwrap();
        assert_eq!(replay, 0, "same outpoint never re-inserts");
    }

    /// H6: the re-key removed exclusivity, so the read must carry a BOUND —
    /// and the bound must not re-create the censorship the re-key removed.
    ///
    /// Pins both halves against the real schema: the window caps rows per
    /// pair, AND a pre-emptively filed squat can never displace the victim's
    /// later genuine marker (which is what an oldest-first order would do).
    #[test]
    fn collected_window_is_bounded_and_a_prefiled_squat_cannot_evict_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        let game = h64(0x11);

        // The squatter pre-files a FULL window's worth at deal time (t=1..12),
        // more than the cap, all naming the victim.
        for i in 0..12u32 {
            conn.execute(
                "INSERT OR IGNORE INTO collected_markers_v2 \
                 (identity, gameId, txid, outputIndex, sigHex, createdAt) \
                 VALUES (?1, ?2, ?3, ?4, 'sigJUNK', ?5)",
                rusqlite::params![victim, game, h64(0xa0 + i as u8), i, i as i64 + 1],
            )
            .unwrap();
        }
        // The victim's GENUINE marker lands afterwards.
        conn.execute(
            "INSERT OR IGNORE INTO collected_markers_v2 \
             (identity, gameId, txid, outputIndex, sigHex, createdAt) \
             VALUES (?1, ?2, ?3, 0, 'sigREAL', 99)",
            rusqlite::params![victim, game, h64(0xbb)],
        )
        .unwrap();

        let sql = collected_records_batch_sql(1);
        let mut stmt = conn.prepare(&sql).expect("shipped batch SQL must parse");
        let rows: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![victim, game], |row| {
                Ok((row.get::<_, String>(2)?, row.get::<_, String>(4)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();

        assert_eq!(
            rows.len(),
            COLLECTED_ROWS_PER_PAIR,
            "the per-pair window must bound an unbounded mint"
        );
        assert!(
            rows.iter()
                .any(|(txid, sig)| *txid == h64(0xbb) && sig == "sigREAL"),
            "a PRE-FILED squat must not evict the victim's later genuine marker \
             — an oldest-first window would hand the squatter every slot: {rows:?}"
        );
    }

    /// The one-time carry migration must move a v1 row into v2 and be a no-op
    /// on re-run — the runner re-executes every statement on every cold start.
    /// An honest row admitted before the re-key is NEVER orphaned (Rule 14:
    /// read-both/write-new, nothing in flight is lost).
    #[test]
    fn collected_v1_rows_are_carried_into_v2_and_the_carry_is_rerun_safe() {
        let conn = production_schema_db();
        let me = victim_id();
        // A row that predates the re-key, in the write-frozen v1 table.
        conn.execute(
            "INSERT INTO collected_markers (identity, gameId, txid, sigHex, createdAt) \
             VALUES (?1, ?2, ?3, 'sigOLD', 7)",
            rusqlite::params![me, h64(0x11), h64(0xc1)],
        )
        .unwrap();

        let carry = crate::d1::OVERLAY_MIGRATIONS
            .iter()
            .find(|sql| {
                sql.trim_start()
                    .starts_with("INSERT OR IGNORE INTO collected_markers_v2")
            })
            .expect("the S8 carry migration must exist");

        conn.execute_batch(carry).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM collected_markers_v2 WHERE txid = ?1",
                rusqlite::params![h64(0xc1)],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 1,
            "the honest pre-re-key row is carried, not orphaned"
        );

        // Re-run (cold start): still exactly one row, no error.
        conn.execute_batch(carry).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM collected_markers_v2 WHERE txid = ?1",
                rusqlite::params![h64(0xc1)],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 1,
            "carry is idempotent under the re-run-everything runner"
        );
    }

    fn victim_id() -> String {
        format!("02{}", "a1".repeat(32))
    }

    /// Insert a pot row whose spend, when present, is CONFIRMED.
    ///
    /// `spentConfirmed` is derived from `spent` DELIBERATELY here and is
    /// documented as such (#323 LOW-1). The hazard is not the derivation —
    /// it is an UNDOCUMENTED one, which leaves callers believing they
    /// exercised a PARKED row (`spent = 1, spentConfirmed = 0`, the shape
    /// production reaches whenever a non-final refund is admitted before it
    /// mines) when the fixture could never produce one. Pass an explicit
    /// flag via [`insert_pot_with`] if a parked row is ever needed here.
    fn insert_pot(
        conn: &rusqlite::Connection,
        txid: &str,
        vout: u32,
        created_at: i64,
        spent: bool,
    ) {
        insert_pot_with(conn, txid, vout, created_at, spent, spent);
    }

    #[allow(dead_code)]
    fn insert_pot_with(
        conn: &rusqlite::Connection,
        txid: &str,
        vout: u32,
        created_at: i64,
        spent: bool,
        confirmed: bool,
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
                i32::from(confirmed),
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

    /// File a potparty marker WITH the #283 admission-time validity latch
    /// set. `sig_valid`: `Some(true)` = every signature the marker carries
    /// verified at admission; `Some(false)` = at least one did not;
    /// `None` = a pre-migration row (never evaluated), which is what plain
    /// [`insert_potparty`] writes.
    #[allow(clippy::too_many_arguments)]
    fn insert_potparty_latched(
        conn: &rusqlite::Connection,
        identity: &str,
        pot_txid: &str,
        pot_vout: u32,
        marker_txid: &str,
        created_at: i64,
        settle_pubkey: Option<&str>,
        sig_valid: Option<bool>,
    ) {
        conn.execute(
            "INSERT OR IGNORE INTO potparty_records \
             (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
              sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt, \
              sigValid) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, '3045id', ?7, ?8, ?9, 0, ?10, ?11)",
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
                created_at,
                sig_valid.map(i32::from)
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
        // #283a: promotion is AGE-BOUNDED (unixepoch()-anchored in the
        // shipped SQL), so the fresh pot's marker must carry a genuinely
        // fresh server-style stamp — exactly what the admit path writes.
        let now = now_secs();
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
        insert_potparty(&conn, &victim, &fresh, 0, "txFRESH", now - 30, None);

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

    /// Wall clock as the shipped SQL sees it (`unixepoch()`).
    fn now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64
    }

    // ── #283a — quota slots are AGE-BOUNDED + OLDEST-FIRST, not recency ──

    /// The #283 gate's executed failing case, now GREEN: a victim with 100
    /// indexed pots and ONE freshly funded pot (tm_pot admission in flight);
    /// an attacker files 10 ghost markers NEWER than the honest one. Under
    /// recency allocation the ghosts took every promoted slot and the fresh
    /// pot went ABSENT — exactly the case the quota was introduced to
    /// prevent. Oldest-first allocation ranks the honest (earlier) marker
    /// ahead of every later ghost.
    #[test]
    fn fresh_pot_survives_ghost_markers_filed_after_it_real_sqlite() {
        let now = now_secs();
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
        let fresh = h64(0xfa);
        insert_potparty(&conn, &victim, &fresh, 0, "txFRESH", now - 120, None);
        // Ghosts NEWER than the honest marker — the attacker reacted to the
        // funding (they cannot backdate a server stamp).
        for i in 0..(unknown_pot_quota(100) as u32) {
            insert_potparty(
                &conn,
                &victim,
                &format!("{:064x}", 0xdead_beefu64 + i as u64),
                0,
                &format!("txGHOSTN{i:02}"),
                now - 60 + i as i64,
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
        assert!(
            got.contains(&fresh),
            "the fresh honest pot must survive newer ghost markers (#283a): {got:?}"
        );
    }

    /// A STALE unknown pot (older than the promotion window) never occupies
    /// a promoted slot — ghosts age out; the one-time 10-marker flood dies.
    /// The stale rows are DEMOTED, not dropped (still reachable past the
    /// indexed pots).
    #[test]
    fn stale_unknown_pots_never_occupy_promotion_slots_real_sqlite() {
        let now = now_secs();
        let stale = now - (UNKNOWN_POT_PROMOTION_MAX_AGE_SECS as i64) - 100;
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
        let fresh = h64(0xfa);
        insert_potparty(&conn, &victim, &fresh, 0, "txFRESH", now - 30, None);
        // Ghosts OLDER than the honest marker but STALE — pre-#283a these
        // could never even be beaten oldest-first; the age bound retires them.
        for i in 0..20u32 {
            insert_potparty(
                &conn,
                &victim,
                &format!("{:064x}", 0xdead_beefu64 + i as u64),
                0,
                &format!("txGHOSTS{i:02}"),
                stale + i as i64,
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
        assert!(
            got.contains(&fresh),
            "stale ghosts must not displace the fresh pot's promoted slot: {got:?}"
        );
    }

    /// RESIDUAL, pinned so it cannot drift silently (#283a doc): an attacker
    /// who keeps ≥quota ghost markers INSIDE the freshness window and OLDER
    /// than the victim's funding moment (a sustained rolling flood — ~quota
    /// markers per hour, forever) still occupies the promoted slots. The fix
    /// raises the cost from 10 markers once to a continuous flood; it does
    /// not close the window (the closures are verified-key binding — absent
    /// for discovery — or priced admission, an owner decision).
    #[test]
    fn sustained_fresh_older_ghost_flood_still_displaces_residual_real_sqlite() {
        let now = now_secs();
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
        let fresh = h64(0xfa);
        insert_potparty(&conn, &victim, &fresh, 0, "txFRESH", now - 30, None);
        // Fresh ghosts OLDER than the honest marker (the sustained flood).
        for i in 0..(unknown_pot_quota(100) as u32) {
            insert_potparty(
                &conn,
                &victim,
                &format!("{:064x}", 0xdead_beefu64 + i as u64),
                0,
                &format!("txGHOSTO{i:02}"),
                now - 600 + i as i64,
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
        assert!(
            !got.contains(&fresh),
            "KNOWN residual: a sustained older-but-fresh ghost flood still displaces — \
             if this starts PASSING, the allocation changed and the docs must move with it"
        );
    }

    // ── #283 — the ADMISSION WRITER, pinned behaviourally ────────────────

    /// The client's FROZEN golden v2 marker (real `@bsv/sdk` ProtoWallet
    /// output, pinned in `app/src/lib/potParty.test.ts`) — the only honest
    /// artifact available here that a REAL signature check can pass. Using a
    /// hand-built record instead would make the cell blind to the dimension
    /// it exists to measure (epoch Rule 18).
    const GOLDEN_V2_MARKER_HEX: &str = "006a0f4c4f572f706f7470617274792f7632210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982102c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee520cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc20dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd04010000000428a00e002103d3e37fc9edbd1c225d703873b45f66368e86c633cb613252b3254ffe0b8ad5ee4630440220106a632f58753f6b9ebaf20d105874d3aed43c28dab90e8b6a8a51dbd610e1e402204c7837248995842ec551eb3c8510b5862f87bf0c54368534fd3d7c1e3b9a50fd473045022100d3ea901d46fa588cb2f20e0bb0a3c7e23f6320138efee69f9e506a8e79abbaa102207cfccbd475e5d9e789091acdfa7d81503b950ebf51da6a1ac9fec44c84553773";

    fn golden_potparty_record(marker_txid: &str) -> PotpartyRecord {
        let m = overlay_discovery::potparty::parse_potparty_marker(
            &hex::decode(GOLDEN_V2_MARKER_HEX).unwrap(),
        )
        .expect("the frozen client golden parses");
        PotpartyRecord {
            identity: hex::encode(&m.identity),
            opponent_identity: hex::encode(&m.opponent),
            game_id: hex::encode(m.game_id),
            pot_txid: hex::encode(m.pot_txid),
            pot_vout: m.pot_vout,
            recovery_height: m.recovery_height,
            sig_hex: hex::encode(&m.sig),
            seat_settle_pubkey: m.seat_settle_pubkey.as_ref().map(hex::encode),
            seat_sig_hex: m.seat_sig.as_ref().map(hex::encode),
            txid: marker_txid.to_string(),
            output_index: 0,
            created_at: 0,
        }
    }

    /// Replay a [`Query`] built by production against real SQLite.
    fn exec_query(conn: &rusqlite::Connection, q: &crate::d1::Query) {
        let vals: Vec<rusqlite::types::Value> = q
            .params()
            .iter()
            .map(|p| match p {
                crate::d1::QVal::Null => rusqlite::types::Value::Null,
                crate::d1::QVal::Int(i) => rusqlite::types::Value::Integer(*i),
                crate::d1::QVal::Text(s) => rusqlite::types::Value::Text(s.clone()),
                crate::d1::QVal::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
                crate::d1::QVal::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
                crate::d1::QVal::Float(f) => rusqlite::types::Value::Real(*f),
            })
            .collect();
        conn.execute(q.sql(), rusqlite::params_from_iter(vals.iter()))
            .expect("the production insert query must execute against the production schema");
    }

    /// THE WRITER PIN (bsv-low #283, from the adversarial gate's HIGH-2).
    ///
    /// Every other #283 cell in this repo RECONSTRUCTS the latch in its
    /// fixture, because `store_record` needs a `D1Database` and cannot run
    /// natively. So NOTHING bound the writer to the predicate: the gate
    /// changed the `sigValid` bind to `None` and the entire suite stayed
    /// green while every new production row landed in the legacy tier and
    /// #283 did nothing at all. That is the "right check that never runs"
    /// failure (epoch Rule 6b) in its purest form.
    ///
    /// This drives the REAL writer — `potparty_insert_query`, the exact value
    /// `store_record` executes — and replays its OWN sql and bind list
    /// against real SQLite with the production migrations, then reads the
    /// column back. Binding a constant, a `None`, or the wrong record all
    /// fail it.
    #[test]
    fn the_admission_write_latches_sig_valid_through_the_real_writer() {
        let conn = production_schema_db();
        let honest = golden_potparty_record("txGOLDEN");
        exec_query(&conn, potparty_insert_query(&honest, 1_234).query());

        let (latched, at): (Option<i64>, i64) = conn
            .query_row(
                "SELECT sigValid, createdAt FROM potparty_records WHERE txid = 'txGOLDEN'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("the row landed");
        assert_eq!(
            latched,
            Some(1),
            "the REAL client's genuinely-signed marker must latch 1 through the \
             REAL writer — a constant/None/absent bind fails here"
        );
        assert_eq!(at, 1_234, "and createdAt is still the SERVER's stamp");

        // A tampered twin latches 0 — so the cell measures the PREDICATE's
        // verdict flowing through the writer, not just "something was bound".
        let mut forged = golden_potparty_record("txFORGED");
        forged.recovery_height += 1;
        exec_query(&conn, potparty_insert_query(&forged, 1_235).query());
        let forged_latched: Option<i64> = conn
            .query_row(
                "SELECT sigValid FROM potparty_records WHERE txid = 'txFORGED'",
                [],
                |r| r.get(0),
            )
            .expect("the forged row landed too — the latch NEVER refuses an admission");
        assert_eq!(forged_latched, Some(0));

        // Both rows are present: the latch is a hint, never an admission gate.
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM potparty_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2, "a 0-latched marker is STORED, never refused");
    }

    /// The FROZEN cross-repo hopparty golden marker (bsv-low #315) —
    /// RFC6979-deterministic real signatures. The only honest artifact
    /// available here that a REAL signature check can pass; a hand-built
    /// record would make the cell blind to the dimension it exists to
    /// measure (epoch Rule 18).
    fn golden_hopparty_record(marker_txid: &str, pays: bool) -> HoppartyRecord {
        let m = overlay_discovery::hopparty::parse_hopparty_marker(
            &hex::decode(overlay_discovery::hopparty::GOLDEN_HOPPARTY_HEX).unwrap(),
        )
        .expect("the frozen golden parses");
        // The CONTAINER's own output at hopVout, as the lookup service
        // decodes it: honest = really pays the claimed value to the claimed
        // settle key.
        let lock =
            overlay_discovery::hopparty::validity::expected_hop_lock_hex(&m.seat_settle_pubkey);
        HoppartyRecord {
            identity: hex::encode(&m.identity),
            opponent_identity: hex::encode(&m.opponent),
            game_id: hex::encode(m.game_id),
            hop_vout: m.hop_vout,
            hop_sats: m.hop_sats,
            seat_settle_pubkey: hex::encode(&m.seat_settle_pubkey),
            seat_sig_hex: hex::encode(&m.seat_sig),
            identity_sig_hex: hex::encode(&m.identity_sig),
            hop_lock_hex: if pays { lock } else { None },
            hop_sats_on_chain: if pays { Some(m.hop_sats) } else { None },
            container_outputs: 2,
            txid: marker_txid.to_string(),
            output_index: 1,
            created_at: 0,
        }
    }

    /// THE HOPPARTY WRITER PIN (bsv-low #362) — the #283 HIGH-2 lesson,
    /// applied before it could be repeated.
    ///
    /// `store_record` needs a `D1Database` and cannot run natively, so
    /// nothing else in this repo binds the writer to the predicate: replacing
    /// its body with an inline `INSERT … VALUES (…, NULL)` would leave every
    /// new production row in the legacy tier with the suite green (that is
    /// exactly what happened to potparty: 293 passed, 0 failed). This drives
    /// the REAL writer — `hopparty_insert_query`, the exact value
    /// `store_record` executes — replays its OWN sql and bind list against
    /// real SQLite with the production migrations, and reads the column back.
    #[test]
    fn the_hopparty_admission_write_latches_marker_valid_through_the_real_writer() {
        let conn = production_schema_db();
        let honest = golden_hopparty_record("txHOPGOLDEN", true);
        exec_query(&conn, hopparty_insert_query(&honest, 4_321).query());

        let (latched, at): (Option<i64>, i64) = conn
            .query_row(
                "SELECT markerValid, createdAt FROM hopparty_records \
                 WHERE txid = 'txHOPGOLDEN'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("the row landed");
        assert_eq!(
            latched,
            Some(1),
            "the REAL client's genuinely-signed marker, in a container that \
             really pays it, must latch 1 through the REAL writer — a \
             constant/None/absent bind fails here"
        );
        assert_eq!(at, 4_321, "and createdAt is still the SERVER's stamp");

        // A container that does NOT pay latches 0 — so the cell measures the
        // PREDICATE's verdict flowing through the writer, not just "something
        // was bound". The signatures are byte-identical in both rows.
        let unpaid = golden_hopparty_record("txHOPUNPAID", false);
        assert_eq!(unpaid.seat_sig_hex, honest.seat_sig_hex, "same signatures");
        exec_query(&conn, hopparty_insert_query(&unpaid, 4_322).query());
        let unpaid_latched: Option<i64> = conn
            .query_row(
                "SELECT markerValid FROM hopparty_records WHERE txid = 'txHOPUNPAID'",
                [],
                |r| r.get(0),
            )
            .expect("the unpaid row landed too — the latch NEVER refuses an admission");
        assert_eq!(unpaid_latched, Some(0));

        // …and a tampered SIGNATURE latches 0 with the container untouched,
        // so both halves of the predicate are observed through the writer.
        let mut forged = golden_hopparty_record("txHOPFORGED", true);
        forged.identity_sig_hex = "30".to_string() + &forged.identity_sig_hex[2..];
        forged.identity_sig_hex.push_str("00");
        exec_query(&conn, hopparty_insert_query(&forged, 4_323).query());
        let forged_latched: Option<i64> = conn
            .query_row(
                "SELECT markerValid FROM hopparty_records WHERE txid = 'txHOPFORGED'",
                [],
                |r| r.get(0),
            )
            .expect("the forged row landed too");
        assert_eq!(forged_latched, Some(0));

        // All three rows are present: the latch is a hint, never a gate.
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM hopparty_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3, "a 0-latched marker is STORED, never refused");
    }

    // ── #355 + #367 — the RE-LATCH pass, driven against real SQLite ──────

    /// Replay a production [`Query`] as a SELECT and hand each row back as a
    /// JSON object keyed by the statement's OWN column names.
    ///
    /// Deserialising THAT through the production row struct is the point: it
    /// pins the SELECT list against the `serde(rename)`s that read it, which
    /// is the boundary a hand-written row mapper in a test would silently
    /// paper over (epoch Rule 16 — a property spanning two components cannot
    /// be pinned inside either one).
    fn select_json(
        conn: &rusqlite::Connection,
        sql: &str,
        binds: &[i64],
    ) -> Vec<serde_json::Value> {
        let mut stmt = conn.prepare(sql).expect("the production SELECT prepares");
        let cols: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(binds.iter()), |r| {
                let mut obj = serde_json::Map::new();
                for (i, name) in cols.iter().enumerate() {
                    let v = match r.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => serde_json::Value::Null,
                        rusqlite::types::ValueRef::Integer(i) => serde_json::json!(i),
                        rusqlite::types::ValueRef::Real(f) => serde_json::json!(f),
                        rusqlite::types::ValueRef::Text(t) => {
                            serde_json::json!(String::from_utf8_lossy(t))
                        }
                        rusqlite::types::ValueRef::Blob(b) => serde_json::json!(hex::encode(b)),
                    };
                    obj.insert(name.clone(), v);
                }
                Ok(serde_json::Value::Object(obj))
            })
            .expect("query_map");
        rows.map(|r| r.expect("row")).collect()
    }

    /// `potparty_records` as a [`crate::relatch::RelatchTable`], over real
    /// SQLite, executing the SHIPPED SQL and the SHIPPED row mapper.
    ///
    /// MODELLING BOUNDARY, stated here as well as in the cells (epoch
    /// Rule 22): this drives the production statements, binds, row struct and
    /// pass logic end to end. What it cannot drive is the `D1Database`
    /// round-trip inside `D1PotpartyStorage`'s own `RelatchTable` impl —
    /// `worker::D1Database` has no native constructor. That impl is three
    /// lines per method and is where a wrong SQL builder would be substituted;
    /// `the_d1_relatch_impls_use_the_shipped_statements` covers that seam by
    /// value.
    struct SqliteRelatchPotparty<'a>(&'a rusqlite::Connection);

    #[async_trait(?Send)]
    impl crate::relatch::RelatchTable for SqliteRelatchPotparty<'_> {
        type Row = PotpartyRelatchRow;
        fn table(&self) -> &'static str {
            POTPARTY_TABLE
        }
        fn rowid(row: &Self::Row) -> i64 {
            row.rowid
        }
        fn stored(row: &Self::Row) -> Option<bool> {
            row.stored
        }
        async fn scan(&self, after: i64, limit: u64) -> Result<Vec<Self::Row>, String> {
            Ok(
                select_json(self.0, &potparty_relatch_scan_sql(), &[after, limit as i64])
                    .into_iter()
                    .map(|v| {
                        serde_json::from_value::<PotpartyRelatchDbRow>(v)
                            .expect("the shipped SELECT list feeds the shipped row struct")
                            .into_row()
                    })
                    .collect(),
            )
        }
        async fn relatch_if_changed(&self, row: &Self::Row) -> Result<Option<bool>, String> {
            let update = potparty_relatch_query(&row.record);
            let verdict = update.sig_valid();
            if row.stored == Some(verdict) {
                return Ok(None);
            }
            exec_query(self.0, update.query());
            Ok(Some(verdict))
        }
        async fn census(&self, after: i64) -> Result<crate::relatch::RelatchCensus, String> {
            let rows = select_json(self.0, &potparty_relatch_census_sql(), &[after]);
            let r: RelatchCensusRow =
                serde_json::from_value(rows.into_iter().next().expect("one aggregate row"))
                    .expect("the census SELECT feeds the census row struct");
            Ok(crate::relatch::RelatchCensus {
                remaining: r.remaining as u64,
                still_null: r.still_null as u64,
            })
        }
    }

    /// `hopparty_records` as a [`crate::relatch::RelatchTable`] — same shape,
    /// same boundary.
    struct SqliteRelatchHopparty<'a>(&'a rusqlite::Connection);

    #[async_trait(?Send)]
    impl crate::relatch::RelatchTable for SqliteRelatchHopparty<'_> {
        type Row = HoppartyRelatchRow;
        fn table(&self) -> &'static str {
            HOPPARTY_TABLE
        }
        fn rowid(row: &Self::Row) -> i64 {
            row.rowid
        }
        fn stored(row: &Self::Row) -> Option<bool> {
            row.stored
        }
        async fn scan(&self, after: i64, limit: u64) -> Result<Vec<Self::Row>, String> {
            Ok(
                select_json(self.0, &hopparty_relatch_scan_sql(), &[after, limit as i64])
                    .into_iter()
                    .map(|v| {
                        serde_json::from_value::<HoppartyRelatchDbRow>(v)
                            .expect("the shipped SELECT list feeds the shipped row struct")
                            .into_row()
                    })
                    .collect(),
            )
        }
        async fn relatch_if_changed(&self, row: &Self::Row) -> Result<Option<bool>, String> {
            let update = hopparty_relatch_query(&row.record);
            let verdict = update.marker_valid();
            if row.stored == Some(verdict) {
                return Ok(None);
            }
            exec_query(self.0, update.query());
            Ok(Some(verdict))
        }
        async fn census(&self, after: i64) -> Result<crate::relatch::RelatchCensus, String> {
            let rows = select_json(self.0, &hopparty_relatch_census_sql(), &[after]);
            let r: RelatchCensusRow =
                serde_json::from_value(rows.into_iter().next().expect("one aggregate row"))
                    .expect("the census SELECT feeds the census row struct");
            Ok(crate::relatch::RelatchCensus {
                remaining: r.remaining as u64,
                still_null: r.still_null as u64,
            })
        }
    }

    /// The cursor store, running the SHIPPED `relatch_cursors` statements
    /// against the production schema.
    struct SqliteCursors<'a>(&'a rusqlite::Connection);

    #[async_trait(?Send)]
    impl crate::relatch::RelatchCursorStore for SqliteCursors<'_> {
        async fn load(&self, table: &str) -> Result<crate::relatch::RelatchCursor, String> {
            let mut stmt = self
                .0
                .prepare(crate::relatch::RELATCH_CURSOR_LOAD_SQL)
                .expect("prepares");
            let got = stmt.query_row([table], |r| {
                Ok(crate::relatch::RelatchCursor {
                    cursor: r.get::<_, i64>(0)?,
                    sweeps: r.get::<_, i64>(1)? as u64,
                })
            });
            match got {
                Ok(c) => Ok(c),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    Ok(crate::relatch::RelatchCursor::default())
                }
                Err(e) => Err(e.to_string()),
            }
        }
        async fn store(&self, table: &str, c: crate::relatch::RelatchCursor) -> Result<(), String> {
            self.0
                .execute(
                    crate::relatch::RELATCH_CURSOR_STORE_SQL,
                    rusqlite::params![table, c.cursor, c.sweeps as i64],
                )
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
    }

    /// Force a stored verdict WITHOUT going through the production writer —
    /// the only way to manufacture the two populations the pass exists for
    /// (a legacy `NULL`, and a `0`/`1` a transient fault got wrong).
    fn poison_potparty(conn: &rusqlite::Connection, txid: &str, v: Option<i64>) {
        conn.execute(
            "UPDATE potparty_records SET sigValid = ?1 WHERE txid = ?2",
            rusqlite::params![v, txid],
        )
        .expect("poison");
    }

    fn stored_potparty(conn: &rusqlite::Connection, txid: &str) -> Option<i64> {
        conn.query_row(
            "SELECT sigValid FROM potparty_records WHERE txid = ?1",
            [txid],
            |r| r.get(0),
        )
        .expect("the row exists")
    }

    /// THE #355 CLOSURE CRITERION, EXECUTED: every row's `sigValid` equals
    /// `record_sig_valid` recomputed now — reached from ALL THREE starting
    /// states, through the SHIPPED statements against the production schema.
    ///
    /// A `WHERE sigValid IS NULL` pass passes the first leg and fails the
    /// other two, which is exactly why the criterion is a fixpoint and not a
    /// NULL census (bsv-low#355's WIDENED section).
    #[tokio::test]
    async fn the_relatch_pass_reaches_the_fixpoint_from_null_zero_and_one() {
        let conn = production_schema_db();
        // An honest, genuinely-signed marker and a tampered twin, both landed
        // through the REAL admission writer so the honest artifact is honest
        // in the dimension under test (epoch Rule 18).
        let honest = golden_potparty_record("txRELATCHOK");
        let mut forged = golden_potparty_record("txRELATCHBAD");
        forged.recovery_height += 1;
        exec_query(&conn, potparty_insert_query(&honest, 10).query());
        exec_query(&conn, potparty_insert_query(&forged, 11).query());

        // The three populations, manufactured:
        //  - LEGACY: admitted before the migration, never evaluated;
        //  - FAULTED 0: an honest row a transient predicate fault refuted —
        //    the row that sorts BELOW the legacy tier, forever;
        //  - FAULTED 1: a junk row wrongly accepted.
        poison_potparty(&conn, "txRELATCHOK", None);
        poison_potparty(&conn, "txRELATCHBAD", Some(1));
        let legacy = golden_potparty_record("txRELATCHLEGACY");
        exec_query(&conn, potparty_insert_query(&legacy, 12).query());
        poison_potparty(&conn, "txRELATCHLEGACY", None);
        // …and one already-converged row, which must cost no write at all.
        let mut converged = golden_potparty_record("txRELATCHCONV");
        converged.pot_vout = 7;
        exec_query(&conn, potparty_insert_query(&converged, 13).query());
        assert_eq!(stored_potparty(&conn, "txRELATCHCONV"), Some(0));

        let table = SqliteRelatchPotparty(&conn);
        let cursors = SqliteCursors(&conn);
        let s = crate::relatch::relatch_pass(&table, &cursors, 100).await;

        assert_eq!(s.scanned, 4);
        assert_eq!(s.latched, 2, "both NULL rows now carry a verdict");
        assert_eq!(
            s.promoted, 0,
            "no 0→1 here: the faulted honest row was NULLed, not zeroed"
        );
        assert_eq!(s.demoted, 1, "the wrongly-accepted forged row is refuted");
        assert_eq!(s.errors, 0);
        assert_eq!(s.still_null, 0, "the legacy tier is gone for these rows");

        assert_eq!(stored_potparty(&conn, "txRELATCHOK"), Some(1));
        assert_eq!(stored_potparty(&conn, "txRELATCHBAD"), Some(0));
        assert_eq!(stored_potparty(&conn, "txRELATCHLEGACY"), Some(1));
        assert_eq!(stored_potparty(&conn, "txRELATCHCONV"), Some(0));

        // The 0→1 repair — the population a NULL-census structurally skips.
        poison_potparty(&conn, "txRELATCHOK", Some(0));
        let s2 = crate::relatch::relatch_pass(&table, &cursors, 100).await;
        assert_eq!(
            (s2.latched, s2.promoted, s2.demoted),
            (0, 1, 0),
            "a row a transient fault refuted is REPAIRED, not skipped"
        );
        assert_eq!(stored_potparty(&conn, "txRELATCHOK"), Some(1));

        // Idempotence: the fixpoint is reached, so a further tick writes
        // nothing…
        let s3 = crate::relatch::relatch_pass(&table, &cursors, 100).await;
        assert_eq!(s3.changed(), 0);
        assert_eq!(s3.scanned, 4, "…while still VISITING every row");

        // …and nothing was ever removed: the pass moves a sort key, never a
        // row (epoch Rule 23).
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM potparty_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 4);
    }

    /// The same criterion for `hopparty_records` (#367) — the table with NO
    /// other repair path at all, because a hop marker rides a transaction
    /// that is already on chain.
    #[tokio::test]
    async fn the_hopparty_relatch_pass_reaches_the_fixpoint_from_null_zero_and_one() {
        let conn = production_schema_db();
        let honest = golden_hopparty_record("txHOPRELATCH", true);
        let unpaid = golden_hopparty_record("txHOPRELATCHBAD", false);
        exec_query(&conn, hopparty_insert_query(&honest, 20).query());
        exec_query(&conn, hopparty_insert_query(&unpaid, 21).query());
        conn.execute(
            "UPDATE hopparty_records SET markerValid = NULL WHERE txid = 'txHOPRELATCH'",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE hopparty_records SET markerValid = 1 WHERE txid = 'txHOPRELATCHBAD'",
            [],
        )
        .unwrap();

        let table = SqliteRelatchHopparty(&conn);
        let cursors = SqliteCursors(&conn);
        let s = crate::relatch::relatch_pass(&table, &cursors, 100).await;
        assert_eq!((s.scanned, s.latched, s.demoted, s.errors), (2, 1, 1, 0));

        let read = |txid: &str| -> Option<i64> {
            conn.query_row(
                "SELECT markerValid FROM hopparty_records WHERE txid = ?1",
                [txid],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(read("txHOPRELATCH"), Some(1), "the legacy row is repaired");
        assert_eq!(read("txHOPRELATCHBAD"), Some(0), "…and the junk refuted");

        // 0 → 1, the population a NULL census skips.
        conn.execute(
            "UPDATE hopparty_records SET markerValid = 0 WHERE txid = 'txHOPRELATCH'",
            [],
        )
        .unwrap();
        let s2 = crate::relatch::relatch_pass(&table, &cursors, 100).await;
        assert_eq!((s2.promoted, s2.latched, s2.demoted), (1, 0, 0));
        assert_eq!(read("txHOPRELATCH"), Some(1));
    }

    /// The CURSOR is durable in the production table and the sweep WRAPS —
    /// driven through the shipped `relatch_cursors` statements, so a cursor
    /// that never persists (the failure that turns the pass into a repeated
    /// head-scan of the first page) is visible here.
    #[tokio::test]
    async fn the_relatch_cursor_persists_and_wraps_through_the_production_table() {
        let conn = production_schema_db();
        for i in 0..5u32 {
            let mut r = golden_potparty_record(&format!("txCURSOR{i}"));
            r.pot_vout = i;
            exec_query(&conn, potparty_insert_query(&r, 100 + i as i64).query());
        }
        let table = SqliteRelatchPotparty(&conn);
        let cursors = SqliteCursors(&conn);

        let a = crate::relatch::relatch_pass(&table, &cursors, 2).await;
        assert_eq!((a.scanned, a.wrapped, a.remaining), (2, false, 3));
        let persisted: i64 = conn
            .query_row(
                "SELECT cursorRowid FROM relatch_cursors WHERE tableName = ?1",
                [POTPARTY_TABLE],
                |r| r.get(0),
            )
            .expect("the cursor row landed in the PRODUCTION table");
        assert_eq!(persisted, a.cursor);
        assert!(a.cursor > 0, "the cursor advanced");

        let b = crate::relatch::relatch_pass(&table, &cursors, 2).await;
        assert!(
            b.cursor > a.cursor,
            "the next tick RESUMED, never restarted"
        );
        let c = crate::relatch::relatch_pass(&table, &cursors, 2).await;
        assert_eq!(
            (c.scanned, c.wrapped, c.cursor, c.sweeps, c.remaining),
            (1, true, 0, 1, 0),
            "the tail wraps into a new sweep — the fixpoint, not a backfill"
        );
        let (persisted, sweeps): (i64, i64) = conn
            .query_row(
                "SELECT cursorRowid, sweeps FROM relatch_cursors WHERE tableName = ?1",
                [POTPARTY_TABLE],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((persisted, sweeps), (0, 1));

        // The two tables keep INDEPENDENT cursors — one key each, so a busy
        // potparty sweep can never starve the hopparty one.
        let hop = SqliteRelatchHopparty(&conn);
        let h = crate::relatch::relatch_pass(&hop, &cursors, 2).await;
        assert_eq!(h.scanned, 0, "an empty hopparty table");
        let keys: i64 = conn
            .query_row("SELECT COUNT(*) FROM relatch_cursors", [], |r| r.get(0))
            .unwrap();
        assert_eq!(keys, 2, "one cursor per table");
    }

    /// The scan must NEVER filter on the verdict column: a `WHERE sigValid IS
    /// NULL` pass would leave every faulted `0` unreachable forever.
    ///
    /// Asserted on the CONSTRUCT and, above, behaviourally — this cell is the
    /// belt (epoch Rule 12a addendum: the behaviour is the bar, the scan is
    /// the belt), and it is a POSITIVE count so it fails loudly if the needle
    /// stops matching (Rule 9).
    #[test]
    fn neither_relatch_scan_filters_on_its_verdict_column() {
        for (sql, col) in [
            (
                potparty_relatch_scan_sql(),
                overlay_discovery::potparty::validity::SIG_VALID_COLUMN,
            ),
            (
                hopparty_relatch_scan_sql(),
                overlay_discovery::hopparty::validity::MARKER_VALID_COLUMN,
            ),
        ] {
            assert_eq!(
                sql.matches(col).count(),
                1,
                "the verdict column appears ONCE — in the SELECT list — and \
                 never in a predicate: {sql}"
            );
            let (select_list, rest) = sql.split_once(" FROM ").expect("one FROM");
            assert!(
                select_list.contains(col),
                "…and that one is the SELECT: {sql}"
            );
            assert!(
                !rest.contains(col),
                "a filter on the verdict column would strand every faulted 0: {sql}"
            );
            assert!(
                rest.contains("WHERE rowid > ?") && rest.contains("ORDER BY rowid ASC"),
                "the scan is rowid-cursored over ALL rows: {sql}"
            );
        }
    }

    /// The D1 impls must run the SHIPPED statements, not a private
    /// transcription — the seam the SQLite tier above cannot reach (Rule 22).
    /// Asserted by VALUE: the builders are pure, so the exact strings the D1
    /// path prepares are checkable without a `D1Database`.
    #[test]
    fn the_relatch_statements_are_the_ones_the_writers_build() {
        let up = potparty_relatch_query(&golden_potparty_record("txQ"));
        assert_eq!(
            up.query().sql(),
            "UPDATE potparty_records SET sigValid = ? WHERE txid = ? AND outputIndex = ?"
        );
        assert_eq!(up.query().params().len(), 3, "verdict, txid, outputIndex");
        assert!(up.sig_valid(), "the golden's verdict rides the update");

        let hup = hopparty_relatch_query(&golden_hopparty_record("txQ", true));
        assert_eq!(
            hup.query().sql(),
            "UPDATE hopparty_records SET markerValid = ? WHERE txid = ? AND outputIndex = ?"
        );
        assert_eq!(hup.query().params().len(), 3);
        assert!(hup.marker_valid());

        // Addressed by the OUTPOINT PRIMARY KEY, so a stale cursor can never
        // splash a second row.
        for sql in [up.query().sql(), hup.query().sql()] {
            assert!(sql.contains("WHERE txid = ? AND outputIndex = ?"), "{sql}");
            assert!(!sql.contains("INSERT") && !sql.contains("REPLACE"), "{sql}");
        }
    }

    /// The hopparty writer's bind list and its SQL must agree in COUNT — a
    /// dropped bind shifts every column silently (epoch Rule 9).
    #[test]
    fn the_hopparty_admission_write_binds_every_placeholder() {
        let insert = hopparty_insert_query(&golden_hopparty_record("txN", true), 0);
        let q = insert.query();
        assert_eq!(q.sql().matches('?').count(), 15, "15 placeholders");
        assert_eq!(q.params().len(), 15, "15 binds");
        assert_eq!(
            q.sql().matches(',').count(),
            14 + 14,
            "15 columns + 15 values = 28 separating commas"
        );
        assert!(
            q.sql().contains("markerValid"),
            "the latch column is written"
        );
        assert!(
            matches!(q.params()[14], crate::d1::QVal::Int(1)),
            "the LAST bind is the latch verdict for the golden: {:?}",
            q.params()[14]
        );
        assert!(
            insert.marker_valid(),
            "the accessor reports the value actually bound — telemetry that \
             reads it cannot disagree with the row"
        );
    }

    /// The writer's bind list and its SQL must agree in COUNT — a dropped
    /// bind shifts every column silently. Positive counts on both sides of a
    /// value that cannot move in sympathy (epoch Rule 9).
    #[test]
    fn the_admission_write_binds_every_placeholder() {
        let insert = potparty_insert_query(&golden_potparty_record("txN"), 0);
        let q = insert.query();
        assert_eq!(q.sql().matches('?').count(), 13, "13 placeholders");
        assert_eq!(q.params().len(), 13, "13 binds");
        assert_eq!(
            q.sql().matches(',').count(),
            12 + 12,
            "13 columns + 13 values = 24 separating commas"
        );
        assert!(q.sql().contains("sigValid"), "the latch column is written");
        assert!(
            matches!(q.params()[12], crate::d1::QVal::Int(1)),
            "the LAST bind is the latch verdict for the golden: {:?}",
            q.params()[12]
        );
    }

    /// THE READ PATH REFUSES A WRITE, WHATEVER IT IS SPELLED (bsv-low #283,
    /// gate round 3).
    ///
    /// The round-3 gate defeated the round-2 source pin by changing one
    /// keyword — `INSERT INTO` for `INSERT OR IGNORE INTO` — and smuggling a
    /// NULL-binding write through `PotpartyDb::fetch_all`, with
    /// `potparty_insert_query` still called and `record_sig_valid` still
    /// evaluated once. 294 passed, 0 failed, and every new production row
    /// would have landed in the legacy tier. The bar is now
    /// `is_select_only`, and this cell measures the PROPERTY rather than the
    /// spelling: a table of write forms, none of which shares a keyword
    /// prefix with the others.
    ///
    /// POSITIVE CONTROL FIRST (epoch Rule 9, "the code under test is never
    /// reached"): every read this module actually issues is driven through
    /// the bar from its REAL builder, so a bar that refused everything —
    /// which would pass every refusal leg — fails here.
    ///
    /// BOUNDARY (epoch Rule 22): this drives the PREDICATE. `fetch_all`'s
    /// use of it needs a `D1Database` and is not reachable natively; that
    /// call site is one `if` and is what the belt pin above watches.
    #[test]
    fn the_potparty_read_path_admits_only_selects() {
        use crate::d1_discovery::potparty_write::is_select_only;

        // ── Positive control: the REAL production readers, not hand-fed SQL.
        for sql in [
            potparty_list_for_identity_sql(),
            list_for_pot_sql(POTPARTY_SELECT),
            POTPARTY_SELECT.to_string(),
        ] {
            assert!(
                is_select_only(&sql),
                "a read this module really issues must clear the bar: {}",
                &sql[..sql.len().min(60)]
            );
        }

        // ── Every write form. The round-3 injection is the second entry.
        for sql in [
            "INSERT OR IGNORE INTO potparty_records (identity) VALUES (?)",
            "INSERT INTO potparty_records (identity, sigValid) VALUES (?, NULL)",
            "insert into potparty_records (identity) values (?)",
            "  \n\t INSERT OR REPLACE INTO potparty_records (identity) VALUES (?)",
            "REPLACE INTO potparty_records (identity) VALUES (?)",
            "UPDATE potparty_records SET sigValid = NULL",
            "DELETE FROM potparty_records",
            "PRAGMA writable_schema = ON",
            "",
        ] {
            assert!(
                !is_select_only(sql),
                "the read path must refuse this, whatever it is spelled: {sql}"
            );
        }

        // Case and leading whitespace do not decide it either way.
        assert!(is_select_only("   \n select 1"));
        assert!(is_select_only("SELECT 1"));
    }

    /// Strip Rust comments, leaving string literals intact.
    ///
    /// A scanner that counts PROSE has the same defect as a comment claiming
    /// a fix — the text asserts the property instead of the code having it
    /// (epoch Rule 9, "the scanner counts PROSE"). This file has no raw
    /// strings and no `'"'` char literal, which is what keeps a
    /// character-level stripper honest here; if either appears, this helper
    /// must become a real parse.
    fn strip_rust_comments(src: &str) -> String {
        let b: Vec<char> = src.chars().collect();
        let mut out = String::with_capacity(src.len());
        let (mut i, mut in_str, mut in_line, mut block) = (0usize, false, false, 0usize);
        while i < b.len() {
            let c = b[i];
            let n = if i + 1 < b.len() { b[i + 1] } else { '\0' };
            if in_line {
                if c == '\n' {
                    in_line = false;
                    out.push(c);
                }
                i += 1;
            } else if block > 0 {
                if c == '/' && n == '*' {
                    block += 1;
                    i += 2;
                } else if c == '*' && n == '/' {
                    block -= 1;
                    i += 2;
                } else {
                    if c == '\n' {
                        out.push(c);
                    }
                    i += 1;
                }
            } else if in_str {
                if c == '\\' {
                    out.push(c);
                    if i + 1 < b.len() {
                        out.push(n);
                    }
                    i += 2;
                } else {
                    if c == '"' {
                        in_str = false;
                    }
                    out.push(c);
                    i += 1;
                }
            } else if c == '/' && n == '/' {
                in_line = true;
                i += 2;
            } else if c == '/' && n == '*' {
                block = 1;
                i += 2;
            } else {
                if c == '"' {
                    in_str = true;
                }
                out.push(c);
                i += 1;
            }
        }
        out
    }

    /// This module's PRODUCTION source, comments stripped and this test
    /// module removed — so a scan can never count its own needles or a
    /// fixture's SQL (epoch Rule 9, third failure mode).
    fn production_source() -> String {
        let stripped = strip_rust_comments(include_str!("d1_discovery.rs"));
        let test_marker = ["#[cfg(", "test)]"].concat();
        assert_eq!(
            stripped.matches(&test_marker).count(),
            1,
            "exactly one test module in this file — the split below assumes it"
        );
        let prod = stripped
            .split(&test_marker)
            .next()
            .expect("split always yields a first part")
            .to_string();
        assert!(
            prod.contains(&["pub mod potparty", "_write {"].concat()),
            "the production prefix must actually contain the write module"
        );
        prod
    }

    /// THE WRITE IS UNBYPASSABLE — the belt behind the compile error
    /// (bsv-low #283, gate round 2 MED-2).
    ///
    /// Round 1's writer pin was real and pinned a function nobody was
    /// obliged to call: the gate replaced `store_record`'s body with an
    /// inline `INSERT … sigValid) VALUES (…, NULL)` and got 293 passed, 0
    /// failed. `potparty_write` closes that by CAPABILITY — the storage impl
    /// can no longer reach a `D1Database`, so that injection does not
    /// compile.
    ///
    /// This cell is the BELT behind `potparty_write::is_select_only`, which
    /// is what actually closes the `fetch_all` door. Positive exact counts,
    /// never `assert!(!contains)` (epoch Rule 9); needles split so they
    /// cannot match this assertion's own source; run over comment-stripped,
    /// test-module-free source.
    ///
    /// The INSERT needle is deliberately `INTO POTPARTY_RECORDS` over
    /// UPPERCASED source, NOT the full `INSERT OR IGNORE INTO …` statement
    /// head. Round 2 used the full head and the round-3 gate walked through
    /// it by spelling the smuggled write `INSERT INTO` — one keyword, 294
    /// passed, 0 failed, #283 inoperative. Narrowing to `INTO
    /// potparty_records` was still not enough: my own RED-verification then
    /// walked through THAT with a lowercase `replace into`. Every write form
    /// (`INSERT`, `INSERT OR IGNORE`, `INSERT OR REPLACE`, `REPLACE`) must
    /// name the table with `INTO`, and SQL is case-insensitive, so the needle
    /// is now blind on both axes the two injections used.
    ///
    /// BOUNDARY (epoch Rule 22): this pins the SHAPE of the production
    /// source, not the D1 round-trip, and it is a belt — the bar is the
    /// runtime guard. The verdict flowing into the bound column is pinned
    /// behaviourally by
    /// `the_admission_write_latches_sig_valid_through_the_real_writer`.
    #[test]
    fn exactly_one_potparty_insert_statement_exists_in_this_module() {
        let prod = production_source();

        // CASE-BLIND, like the runtime bar. My own round-3 RED-verification
        // caught this pin one more time: a lowercase `replace into
        // potparty_records` smuggled through `fetch_all` compiled and left
        // this cell GREEN — the needle was still spelling-sensitive, just one
        // axis narrower than before. `is_select_only` refuses it at runtime
        // either way, which is the argument for the bar being the bar and
        // this being the belt.
        let shouty = prod.to_ascii_uppercase();
        assert_eq!(
            shouty
                .matches(&["INTO POTPARTY", "_RECORDS"].concat())
                .count(),
            1,
            "exactly ONE statement in this module writes `potparty_records` — \
             a second one (inline in `store_record`, or smuggled through \
             `fetch_all` under ANY spelling OR CASE) is how the latch gets \
             dropped while the suite stays green"
        );
        assert_eq!(
            prod.matches(&["potparty_insert", "_query("].concat())
                .count(),
            2,
            "the pure producer is DEFINED once and CALLED once (by \
             `store_record`) — a 1 here means the only call site is gone"
        );
        assert_eq!(
            prod.matches(&["record_sig", "_valid("].concat()).count(),
            2,
            "the latch predicate has exactly TWO call sites — the admission \
             INSERT and the #355 re-latch UPDATE — and each evaluates it ONCE \
             per row, reading the verdict off the built query rather than \
             re-deriving it (gate round 2 LOW-2). A 3 means somebody added a \
             second derivation beside a write; a 1 means a write stopped \
             latching"
        );

        // ── The #355 RE-LATCH is the SECOND write this table now has, so it
        // gets the same treatment as the first: exactly one UPDATE statement,
        // reachable only through `PotpartyDb::relatch`, which accepts only a
        // `LatchedPotpartyRelatch`. `INTO` does not appear in an UPDATE, so
        // the needle above cannot see this statement at all — that gap IS the
        // reason for this assertion (epoch Rule 7: when one instance of a
        // class surfaces, sweep for the rest). Case-blind, split needle.
        assert_eq!(
            shouty
                .matches(&["UPDATE POTPARTY", "_RECORDS"].concat())
                .count(),
            1,
            "exactly ONE statement in this module UPDATEs `potparty_records` \
             — a second one is how a re-latch that binds a caller-supplied \
             verdict (rather than a re-derived one) gets in"
        );
        assert_eq!(
            prod.matches(&["potparty_relatch", "_query("].concat())
                .count(),
            2,
            "the re-latch producer is DEFINED once and CALLED once (by the \
             `RelatchTable` impl) — a 1 means the only call site is gone and \
             the pass has stopped repairing anything"
        );

        // ── The read-path bar's CALL SITES. `fetch_all` needs a live
        // `D1Database`, so no native cell can watch it refuse — the same
        // unreachability that produced this whole class. The predicate is
        // pinned behaviourally by `the_potparty_read_path_admits_only_selects`;
        // this pins that BOTH `fetch_all`s still consult it, scoped to the
        // guard EXPRESSION rather than a region (epoch Rule 9, fourth failure
        // mode), so `&& false` or a swapped argument changes the needle.
        //
        // TWO since #362: `potparty_write::PotpartyDb` and
        // `hopparty_write::HoppartyDb` share ONE bar rather than each growing
        // a copy (epoch Rule 10 — the durable fix for "these must agree" is
        // one predicate, not two plus a test).
        assert_eq!(
            prod.matches(&["if !is_select", "_only(q.sql()) {"].concat())
                .count(),
            2,
            "both read paths guard on the shared read bar, in exactly that \
             form — this is the door the round-3 gate walked through"
        );
        assert_eq!(
            prod.matches(&["is_select", "_only("].concat()).count(),
            3,
            "the read bar is DEFINED once and CALLED twice (the import names \
             it without parentheses) — a smaller number means a `fetch_all` \
             stopped consulting it"
        );
    }

    /// The #362 twin of the cell above, for `hopparty_records`. Same class,
    /// same two axes the potparty needle was defeated on (keyword, then
    /// case), so the needle is blind on both from the start and the real bar
    /// is still the capability: `hopparty_write` owns the only `D1Database`
    /// this module's hopparty storage can reach.
    ///
    /// BOUNDARY (epoch Rule 22): a belt. The verdict flowing into the bound
    /// column is pinned behaviourally by
    /// `the_hopparty_admission_write_latches_marker_valid_through_the_real_writer`.
    #[test]
    fn exactly_one_hopparty_insert_statement_exists_in_this_module() {
        let prod = production_source();

        let shouty = prod.to_ascii_uppercase();
        assert_eq!(
            shouty
                .matches(&["INTO HOPPARTY", "_RECORDS"].concat())
                .count(),
            1,
            "exactly ONE statement in this module writes `hopparty_records` — \
             a second one (inline in `store_record`, or smuggled through \
             `fetch_all` under ANY spelling OR CASE) is how the latch gets \
             dropped while the suite stays green"
        );
        assert_eq!(
            prod.matches(&["hopparty_insert", "_query("].concat())
                .count(),
            2,
            "the pure producer is DEFINED once and CALLED once (by \
             `store_record`) — a 1 here means the only call site is gone"
        );
        assert_eq!(
            prod.matches(&["record_marker", "_valid("].concat()).count(),
            2,
            "the latch predicate has exactly TWO call sites — the admission \
             INSERT and the #367 re-latch UPDATE — each evaluating it ONCE \
             per row and reading the verdict off the built query"
        );

        // The #367 re-latch write, treated exactly like the potparty twin —
        // `INTO` does not appear in an UPDATE, so the needle above is blind
        // to it.
        assert_eq!(
            shouty
                .matches(&["UPDATE HOPPARTY", "_RECORDS"].concat())
                .count(),
            1,
            "exactly ONE statement in this module UPDATEs `hopparty_records`"
        );
        assert_eq!(
            prod.matches(&["hopparty_relatch", "_query("].concat())
                .count(),
            2,
            "the re-latch producer is DEFINED once and CALLED once — this is \
             the ONLY repair path this table can ever have, because a hop \
             marker rides a transaction that is already on chain"
        );
    }

    // ── #283 — the marker-validity latch (`sigValid`) ────────────────────
    //
    // Everything above this line files markers with `sigValid = NULL`, i.e.
    // rows admitted BEFORE the latch migration. That is deliberate: those
    // cells keep measuring the LEGACY tier, which is exactly the population
    // whose behaviour must not change. The cells below file the same attacks
    // with the latch set, which is what a marker admitted by the deployed
    // writer carries.

    /// THE ATTACK THE QUOTA NEVER SAW (bsv-low#347 + #283a).
    ///
    /// `unknownPot = 0` means only "a `pot_records` row exists". `tm_pot`
    /// admits any structurally-covenant-shaped output with no signature, and
    /// `/submit`'s SEEN-gate is selected by a caller-supplied header — so an
    /// attacker files a `pot_records` row for a pot that does not exist, for
    /// FREE, and its ghost marker lands in tier 0 ordered `potCreatedAt
    /// DESC` (freshest first). It never touches `unknown_pot_quota` at all:
    /// no quota allocation, however clever, bounds this.
    ///
    /// What DOES bound it is that the window is per-IDENTITY. To appear in
    /// the victim's answer the ghost must NAME the victim's identity, and
    /// the marker's identity signature is over a challenge binding that
    /// identity — so a ghost naming the victim latches `sigValid = 0` and
    /// sorts behind every honest row, whatever `pot_records` says about it.
    ///
    /// Executed at 200 ghosts against a 100-pot page: TWICE the page size,
    /// all with fresher pot rows than every honest pot.
    #[test]
    fn free_ghost_pot_records_cannot_erase_the_victims_pots_real_sqlite() {
        let now = now_secs();
        let conn = production_schema_db();
        let victim = victim_id();
        let mut honest = Vec::new();
        for i in 0..100u32 {
            let pot = format!("{:064x}", 0x0000_1000u64 + i as u64);
            insert_pot(&conn, &pot, 0, 1_000 + i as i64, true);
            insert_potparty_latched(
                &conn,
                &victim,
                &pot,
                0,
                &format!("txM{i:03}"),
                1_000 + i as i64,
                None,
                Some(true),
            );
            honest.push(pot);
        }
        // 200 free ghosts: each gets its own fabricated `pot_records` row,
        // stamped NOW, so each is tier 0 AND newer than every honest pot.
        for i in 0..200u32 {
            let ghost = format!("{:064x}", 0xdead_0000u64 + i as u64);
            insert_pot(&conn, &ghost, 0, now, false);
            insert_potparty_latched(
                &conn,
                &victim,
                &ghost,
                0,
                &format!("txGP{i:03}"),
                now,
                None,
                Some(false),
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
        for pot in &honest {
            assert!(
                unique.contains(pot),
                "every honest pot must survive 200 free ghost pot_records rows: \
                 {} missing, {} pots returned",
                pot,
                unique.len()
            );
        }
        assert_eq!(unique.len(), 100, "and the page is the honest 100");
    }

    /// The SAME 200 ghosts against a victim whose markers are all LEGACY
    /// (`sigValid = NULL`) erase the page. This is the pre-latch behaviour,
    /// executed rather than asserted — it is what makes the cell above a
    /// measurement instead of a tautology (epoch Rule 12a: the control must
    /// be able to fail).
    #[test]
    fn free_ghost_pot_records_do_erase_legacy_unlatched_rows_real_sqlite() {
        let now = now_secs();
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
        for i in 0..200u32 {
            let ghost = format!("{:064x}", 0xdead_0000u64 + i as u64);
            insert_pot(&conn, &ghost, 0, now, false);
            insert_potparty(&conn, &victim, &ghost, 0, &format!("txGP{i:03}"), now, None);
        }
        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "potTxid",
        );
        let unique: std::collections::HashSet<&String> = got.iter().collect();
        assert!(
            !unique.contains(&format!("{:064x}", 0x0000_1000u64)),
            "PRE-LATCH CONTROL: free ghost pot rows DO erase an all-legacy page \
             — if this starts passing, the legacy tier changed and the residual \
             note in `sig_rank_expr` must move with it"
        );
    }

    /// #283a/#283b closed for latched rows: ghost markers can occupy NO
    /// promoted quota slot, because promotion now requires the pot's best
    /// marker to be something other than provably-forged. The #281 gate's
    /// executed numbers were "10 ghosts ⇒ the fresh pot goes ABSENT" and
    /// "50 ghosts ⇒ exactly 10 real pots displaced (the quota)". Re-measured
    /// here at 50 — five times the quota — with the fresh pot present and
    /// every real pot kept.
    #[test]
    fn latched_ghosts_take_no_promoted_slot_real_sqlite() {
        let now = now_secs();
        let conn = production_schema_db();
        let victim = victim_id();
        for i in 0..100u32 {
            let pot = format!("{:064x}", 0x0000_1000u64 + i as u64);
            insert_pot(&conn, &pot, 0, 1_000 + i as i64, true);
            insert_potparty_latched(
                &conn,
                &victim,
                &pot,
                0,
                &format!("txM{i:03}"),
                1_000 + i as i64,
                None,
                Some(true),
            );
        }
        let fresh = h64(0xfa);
        insert_potparty_latched(
            &conn,
            &victim,
            &fresh,
            0,
            "txFRESH",
            now - 30,
            None,
            Some(true),
        );
        // 50 ghosts, ALL of them OLDER than the honest marker and inside the
        // freshness window — i.e. the sustained rolling flood that is still
        // a residual for legacy rows, five times the quota.
        for i in 0..50u32 {
            insert_potparty_latched(
                &conn,
                &victim,
                &format!("{:064x}", 0xdead_beefu64 + i as u64),
                0,
                &format!("txGL{i:03}"),
                now - 600 + i as i64,
                None,
                Some(false),
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
        assert!(
            unique.contains(&fresh),
            "the fresh in-flight pot keeps its promoted slot under a 50-ghost flood"
        );
        assert_eq!(
            unique.len(),
            100,
            "and the flood displaces ZERO real pots (was: exactly quota=10)"
        );
    }

    /// An honest marker is never EVICTED within its own pot by forged
    /// siblings, whatever their stamps: `PARTYFOR_ROWS_PER_GROUP` junk rows
    /// stamped EARLIER used to take the whole group. Rank-first ordering
    /// puts the verified row at `rn = 1` regardless.
    #[test]
    fn a_latched_marker_is_never_evicted_within_its_pot_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        let pot = h64(0xaa);
        insert_pot(&conn, &pot, 0, 1_000, true);
        // Forged siblings first (EARLIER stamps AND physically first), then
        // the honest row — so no incidental order can produce a pass.
        for i in 0..(PARTYFOR_ROWS_PER_GROUP as u32 * 4) {
            insert_potparty_latched(
                &conn,
                &victim,
                &pot,
                0,
                &format!("txJ{i:03}"),
                10 + i as i64,
                None,
                Some(false),
            );
        }
        insert_potparty_latched(&conn, &victim, &pot, 0, "txHONEST", 9_999, None, Some(true));
        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "txid",
        );
        assert!(
            got.contains(&"txHONEST".to_string()),
            "the verified marker survives 4x the group cap of earlier forgeries: {got:?}"
        );
    }

    /// FAIL DIRECTION (the property that makes this safe to ship): the latch
    /// is a SORT KEY, never a filter. A lone marker that latches `false` —
    /// the shape a cross-language signer disagreement would produce — is
    /// still served, and its pot is still on the page. Ranking last in a
    /// window you are the only occupant of changes nothing.
    #[test]
    fn a_false_latch_never_removes_a_row_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        let pot = h64(0xab);
        insert_pot(&conn, &pot, 0, 1_000, true);
        insert_potparty_latched(&conn, &victim, &pot, 0, "txONLY", 1_000, None, Some(false));
        let unknown = h64(0xac);
        insert_potparty_latched(
            &conn,
            &victim,
            &unknown,
            0,
            "txUNK",
            1_001,
            None,
            Some(false),
        );
        let got = window_col(
            &conn,
            &potparty_list_for_identity_sql(),
            &victim,
            100,
            "potTxid",
        );
        assert!(
            got.contains(&pot),
            "an indexed pot is served whatever the latch says"
        );
        assert!(
            got.contains(&unknown),
            "and an UNKNOWN pot is DEMOTED, never dropped — the fail direction \
             the promotion tier has always had"
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
        assert_eq!(
            p2.len(),
            31,
            "page 2 = the remaining 30 junk + the honest row"
        );
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

    /// bsv-low#354/#356 — the POTPARTY `byPot` window, offset-paged through
    /// the SHIPPED SQL.
    ///
    /// This window is not identity-scoped: `potTxid`/`potVout` are payload
    /// CLAIMS, so a stranger files unlimited markers naming a victim's public
    /// pot from its own transactions, and it can file them BEFORE funding, so
    /// server-stamped `createdAt` puts them permanently at the head of the
    /// oldest-first order. The SQL has been `LIMIT ? OFFSET ?` since #291
    /// gate M2 and NO CALLER COULD REACH THE OFFSET — the mitigation existed
    /// as advice with no mechanism (epoch Rule 13: a surfaced limitation with
    /// no escape hatch is worse than an honest error).
    ///
    /// Both legs are asserted: page 0 really does bury both honest seats
    /// (pinned from the unsafe side, so the cell measures the attack), and
    /// paging really does reach them.
    #[test]
    fn potparty_by_pot_offset_pages_reach_buried_seat_markers_real_sqlite() {
        let conn = production_schema_db();
        let pot = h64(0xcd);
        // 130 rows filed before funding, each naming its OWN identity — which
        // is why a `sigValid` rank would not help here: every one of them can
        // carry a genuinely valid signature under the key it names.
        for i in 0..130u32 {
            insert_potparty(
                &conn,
                &format!("02{:064x}", i),
                &pot,
                0,
                &format!("txJUNK{i:03}"),
                100 + i as i64,
                None,
            );
        }
        // The two honest seat markers, published AT funding.
        insert_potparty(&conn, &victim_id(), &pot, 0, "txSEATA", 10_000, None);
        insert_potparty(
            &conn,
            &format!("03{}", "b2".repeat(32)),
            &pot,
            0,
            "txSEATB",
            10_001,
            None,
        );

        let sql = list_for_pot_sql(POTPARTY_SELECT);
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
        assert_eq!(p1.len(), 100, "page 0 is LIMIT-bounded…");
        assert!(
            !p1.iter().any(|t| t.starts_with("txSEAT")),
            "…and contains NEITHER seat: this is exactly the eviction \
             bsv-low#354 filed, and it is what a caller that cannot page sees \
             forever"
        );

        let p2 = page(100, 100);
        assert_eq!(p2.len(), 32, "30 remaining junk rows + BOTH seats");
        assert_eq!(
            p2[p2.len() - 2..],
            ["txSEATA".to_string(), "txSEATB".to_string()],
            "both honest markers are REACHABLE by paging — the cap bounds a \
             response, never the reachable set"
        );

        // Disjoint + covering: the total order is append-only, so pages
        // partition it and a concurrent insert cannot shift a row across a
        // boundary already fetched. (That stability is also why a MUTABLE
        // rank term — `sigValid`, which the #355 sweep rewrites — must not
        // lead this ORDER BY.)
        let mut all: Vec<String> = p1.iter().chain(p2.iter()).cloned().collect();
        let n = all.len();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), n, "pages are disjoint");
        assert_eq!(n, 132, "pages cover every admitted row");
    }

    /// The two `byX` windows partition on DIFFERENT KINDS of key, and that
    /// difference is the whole reason one needs a cursor and the other does
    /// not (see both SQL builders' docs).
    ///
    /// `ls_hopparty byHop` keys on `txid` — the marker's OWN container, a
    /// primary-key half — so its partition holds only outputs of one
    /// transaction and a stranger cannot add to it. `ls_potparty byPot` keys
    /// on `potTxid`, a CLAIM inside the payload, so anyone can file unlimited
    /// rows into it.
    #[test]
    fn the_by_hop_partition_keys_on_the_container_and_by_pot_on_a_claim() {
        let by_hop = hopparty_list_for_hop_sql();
        assert!(
            by_hop.contains("WHERE txid = ? AND hopVout = ?"),
            "byHop partitions on the marker's own container txid: {by_hop}"
        );
        assert!(
            !by_hop.contains("potTxid"),
            "…and never on a payload-claimed pot outpoint: {by_hop}"
        );
        // Uncrowdable ⇒ no cursor is owed. If this window ever gains a
        // payload-claimed partition key, that argument dies and it needs
        // `OFFSET` the way byPot now has it.
        assert!(
            !by_hop.contains("OFFSET"),
            "byHop is single-page ON PURPOSE: {by_hop}"
        );

        let by_pot = list_for_pot_sql(POTPARTY_SELECT);
        assert!(
            by_pot.contains("WHERE potTxid = ? AND potVout = ?"),
            "byPot partitions on a payload CLAIM: {by_pot}"
        );
        assert!(
            by_pot.contains("OFFSET"),
            "…which is why it must stay pageable: {by_pot}"
        );
    }

    /// THE BIND PIN (bsv-low#354, from this lane's own RED-verification).
    ///
    /// `fetch_all` needs a live `D1Database`, so nothing native can watch the
    /// storage impls bind anything — and a differently-spelled injection
    /// proved the consequence: replacing `list_for_pot`'s page-start bind
    /// with a computed `0` compiled and left every cell green, with #354's
    /// fix inoperative and the client's offset silently discarded (epoch
    /// Rule 22 / the #283 HIGH-2 class, caught before shipping this time).
    ///
    /// The builder is pure, so the value production sends is inspectable.
    /// Both callers go through it, so this cell covers BOTH — asserting on
    /// one surface while claiming two is the Rule 10 failure this avoids.
    #[test]
    fn the_by_pot_query_binds_the_page_start_for_both_callers() {
        for select in [POTPARTY_SELECT, POTREFUND_SELECT] {
            let q = by_pot_query(select, "aa", 7, 50, 500);
            assert_eq!(q.sql(), list_for_pot_sql(select), "the SHIPPED statement");
            assert_eq!(q.params().len(), 4, "potTxid, potVout, limit, offset");
            assert_eq!(q.params()[0], crate::d1::QVal::Text("aa".into()));
            assert_eq!(q.params()[1], crate::d1::QVal::Int(7));
            assert_eq!(q.params()[2], crate::d1::QVal::Int(50));
            assert_eq!(
                q.params()[3],
                crate::d1::QVal::Int(500),
                "the PAGE START is the caller's, not a constant — a computed \
                 0 here is #354 silently undone"
            );
            // …and it MOVES with the argument, so a bind that merely happens
            // to equal one probe value cannot satisfy this.
            assert_eq!(
                by_pot_query(select, "aa", 0, 1, 0).params()[3],
                crate::d1::QVal::Int(0)
            );
            assert_eq!(
                by_pot_query(select, "aa", 0, 1, 12_345).params()[3],
                crate::d1::QVal::Int(12_345)
            );
        }
    }

    /// The `byPot` rank decision, asserted on the CONSTRUCT: this window must
    /// NOT lead on `sigValid`, and the reason is not "we forgot" (epoch
    /// Rule 8 — write down which property a field carries).
    ///
    /// Two independent reasons, both load-bearing: the rank is FORGEABLE here
    /// because nothing in the predicate is scoped to a key the attacker does
    /// not hold, and `sigValid` is MUTABLE since the #355 re-latch sweep, so
    /// leading on it would move rows between pages mid-enumeration and break
    /// the offset paging that is the actual fix.
    #[test]
    fn the_by_pot_window_pages_and_never_ranks() {
        let sql = list_for_pot_sql(POTPARTY_SELECT);
        assert!(
            sql.contains("ORDER BY createdAt ASC, rowid ASC LIMIT ? OFFSET ?"),
            "append-only total order + a page cursor: {sql}"
        );
        assert_eq!(
            sql.matches(overlay_discovery::potparty::validity::SIG_VALID_COLUMN)
                .count(),
            0,
            "the verdict is neither ordered on nor filtered on in this \
             window — see `list_for_pot_sql`'s doc for why adding it would be \
             a bar that does not bar AND would break page stability: {sql}"
        );
        // The SHARED-SQL constraint the issue names, executed rather than
        // asserted in prose: the same builder serves potrefund, whose table
        // has no such column at all.
        let refund = list_for_pot_sql(POTREFUND_SELECT);
        assert!(refund.ends_with("ORDER BY createdAt ASC, rowid ASC LIMIT ? OFFSET ?"));
        assert!(!refund.contains("sigValid"));
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

    // ── bsv-low #315: hopparty D1 storage (tm_hopparty / ls_hopparty) ─────

    /// File a hopparty marker through the REAL writer
    /// (`hopparty_insert_query`) — the exact production SQL and bind list,
    /// replayed against real SQLite. `txid` is the CONTAINER (= the hop tx);
    /// the hop outpoint is `(txid, hop_vout)`.
    ///
    /// The marker fields are junk, so every row here latches
    /// `markerValid = 0` — which is the point for the WINDOW cells below: a
    /// refuted row must still be stored, served and ordered, never dropped.
    /// The latch's own behaviour is pinned separately, on the frozen golden.
    #[allow(clippy::too_many_arguments)]
    fn insert_hopparty(
        conn: &rusqlite::Connection,
        identity: &str,
        container_txid: &str,
        marker_vout: u32,
        hop_vout: u32,
        hop_sats: u64,
        on_chain_sats: Option<u64>,
        created_at: i64,
    ) {
        // The production write stamps `current_unix_seconds_i64()`; the
        // test passes a controlled stamp to the SAME parameter so ordering
        // assertions are deterministic (the SQL string is the shipped one).
        exec_query(
            conn,
            hopparty_insert_query(
                &HoppartyRecord {
                    identity: identity.to_string(),
                    opponent_identity: h64(0xbb),
                    game_id: h64(0x11),
                    hop_vout,
                    hop_sats,
                    seat_settle_pubkey: format!("03{}", "c4".repeat(32)),
                    seat_sig_hex: "3045seat".into(),
                    identity_sig_hex: "3045id".into(),
                    hop_lock_hex: on_chain_sats.map(|_| format!("76a914{}88ac", "d4".repeat(20))),
                    hop_sats_on_chain: on_chain_sats,
                    container_outputs: 2,
                    txid: container_txid.to_string(),
                    output_index: marker_vout,
                    created_at: 0, // ignored by the writer — the stamp wins
                },
                created_at,
            )
            .query(),
        );
    }

    /// bsv-low #362, BEHAVIOURALLY on real SQLite: in `ls_hopparty hopsFor`
    /// the latched verdict LEADS, and a refuted or legacy row is still
    /// SERVED behind it.
    ///
    /// Driven through the REAL writer for the verified row (the frozen
    /// client golden, so the `1` is earned by real signatures) and through
    /// the same writer for the junk rows (which earn their `0`). The legacy
    /// row is the golden, un-latched afterwards — the only honest way to
    /// produce one, since the writer always latches.
    #[test]
    fn the_hopparty_window_leads_on_the_latched_verdict_and_hides_nothing() {
        let conn = production_schema_db();
        let golden = golden_hopparty_record("txVERIFIED", true);
        let identity = golden.identity.clone();

        // A LEGACY row, stamped OLDEST so it leads on every key but the
        // verdict — and a fresh unknown hop, so it also wins the tier.
        let mut legacy = golden_hopparty_record("txLEGACY", true);
        legacy.hop_vout += 1;
        exec_query(&conn, hopparty_insert_query(&legacy, 100).query());
        conn.execute(
            "UPDATE hopparty_records SET markerValid = NULL WHERE txid = 'txLEGACY'",
            [],
        )
        .unwrap();

        // Three REFUTED rows, older than the verified one.
        for i in 0..3u32 {
            insert_hopparty(
                &conn,
                &identity,
                &format!("txJUNK{i}"),
                1,
                i,
                80_800,
                Some(80_800),
                200 + i as i64,
            );
        }

        // …and the verified one, NEWEST.
        exec_query(&conn, hopparty_insert_query(&golden, 9_000).query());

        // The verdict is an ORDERING HINT here and is deliberately NOT on
        // the `ls_hopparty` wire: this window's callers re-verify the
        // signatures they are handed, exactly as before, so adding a
        // server-asserted label would be a new claim nobody asked for. The
        // ORDER is the observable, and it is what this cell measures.
        let sql = hopparty_list_for_identity_sql();
        let mut stmt = conn.prepare(&sql).unwrap();
        let rows: Vec<String> = stmt
            .query_map(rusqlite::params![identity, 100u32, 10u32, 400u32], |r| {
                r.get("txid")
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows.len(), 5, "EVERY row is served — never a WHERE");
        assert_eq!(
            rows[0], "txVERIFIED",
            "the verified row leads despite being the NEWEST — the verdict \
             is genuinely the leading key: {rows:?}"
        );
        assert_eq!(
            rows[1], "txLEGACY",
            "…then the legacy tier, above the refuted rows: {rows:?}"
        );
        assert_eq!(
            rows[2..].iter().filter(|t| t.starts_with("txJUNK")).count(),
            3,
            "…and every refuted row is LAST, and PRESENT: {rows:?}"
        );
    }

    /// The SHIPPED hopparty insert + both list SQLs on the production
    /// schema: outpoint-keyed replay is a no-op, `byHop` is oldest-first,
    /// `hopsFor` serves a bounded per-outpoint SUPERSET (never a one-row
    /// collapse — verification happens at READ in `/hops-view`), and the
    /// CONTAINER's decoded facts survive the round-trip.
    #[test]
    fn hopparty_store_and_windows_real_sqlite() {
        let conn = production_schema_db();
        let victim = victim_id();
        let hop = h64(0xaa); // the container = the hop tx
                             // The hop outpoint is indexed via tm_lowfund (pot_records).
        insert_pot(&conn, &hop, 0, 1_000, false);
        // The honest marker (output 1 of the hop tx), then a same-outpoint
        // replay — ignored on the PK.
        insert_hopparty(&conn, &victim, &hop, 1, 0, 80_800, Some(80_800), 1_001);
        insert_hopparty(&conn, &victim, &hop, 1, 0, 99_999, Some(99_999), 9_999);
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM hopparty_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "same-outpoint replay is a no-op (INSERT OR IGNORE)");

        // Two more markers on the SAME container naming the same hop
        // outpoint at distinct marker vouts, stamped EARLIER — the superset
        // must keep the honest row (rn <= 4, oldest-first).
        for i in 0..2u32 {
            insert_hopparty(&conn, &victim, &hop, 2 + i, 0, 1, Some(1), 100 + i as i64);
        }
        let sql = hopparty_list_for_identity_sql();
        let mut stmt = conn.prepare(&sql).expect("shipped hopsFor SQL parses");
        let rows: Vec<(i64, i64, Option<i64>, i64)> = stmt
            .query_map(
                rusqlite::params![victim, 10u32, unknown_pot_quota(10) as u32, 40u32],
                |r| {
                    Ok((
                        r.get::<_, i64>("outputIndex")?,
                        r.get::<_, i64>("hopSats")?,
                        r.get::<_, Option<i64>>("hopSatsOnChain")?,
                        r.get::<_, i64>("containerOutputs")?,
                    ))
                },
            )
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows.len(),
            3,
            "superset: all three rows for the one outpoint"
        );
        assert!(
            rows.iter()
                .any(|(vout, sats, _, _)| *vout == 1 && *sats == 80_800),
            "two earlier-stamped forgeries must NOT evict the honest row \
             (verification-before-collapse: the reader decides)"
        );
        // The CONTAINER's decoded facts reach the outer select.
        assert!(rows
            .iter()
            .all(|(_, _, on_chain, outs)| on_chain.is_some() && *outs == 2));
        // Within the outpoint: oldest first (the total order the reader
        // labels through).
        assert_eq!(rows[0].0, 2, "the oldest-stamped marker leads");

        // byHop: oldest first through the shipped SQL, keyed on the hop
        // OUTPOINT (container txid + hopVout).
        let mut stmt = conn.prepare(&hopparty_list_for_hop_sql()).unwrap();
        let by_hop: Vec<i64> = stmt
            .query_map(rusqlite::params![hop, 0u32, 10u32], |r| {
                r.get::<_, i64>("outputIndex")
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(by_hop, vec![2, 3, 1]);
        // A different hopVout of the same container matches nobody.
        let mut stmt = conn.prepare(&hopparty_list_for_hop_sql()).unwrap();
        assert_eq!(
            stmt.query_map(rusqlite::params![hop, 9u32, 10u32], |r| r
                .get::<_, i64>("outputIndex"))
                .unwrap()
                .count(),
            0
        );
    }

    /// A marker whose CONTAINER lacks an output at hopVout stores NULL
    /// on-chain facts with `containerOutputs` making the absence PROVEN —
    /// the unknown-vs-refuted distinction the reader depends on.
    #[test]
    fn hopparty_absent_container_output_stores_proven_absence() {
        let conn = production_schema_db();
        let victim = victim_id();
        insert_hopparty(&conn, &victim, &h64(0xaa), 1, 7, 80_800, None, 1_000);
        let sql = hopparty_list_for_identity_sql();
        let mut stmt = conn.prepare(&sql).unwrap();
        let (lock, on_chain, outs): (Option<String>, Option<i64>, i64) = stmt
            .query_row(
                rusqlite::params![victim, 10u32, unknown_pot_quota(10) as u32, 40u32],
                |r| {
                    Ok((
                        r.get("hopLockHex")?,
                        r.get("hopSatsOnChain")?,
                        r.get("containerOutputs")?,
                    ))
                },
            )
            .unwrap();
        assert!(lock.is_none() && on_chain.is_none());
        assert_eq!(outs, 2, "absence is PROVEN by the container's output count");
    }

    /// The existence tier: markers naming hops the overlay never indexed
    /// (absent from `pot_records`) sort behind indexed hops beyond the
    /// fresh-unknown quota — and `limit` counts OUTPOINTS.
    #[test]
    fn hopparty_window_tiers_unknown_hops_and_counts_outpoints() {
        let conn = production_schema_db();
        let victim = victim_id();
        // Three REAL (indexed) hop txs, newest first expected.
        for i in 1u8..=3 {
            let hop = h64(0x10 + i);
            insert_pot(&conn, &hop, 0, 1_000 + i as i64, false);
            insert_hopparty(&conn, &victim, &hop, 1, 0, 500, Some(500), 1_000 + i as i64);
        }
        // Five GHOST containers (never indexed) with ANCIENT stamps
        // (outside the fresh-unknown window) — demoted behind every real
        // hop, but still served.
        for i in 0..5u8 {
            insert_hopparty(
                &conn,
                &victim,
                &h64(0xe0 + i),
                1,
                0,
                1,
                Some(1),
                10 + i as i64,
            );
        }

        let sql = hopparty_list_for_identity_sql();
        let mut stmt = conn.prepare(&sql).unwrap();
        let hops: Vec<String> = stmt
            .query_map(
                rusqlite::params![victim, 4u32, unknown_pot_quota(4) as u32, 16u32],
                |r| r.get::<_, String>("txid"),
            )
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let mut distinct: Vec<&String> = Vec::new();
        for h in &hops {
            if !distinct.contains(&h) {
                distinct.push(h);
            }
        }
        assert_eq!(distinct.len(), 4, "limit counts outpoints");
        assert_eq!(*distinct[0], h64(0x13), "newest indexed hop first");
        assert_eq!(*distinct[1], h64(0x12));
        assert_eq!(*distinct[2], h64(0x11));
        assert!(
            distinct[3].starts_with('e'),
            "the demoted tier fills the remainder; got {}",
            distinct[3]
        );
    }

    /// Structural pin for the hopparty window SQL — the same bars the
    /// potparty/potrefund windows are pinned to (partitioned on the full
    /// OUTPOINT, existence tier with a quota bind, explicit ORDER BY at
    /// every level, exactly four numbered binds).
    #[test]
    fn hopparty_identity_window_is_partitioned_tiered_and_ordered() {
        let sql = hopparty_list_for_identity_sql();
        assert!(
            sql.contains("ROW_NUMBER() OVER (PARTITION BY hp.txid, hp.hopVout"),
            "the window must partition on the hop OUTPOINT (container txid + vout)"
        );
        assert!(
            sql.contains("AS unknownPot") && sql.contains("potRank <= ?"),
            "existence tier with a reserved quota"
        );
        assert_eq!(sql.matches("ORDER BY").count(), 4, "ordered throughout");
        for n in 1..=4 {
            assert!(sql.contains(&format!("?{n}")), "bind ?{n} is used");
        }
        assert!(!sql.contains("?5"), "exactly four binds");
        assert!(
            sql.contains(&format!("rn <= {HOPSFOR_ROWS_PER_OUTPOINT}")),
            "bounded SUPERSET per outpoint, never rn = 1"
        );
        // ── bsv-low #362: the latched verdict LEADS, and filters nothing.
        assert!(
            sql.contains(&overlay_discovery::hopparty::validity::marker_rank_expr(
                "hp."
            )),
            "the rank CASE is the overlay's shared expression, verbatim"
        );
        assert_eq!(
            sql.matches("DENSE_RANK() OVER (ORDER BY outpointMarkerRank DESC, tier ASC, ")
                .count(),
            1,
            "the page-allocating rank leads on the latched verdict"
        );
        assert_eq!(
            sql.matches("ORDER BY outpointMarkerRank DESC, tier ASC")
                .count(),
            2,
            "the DENSE_RANK's ordering and the served ORDER BY both lead on it"
        );
        assert!(
            sql.contains(
                "MAX(markerRank) OVER (PARTITION BY txid, hopVout) \
                                  AS outpointMarkerRank"
            ),
            "the OUTPOINT aggregate is what keeps finalRank counting outpoints"
        );
        // NEVER A WHERE. Exactly the three this window has always had
        // (identity scope, per-outpoint superset, page rank). Asserted as a
        // COUNT rather than an absent-substring needle, which would be one
        // whitespace or one alias wide (epoch Rule 12a).
        assert_eq!(
            sql.matches("WHERE ").count(),
            3,
            "identity scope, rn <= superset, finalRank <= page — a fourth \
             WHERE (in ANY spelling) means somebody started HIDING rows"
        );
        // Every wire column reaches the OUTER select (decode-at-write means
        // the reader needs no second query).
        for col in [
            "opponentIdentity",
            "hopSats",
            "seatSettlePubkey",
            "seatSigHex",
            "identitySigHex",
            "hopLockHex",
            "hopSatsOnChain",
            "containerOutputs",
        ] {
            assert!(
                sql.matches(col).count() >= 4,
                "column {col} must survive to the outer select"
            );
        }
    }
}
