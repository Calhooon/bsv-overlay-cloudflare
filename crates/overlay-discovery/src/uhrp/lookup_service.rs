//! UHRP Lookup Service — indexes and queries UHRP advertisement records.
//!
//! When outputs are admitted to `tm_uhrp`, this service decodes the PushDrop
//! fields (5 data + signature) and stores key metadata via `UHRPStorage`.
//! Clients query `ls_uhrp` by `outpoint`, or by any of `uhrpUrl`,
//! `expiryTime`, `hostIdentityKey`, paged by `limit` and `offset`: the
//! reference's door at ts-stack@9262cac66 (bsv-low #819), see [`door_query`].
//!
//! No TypeScript reference exists — `@bsv/overlay-discovery-services` ships
//! SHIP + SLAP only. Structure mirrors
//! `super::super::ship::lookup_service::SHIPLookupService`.

use async_trait::async_trait;
use bsv_rs::primitives::ec::PublicKey;
use bsv_rs::primitives::encoding::Reader;
use bsv_rs::script::templates::PushDrop;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::storage::{UHRPQuery, UHRPStorage};

/// The fields the reference's `ls_uhrp` accepts (`UHRPLookupService.ts:82-89`).
pub const UHRP_QUERY_FIELDS: [&str; 6] = [
    "outpoint",
    "uhrpUrl",
    "expiryTime",
    "hostIdentityKey",
    "limit",
    "offset",
];

/// The reference's sentence for a query naming no filter
/// (`UHRPLookupService.ts:120-124`).
pub const UHRP_FILTER_REQUIRED: &str =
    "Lookup must specify either outpoint, or at least one of (uhrpUrl, expiryTime, hostIdentityKey)";

/// `Number.MAX_SAFE_INTEGER`.
const MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;

fn invalid(message: impl std::fmt::Display) -> String {
    format!("Invalid lookup query: {message}")
}

/// `readString` (`queryValidation.ts:46-59`): absent is `None`; any present
/// value that is not a string (a JSON `null` included) is refused.
fn read_string<'a>(
    query: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
    max_bytes: usize,
) -> Result<Option<&'a str>, String> {
    match query.get(field) {
        None => Ok(None),
        Some(serde_json::Value::String(s)) => {
            if s.is_empty() || s.len() > max_bytes {
                Err(invalid(format!(
                    "{field} must contain 1-{max_bytes} UTF-8 bytes"
                )))
            } else {
                Ok(Some(s.as_str()))
            }
        }
        Some(_) => Err(invalid(format!("{field} must be a string"))),
    }
}

/// `readInteger` (`queryValidation.ts:61-74`): `query[field] ?? default`, so a
/// JSON `null` takes the default; then `Number.isSafeInteger` and the bounds.
fn read_integer(
    query: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    default: i64,
    minimum: i64,
    maximum: i64,
) -> Result<i64, String> {
    let refused = || {
        invalid(format!(
            "{field} must be an integer from {minimum} to {maximum}"
        ))
    };
    let value = match query.get(field) {
        None | Some(serde_json::Value::Null) => default,
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                i
            } else if let Some(f) = n.as_f64() {
                // A JSON number JS reads as a safe integer (`2.0`, `-0`).
                if f.fract() != 0.0 || !f.is_finite() || f.abs() > MAX_SAFE_INTEGER as f64 {
                    return Err(refused());
                }
                f as i64
            } else {
                return Err(refused());
            }
        }
        Some(_) => return Err(refused()),
    };
    if !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&value)
        || value < minimum
        || value > maximum
    {
        return Err(refused());
    }
    Ok(value)
}

/// `requireOutpoint` (`queryValidation.ts:201-210`): `<64 hex>.<index>`, the
/// index with no leading zero, at most ten digits and at most `0xffffffff`;
/// the txid lowercased.
fn require_outpoint(value: &str) -> Result<(String, u32), String> {
    let refused = || invalid("outpoint must be a canonical outpoint");
    let (txid, index) = value.split_once('.').ok_or_else(refused)?;
    if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(refused());
    }
    let canonical_index = index == "0"
        || (!index.is_empty()
            && index.len() <= 10
            && index.bytes().all(|b| b.is_ascii_digit())
            && !index.starts_with('0'));
    if !canonical_index {
        return Err(refused());
    }
    let n: u64 = index.parse().map_err(|_| refused())?;
    let output_index =
        u32::try_from(n).map_err(|_| invalid("outpoint output index is out of range"))?;
    Ok((txid.to_ascii_lowercase(), output_index))
}

/// `StorageUtils.normalizeURL` (ts-stack@9262cac66
/// `packages/sdk/src/storage/StorageUtils.ts:9-13`).
fn normalize_uhrp_url(url: &str) -> &str {
    let bare = if url
        .get(..5)
        .is_some_and(|s| s.eq_ignore_ascii_case("uhrp:"))
    {
        &url[5..]
    } else {
        url
    };
    bare.strip_prefix("//").unwrap_or(bare)
}

/// `StorageUtils.isValidURL` (`StorageUtils.ts:51-75`): Base58Check with a
/// two-byte prefix `ce00` over 32 bytes.
fn is_valid_uhrp_url(url: &str) -> bool {
    bsv_rs::primitives::encoding::from_base58_check_with_prefix_length(normalize_uhrp_url(url), 2)
        .is_ok_and(|(prefix, data)| prefix == [0xce, 0x00] && data.len() == 32)
}

/// The reference's door on an `ls_uhrp` query, check for check and in its
/// order (`requireLookupQuery`, then `UHRPLookupService.lookup`): the query a
/// JSON object whose every key is in [`UHRP_QUERY_FIELDS`]; an `outpoint`
/// answers that one output (the other fields unread, as there); otherwise a
/// valid `uhrpUrl`, `hostIdentityKey` or `expiryTime` is required, `limit`
/// 1 to 200 (default 50, a value past the cap REFUSED, as there) and `offset`
/// 0 to 100,000. There is no find-all. Every refusal is the reference's
/// sentence (`Err`), before storage is read.
///
/// Kept from before the door, each a read of the pluggable storage surface,
/// never a widening: an advert past its expiry stays hidden (expiry 0 never
/// expires), and rows come newest first where the reference sorts by
/// outpoint; a valid query's answer is unchanged but for the page bound.
pub fn door_query(query: &serde_json::Value) -> Result<UHRPQuery, String> {
    let query = match query {
        serde_json::Value::Object(map) => map,
        _ => return Err(invalid("query must be an object")),
    };
    // `__proto__`, `constructor` and `prototype` are outside the list too.
    if let Some(key) = query
        .keys()
        .find(|k| !UHRP_QUERY_FIELDS.contains(&k.as_str()))
    {
        return Err(invalid(format!("unexpected field {key}")));
    }

    if let Some(outpoint) = read_string(query, "outpoint", 75)? {
        return Ok(UHRPQuery {
            outpoint: Some(require_outpoint(outpoint)?),
            limit: Some(1),
            ..Default::default()
        });
    }

    let uhrp_url = match read_string(query, "uhrpUrl", 128)? {
        Some(raw) if !is_valid_uhrp_url(raw) => return Err(invalid("uhrpUrl is invalid")),
        Some(raw) => Some(normalize_uhrp_url(raw).to_string()),
        None => None,
    };
    let host_identity_key = match read_string(query, "hostIdentityKey", 66)? {
        Some(key) => {
            let shaped = key.len() == 66
                && (key.starts_with("02") || key.starts_with("03"))
                && key.bytes().all(|b| b.is_ascii_hexdigit());
            if !shaped {
                return Err(invalid("hostIdentityKey must be a compressed public key"));
            }
            if PublicKey::from_hex(key).is_err() {
                return Err(invalid("hostIdentityKey must be a valid public key"));
            }
            Some(key.to_ascii_lowercase())
        }
        None => None,
    };
    let expiry_time = if query.contains_key("expiryTime") {
        Some(read_integer(query, "expiryTime", 0, 1, MAX_SAFE_INTEGER)?)
    } else {
        None
    };
    if uhrp_url.is_none() && expiry_time.is_none() && host_identity_key.is_none() {
        return Err(UHRP_FILTER_REQUIRED.to_string());
    }
    let limit = read_integer(query, "limit", 50, 1, 200)?;
    let offset = read_integer(query, "offset", 0, 0, 100_000)?;
    Ok(UHRPQuery {
        uhrp_url,
        identity_key: host_identity_key,
        expiry_time,
        limit: Some(limit as u32),
        skip: (offset > 0).then_some(offset as u32),
        ..Default::default()
    })
}

/// UHRP Lookup Service — indexes UHRP advertisements and answers queries.
pub struct UHRPLookupService {
    storage: Rc<dyn UHRPStorage>,
}

impl UHRPLookupService {
    /// Create a new UHRP lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn UHRPStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for UHRPLookupService {
    /// Nothing told to this service reads an off-chain value.
    fn reads_off_chain_values(&self, _topic: &str) -> bool {
        false
    }

    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        SpendNotificationMode::None
    }

    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        let (txid, output_index, topic, locking_script) = match payload {
            OutputAdmittedByTopic::LockingScript {
                txid,
                output_index,
                topic,
                locking_script,
                ..
            } => (txid, *output_index, topic, locking_script),
            _ => {
                return Err(LookupServiceError::Other(
                    "Expected locking-script mode".into(),
                ))
            }
        };

        // Only index tm_uhrp outputs.
        if !crate::name_matches(topic, "tm_uhrp") {
            return Ok(());
        }

        let script = bsv_rs::script::Script::from_binary(locking_script)
            .map_err(|e| LookupServiceError::Other(format!("Script parse error: {e}")))?;
        let pushdrop = PushDrop::decode(&script.into())
            .map_err(|e| LookupServiceError::Other(format!("PushDrop decode error: {e}")))?;

        // Strictly need 6 fields (5 data + signature). Anything else is
        // silently ignored — the topic manager admits only well-formed
        // UHRP advertisements, so in practice this branch is defensive.
        if pushdrop.fields.len() != 6 {
            debug!(
                "UHRP: unexpected field count on admitted output: {}",
                pushdrop.fields.len()
            );
            return Ok(());
        }

        // Defensive: re-validate the identity key shape. The topic manager
        // already checked this, but the lookup service must not trust
        // pre-validated state — indexing service code is a separate
        // trust boundary from admittance code.
        if pushdrop.fields[0].len() != 33 || PublicKey::from_bytes(&pushdrop.fields[0]).is_err() {
            return Ok(());
        }
        if pushdrop.fields[1].len() != 32 {
            return Ok(());
        }

        let identity_key = hex::encode(&pushdrop.fields[0]);

        let download_url = match std::str::from_utf8(&pushdrop.fields[2]) {
            Ok(s) => s.to_string(),
            Err(_) => return Ok(()),
        };

        let expiry_time: i64 = {
            let mut r = Reader::new(&pushdrop.fields[3]);
            match r.read_var_int() {
                Ok(v) => i64::try_from(v).unwrap_or(i64::MAX),
                Err(_) => return Ok(()),
            }
        };
        let content_length: i64 = {
            let mut r = Reader::new(&pushdrop.fields[4]);
            match r.read_var_int() {
                Ok(v) => i64::try_from(v).unwrap_or(i64::MAX),
                Err(_) => return Ok(()),
            }
        };

        // ts-stack@edf6e03 packages/sdk/src/storage/StorageUtils.ts:20-24
        // encodes SHA-256 with ce00; :50-57 refuses other prefixes.
        // The reference returns bare Base58Check, as do storage's goldens.
        let uhrp_url =
            bsv_rs::primitives::encoding::to_base58_check(&pushdrop.fields[1], &[0xce, 0x00]);

        // Dedup: nothing is stored if (txid, output_index) is already indexed.
        self.storage
            .store_record_unless_held(
                txid,
                output_index,
                &uhrp_url,
                &identity_key,
                &download_url,
                expiry_time,
                content_length,
            )
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
        let (txid, output_index, topic) = match payload {
            OutputSpent::None {
                txid,
                output_index,
                topic,
            } => (txid, *output_index, topic),
            _ => return Ok(()),
        };

        if !crate::name_matches(topic, "tm_uhrp") {
            return Ok(());
        }

        self.storage
            .delete_record(txid, output_index)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_evicted(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<(), LookupServiceError> {
        self.storage
            .delete_record(txid, output_index)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if !crate::name_matches(&question.service, "ls_uhrp") {
            return Err(LookupServiceError::Unsupported(format!(
                "Lookup service not supported! Expected ls_uhrp, got {}",
                question.service
            )));
        }

        // The reference's door (bsv-low #819; ts-stack@9262cac66
        // `UHRPLookupService.ts:81-136` over `shared/queryValidation.ts`):
        // every key on the allow-list, a filter required, the page bounded,
        // all before storage is read. A refusal is `InvalidQuery` carrying the
        // reference's sentence; the route answers it 400 by code.
        let query = door_query(&question.query).map_err(LookupServiceError::InvalidQuery)?;
        self.storage
            .find_record(&query)
            .await
            .map(LookupResult::OutputList)
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))
    }

    async fn get_documentation(&self) -> String {
        "UHRP Lookup Service — query for UHRP advertisements (hosting commitments) by outpoint, or by uhrpUrl, expiryTime and hostIdentityKey, paged by limit (1 to 200, default 50) and offset (0 to 100000).".to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "UHRP Lookup Service".to_string(),
            description: Some(
                "Provides lookup capabilities for UHRP advertisement tokens.".to_string(),
            ),
            ..Default::default()
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::expect_used)]
    use super::super::storage::MemoryUHRPStorage;
    use super::*;
    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::primitives::to_var_int;
    use bsv_rs::script::templates::PushDrop as PushDropTemplate;

    fn make_service() -> UHRPLookupService {
        UHRPLookupService::new(Rc::new(MemoryUHRPStorage::new()))
    }

    fn make_service_with_storage() -> (UHRPLookupService, Rc<MemoryUHRPStorage>) {
        let storage = Rc::new(MemoryUHRPStorage::new());
        let svc = UHRPLookupService::new(storage.clone());
        (svc, storage)
    }

    /// Build a UHRP PushDrop locking script (binary bytes) suitable for
    /// `OutputAdmittedByTopic::LockingScript`. Signature is a stub since
    /// the lookup service does NOT re-verify (that's the topic manager's job).
    fn make_uhrp_locking_script(
        identity_pubkey: &[u8],
        hash32: &[u8],
        url: &str,
        expiry: u64,
        length: u64,
    ) -> Vec<u8> {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            identity_pubkey.to_vec(),
            hash32.to_vec(),
            url.as_bytes().to_vec(),
            to_var_int(expiry),
            to_var_int(length),
            vec![0x30, 0x04, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01], // mock sig
        ];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        pushdrop.lock().to_binary()
    }

    fn sample_identity() -> Vec<u8> {
        PublicKey::from_private_key(&PrivateKey::random())
            .to_compressed()
            .to_vec()
    }

    // ---- Trait surface --------------------------------------------------

    #[tokio::test]
    async fn admission_mode_is_locking_script() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
    }

    #[tokio::test]
    async fn spend_notification_mode_is_none() {
        let svc = make_service();
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
    }

    #[tokio::test]
    async fn metadata_correct() {
        let svc = make_service();
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "UHRP Lookup Service");
    }

    #[tokio::test]
    async fn documentation_mentions_uhrp() {
        let svc = make_service();
        let docs = svc.get_documentation().await;
        assert!(docs.contains("UHRP"));
    }

    // ---- admission ------------------------------------------------------

    #[tokio::test]
    async fn admits_and_stores_record() {
        let (svc, storage) = make_service_with_storage();
        let ident = sample_identity();
        let hash = [0x42u8; 32];
        let script =
            make_uhrp_locking_script(&ident, &hash, "https://a.example", 1_900_000_000, 1024);
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc123".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        let records = storage.find_all_records().await.unwrap();
        assert_eq!(records[0].identity_key, hex::encode(&ident));
        // ts-stack@edf6e03 packages/sdk/src/storage/StorageUtils.ts:20-24.
        let expected_uhrp_url = bsv_rs::primitives::encoding::to_base58_check(&hash, &[0xce, 0x00]);
        assert_eq!(records[0].uhrp_url, expected_uhrp_url);
        assert_eq!(records[0].download_url, "https://a.example");
        assert_eq!(records[0].expiry_time, 1_900_000_000);
        assert_eq!(records[0].content_length, 1024);
    }

    #[tokio::test]
    async fn storage_golden_urls_find_admitted_adverts() {
        let vectors = [
            include_str!("fixtures/hash/all_bytes_0_to_255.json"),
            include_str!("fixtures/hash/empty.json"),
            include_str!("fixtures/hash/hello_world_ascii.json"),
            include_str!("fixtures/hash/one_mib_plus_one_byte_repeating.json"),
            include_str!("fixtures/hash/ts_sdk_cross_vector.json"),
        ];
        let (svc, storage) = make_service_with_storage();
        let ident = sample_identity();
        for (i, vector) in vectors.iter().enumerate() {
            let golden: serde_json::Value = serde_json::from_str(vector).unwrap();
            let hash = hex::decode(golden["hash_hex"].as_str().unwrap()).unwrap();
            let url = golden["uhrp_url"].as_str().unwrap();
            let txid = format!("golden-{i}");
            svc.output_admitted_by_topic(&OutputAdmittedByTopic::LockingScript {
                txid: txid.clone(),
                output_index: 0,
                topic: "tm_uhrp".into(),
                satoshis: 1,
                locking_script: make_uhrp_locking_script(
                    &ident,
                    &hash,
                    "https://a.example",
                    1_900_000_000,
                    1024,
                ),
                off_chain_values: None,
            })
            .await
            .unwrap();
            for query_url in [
                url.to_string(),
                format!("uhrp://{url}"),
                format!("UHRP:{url}"),
            ] {
                let result = svc
                    .lookup(&LookupQuestion::new(
                        "ls_uhrp",
                        serde_json::json!({"uhrpUrl": query_url}),
                    ))
                    .await
                    .unwrap();
                let LookupResult::OutputList(outputs) = result else {
                    panic!("expected output list")
                };
                assert_eq!(outputs.len(), 1, "{}", golden["name"]);
                assert_eq!(outputs[0].txid, txid);
                assert_eq!(outputs[0].output_index, 0);
            }
            let records = storage.find_all_records().await.unwrap();
            assert_eq!(
                records.iter().find(|r| r.txid == txid).unwrap().uhrp_url,
                url
            );
        }
    }

    #[tokio::test]
    async fn ignores_non_uhrp_topic() {
        let (svc, storage) = make_service_with_storage();
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: vec![],
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn duplicate_utxo_is_ignored() {
        let (svc, storage) = make_service_with_storage();
        let ident = sample_identity();
        let hash = [0x42u8; 32];
        let script =
            make_uhrp_locking_script(&ident, &hash, "https://a.example", 1_900_000_000, 1024);
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
            satoshis: 1,
            locking_script: script.clone(),
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn rejects_whole_tx_mode() {
        let svc = make_service();
        let payload = OutputAdmittedByTopic::WholeTx {
            atomic_beef: vec![],
            output_index: 0,
            topic: "tm_uhrp".into(),
            off_chain_values: None,
        };
        assert!(svc.output_admitted_by_topic(&payload).await.is_err());
    }

    // ---- spend / evict --------------------------------------------------

    #[tokio::test]
    async fn spend_deletes_record() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "uh", "ik", "u", 0, 1)
            .await
            .unwrap();
        let payload = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
        };
        svc.output_spent(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn evict_deletes_record() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "uh", "ik", "u", 0, 1)
            .await
            .unwrap();
        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    // ---- lookup queries -------------------------------------------------

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_other", serde_json::json!({}));
        assert!(svc.lookup(&q).await.is_err());
    }

    fn url_of(byte: u8) -> String {
        bsv_rs::primitives::encoding::to_base58_check(&[byte; 32], &[0xce, 0x00])
    }

    fn key_of(seed: u8) -> String {
        let mut k = [0u8; 32];
        k[31] = seed;
        hex::encode(
            PublicKey::from_private_key(&PrivateKey::from_bytes(&k).unwrap()).to_compressed(),
        )
    }

    async fn outputs(svc: &UHRPLookupService, query: serde_json::Value) -> Vec<String> {
        svc.lookup(&LookupQuestion::new("ls_uhrp", query))
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList")
            .into_iter()
            .map(|o| format!("{}.{}", o.txid, o.output_index))
            .collect()
    }

    async fn refusal(svc: &UHRPLookupService, query: serde_json::Value) -> String {
        match svc.lookup(&LookupQuestion::new("ls_uhrp", query)).await {
            Err(LookupServiceError::InvalidQuery(m)) => m,
            other => panic!("expected an InvalidQuery refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lookup_find_all_string_is_refused() {
        // bsv-low #819: the reference has no find-all; the string is not an object.
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, &url_of(1), &key_of(1), "u", 0, 1)
            .await
            .unwrap();
        assert_eq!(
            refusal(&svc, serde_json::json!("findAll")).await,
            "Invalid lookup query: query must be an object"
        );
        assert_eq!(
            refusal(&svc, serde_json::json!({"findAll": true})).await,
            "Invalid lookup query: unexpected field findAll"
        );
    }

    #[tokio::test]
    async fn lookup_by_uhrp_url() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, &url_of(1), &key_of(1), "u", 0, 1)
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, &url_of(2), &key_of(2), "u", 0, 1)
            .await
            .unwrap();
        let got = outputs(&svc, serde_json::json!({"uhrpUrl": url_of(1)})).await;
        assert_eq!(got, ["tx1.0"]);
    }

    #[tokio::test]
    async fn lookup_by_identity_key() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, &url_of(1), &key_of(1), "u", 0, 1)
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, &url_of(2), &key_of(2), "u", 0, 1)
            .await
            .unwrap();
        // The reference lowercases the key it filters by.
        let upper = key_of(2).to_ascii_uppercase();
        let got = outputs(&svc, serde_json::json!({"hostIdentityKey": upper})).await;
        assert_eq!(got, ["tx2.0"]);
    }

    #[tokio::test]
    async fn lookup_pages_by_limit_and_offset() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..7 {
            storage
                .store_record(&format!("tx{i}"), 0, &url_of(1), &key_of(1), "d", 0, 1)
                .await
                .unwrap();
        }
        let got = outputs(
            &svc,
            serde_json::json!({"hostIdentityKey": key_of(1), "limit": 3, "offset": 2}),
        )
        .await;
        assert_eq!(got.len(), 3);
        // `skip` was the engine's own name; it is off the reference's list.
        assert_eq!(
            refusal(
                &svc,
                serde_json::json!({"hostIdentityKey": key_of(1), "skip": 2})
            )
            .await,
            "Invalid lookup query: unexpected field skip"
        );
    }

    #[tokio::test]
    async fn lookup_limit_bounds_are_the_references() {
        let svc = make_service();
        for limit in [
            serde_json::json!(0),
            serde_json::json!(201),
            serde_json::json!(1.5),
        ] {
            assert_eq!(
                refusal(
                    &svc,
                    serde_json::json!({"uhrpUrl": url_of(1), "limit": limit})
                )
                .await,
                "Invalid lookup query: limit must be an integer from 1 to 200"
            );
        }
        assert_eq!(
            refusal(
                &svc,
                serde_json::json!({"uhrpUrl": url_of(1), "offset": 100_001})
            )
            .await,
            "Invalid lookup query: offset must be an integer from 0 to 100000"
        );
        // `null` takes the default (`query[field] ?? default`), as there.
        let q = door_query(&serde_json::json!({"uhrpUrl": url_of(1), "limit": null})).unwrap();
        assert_eq!(q.limit, Some(50));
        let q = door_query(&serde_json::json!({"uhrpUrl": url_of(1), "limit": 200.0})).unwrap();
        assert_eq!(q.limit, Some(200));
    }

    #[tokio::test]
    async fn lookup_default_limit_is_fifty() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..60 {
            storage
                .store_record(&format!("tx{i}"), 0, &url_of(1), &key_of(1), "d", 0, 1)
                .await
                .unwrap();
        }
        let got = outputs(&svc, serde_json::json!({"hostIdentityKey": key_of(1)})).await;
        assert_eq!(got.len(), 50);
    }

    #[tokio::test]
    async fn lookup_by_outpoint_answers_that_output() {
        let (svc, storage) = make_service_with_storage();
        let txid = "ab".repeat(32);
        storage
            .store_record(&txid, 1, &url_of(1), &key_of(1), "u", 0, 1)
            .await
            .unwrap();
        storage
            .store_record(&txid, 2, &url_of(1), &key_of(1), "u", 0, 1)
            .await
            .unwrap();
        let got = outputs(
            &svc,
            serde_json::json!({"outpoint": format!("{}.1", txid.to_ascii_uppercase())}),
        )
        .await;
        assert_eq!(got, [format!("{txid}.1")]);
        assert!(
            outputs(&svc, serde_json::json!({"outpoint": format!("{txid}.3")}))
                .await
                .is_empty()
        );
        for bad in [format!("{txid}.01"), format!("{txid}.1.x"), "ab.1".into()] {
            assert_eq!(
                refusal(&svc, serde_json::json!({"outpoint": bad})).await,
                "Invalid lookup query: outpoint must be a canonical outpoint"
            );
        }
        assert_eq!(
            refusal(
                &svc,
                serde_json::json!({"outpoint": format!("{txid}.4294967296")})
            )
            .await,
            "Invalid lookup query: outpoint output index is out of range"
        );
    }

    #[tokio::test]
    async fn lookup_by_expiry_time_is_an_exact_filter() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, &url_of(1), &key_of(1), "u", 4_000_000_000, 1)
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, &url_of(2), &key_of(1), "u", 4_000_000_001, 1)
            .await
            .unwrap();
        let got = outputs(&svc, serde_json::json!({"expiryTime": 4_000_000_000i64})).await;
        assert_eq!(got, ["tx1.0"]);
        assert_eq!(
            refusal(&svc, serde_json::json!({"expiryTime": 0})).await,
            format!(
                "Invalid lookup query: expiryTime must be an integer from 1 to {}",
                (1i64 << 53) - 1
            )
        );
    }

    #[tokio::test]
    async fn lookup_invalid_values_are_refused_not_empty_filters() {
        let svc = make_service();
        assert_eq!(
            refusal(&svc, serde_json::json!({"uhrpUrl": "not-uhrp"})).await,
            "Invalid lookup query: uhrpUrl is invalid"
        );
        assert_eq!(
            refusal(&svc, serde_json::json!({"hostIdentityKey": "02deadbeef"})).await,
            "Invalid lookup query: hostIdentityKey must be a compressed public key"
        );
        assert_eq!(
            refusal(
                &svc,
                serde_json::json!({"hostIdentityKey": format!("02{}", "11".repeat(32))})
            )
            .await,
            "Invalid lookup query: hostIdentityKey must be a valid public key"
        );
        assert_eq!(
            refusal(&svc, serde_json::json!({"uhrpUrl": 7})).await,
            "Invalid lookup query: uhrpUrl must be a string"
        );
    }

    // ---- expired-record filter (task #33), the storage's read ------------
    //
    // Since bsv-low #819 `includeExpired` and `nowUnixSeconds` are off the
    // wire (the reference's list); the hide-past-expiry read stays the
    // storage's, pinned here on the question the door builds.

    /// Seed helper: load two records, one past-expired, one still valid,
    /// at `now = 1_700_000_000`.
    async fn seed_mixed_expiry(storage: &MemoryUHRPStorage) {
        storage
            .store_record("tx_past", 0, &url_of(1), &key_of(1), "u", 1_699_999_999, 1)
            .await
            .unwrap();
        storage
            .store_record("tx_live", 0, &url_of(1), &key_of(1), "u", 1_700_000_100, 1)
            .await
            .unwrap();
    }

    fn at(query: serde_json::Value, now: i64, include_expired: bool) -> UHRPQuery {
        let mut q = door_query(&query).unwrap();
        q.now_unix_seconds = Some(now);
        q.include_expired = include_expired.then_some(true);
        q
    }

    async fn txids(storage: &MemoryUHRPStorage, q: &UHRPQuery) -> Vec<String> {
        storage
            .find_record(q)
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.txid)
            .collect()
    }

    #[tokio::test]
    async fn storage_default_hides_past_expiry_records() {
        let storage = MemoryUHRPStorage::new();
        seed_mixed_expiry(&storage).await;
        let q = at(
            serde_json::json!({"uhrpUrl": url_of(1)}),
            1_700_000_000,
            false,
        );
        assert_eq!(txids(&storage, &q).await, ["tx_live"]);
    }

    #[tokio::test]
    async fn storage_include_expired_returns_all() {
        let storage = MemoryUHRPStorage::new();
        seed_mixed_expiry(&storage).await;
        let q = at(
            serde_json::json!({"uhrpUrl": url_of(1)}),
            1_700_000_000,
            true,
        );
        assert_eq!(txids(&storage, &q).await.len(), 2);
    }

    #[tokio::test]
    async fn storage_zero_expiry_is_never_filtered() {
        let storage = MemoryUHRPStorage::new();
        storage
            .store_record("tx_forever", 0, &url_of(1), &key_of(1), "u", 0, 1)
            .await
            .unwrap();
        let q = at(
            serde_json::json!({"uhrpUrl": url_of(1)}),
            4_000_000_000,
            false,
        );
        assert_eq!(txids(&storage, &q).await, ["tx_forever"]);
    }

    #[tokio::test]
    async fn storage_identity_key_respects_expiry_filter() {
        let storage = MemoryUHRPStorage::new();
        storage
            .store_record(
                "tx_a_past",
                0,
                &url_of(1),
                &key_of(1),
                "u",
                1_699_999_999,
                1,
            )
            .await
            .unwrap();
        storage
            .store_record(
                "tx_a_live",
                0,
                &url_of(1),
                &key_of(1),
                "u",
                1_700_000_100,
                1,
            )
            .await
            .unwrap();
        storage
            .store_record(
                "tx_b_live",
                0,
                &url_of(1),
                &key_of(2),
                "u",
                1_700_000_100,
                1,
            )
            .await
            .unwrap();
        let q = at(
            serde_json::json!({"hostIdentityKey": key_of(1)}),
            1_700_000_000,
            false,
        );
        assert_eq!(txids(&storage, &q).await, ["tx_a_live"]);
    }

    // ---- Full lifecycle -------------------------------------------------

    #[tokio::test]
    async fn lifecycle_admit_query_spend_query() {
        let (svc, storage) = make_service_with_storage();
        let ident = sample_identity();
        let ident_hex = hex::encode(&ident);
        let hash = [0xAAu8; 32];
        let script =
            make_uhrp_locking_script(&ident, &hash, "https://n.example", 1_900_000_000, 4096);

        let admit = OutputAdmittedByTopic::LockingScript {
            txid: "live_tx".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&admit).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        let q = LookupQuestion::new("ls_uhrp", serde_json::json!({"hostIdentityKey": ident_hex}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "live_tx");

        let spend = OutputSpent::None {
            txid: "live_tx".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
        };
        svc.output_spent(&spend).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }
}
