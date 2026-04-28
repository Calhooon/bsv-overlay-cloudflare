//! Storage trait + in-memory backend for `ls_sonicstar` records.
//!
//! Mirrors the shape of `crates/overlay-discovery/src/dm_delegation/storage.rs`
//! but with the SonicStar Song Source Protocol (`sssp`) field set. The
//! concrete D1-backed implementation lives in `overlay-cloudflare`.
//!
//! ## Sort order
//!
//! Every paginated reader returns rows in `admitted_at` descending order, to
//! match Ruth's TS reference (`sonicstarLookup.ts:154`,
//! `.sort({ admittedAt: -1 })`).
//!
//! ## Search semantics
//!
//! - `find_by_artist_name`: case insensitive substring.
//! - `find_by_genre`: exact equality (case sensitive, matches Mongo).
//! - `find_by_search_text`: case insensitive substring across three fields
//!   only — `song_title`, `artist_name`, `album`. The TS docstring claims
//!   four; the TS code does three. We mirror the code.
//!
//! ## Multi-criterion filters
//!
//! [`SonicstarStorage::find_records`] composes any subset of the filter
//! fields via AND. This is the path used by `ls_sonicstar` lookups so that
//! TS-parity queries like `{artistName: "...", genre: "..."}` work
//! end-to-end. The single-criterion `find_by_*` methods remain on the
//! trait as ergonomic shortcuts; they delegate to `find_records` in the
//! in-memory backend.

use async_trait::async_trait;
use overlay_engine::types::UTXOReference;
use serde::{Deserialize, Serialize};

/// A persisted sonicstar track record.
///
/// Field shape mirrors the TS `SonicStarRecord` (`sonicstarLookup.ts:72-77`)
/// extended with the four overlay-supplied fields. JSON renames produce
/// the TS camelCase form so round trips are debuggable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SonicstarRecord {
    // ---- 13 metadata fields (decoded from the OP_RETURN JSON envelope) ----
    #[serde(rename = "songTitle")]
    pub song_title: String,
    #[serde(rename = "artistName")]
    pub artist_name: String,
    /// Always the empty string today. Ruth's TS reference hard codes this
    /// with a `TODO: extract from transaction context` comment; we mirror.
    #[serde(rename = "artistIdentityKey")]
    pub artist_identity_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub duration: u64,
    #[serde(rename = "songFileURL")]
    pub song_file_url: String,
    #[serde(rename = "artFileURL", skip_serializing_if = "Option::is_none")]
    pub art_file_url: Option<String>,
    #[serde(rename = "previewURL", skip_serializing_if = "Option::is_none")]
    pub preview_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genre: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub album: Option<String>,
    #[serde(rename = "releaseDate", skip_serializing_if = "Option::is_none")]
    pub release_date: Option<String>,
    #[serde(rename = "pricePerPlay")]
    pub price_per_play: u64,
    #[serde(rename = "royaltyRate")]
    pub royalty_rate: u8,

    // ---- 4 overlay-supplied fields ----
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    pub satoshis: u64,
    /// Unix milliseconds when the lookup service admitted this record.
    /// Stored as `i64` to match SQLite `INTEGER` and JS `Date.getTime()`.
    #[serde(rename = "admittedAt")]
    pub admitted_at: i64,
}

#[async_trait(?Send)]
pub trait SonicstarStorage {
    /// Check if a record with the same `(txid, output_index)` already exists.
    async fn has_duplicate_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<bool, SonicstarStorageError>;

    /// Insert (or upsert) a sonicstar record. Implementations MUST treat
    /// `(txid, output_index)` as unique and overwrite an existing row to
    /// match the TS `updateOne(..., { upsert: true })` behavior.
    async fn store_record(
        &self,
        record: &SonicstarRecord,
    ) -> Result<(), SonicstarStorageError>;

    /// Delete a record by outpoint (called when the UTXO is spent or evicted).
    async fn delete_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<(), SonicstarStorageError>;

    /// Find by exact outpoint. Returns at most one entry.
    async fn find_by_outpoint(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    /// Find every record sharing this `txid` (across multiple outputs).
    async fn find_by_txid(&self, txid: &str)
        -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    /// Find by case insensitive substring of `artist_name`.
    async fn find_by_artist_name(
        &self,
        name_substr: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    /// Find by exact `genre` (case sensitive equality).
    async fn find_by_genre(
        &self,
        genre: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    /// Free text: case insensitive substring across song_title, artist_name,
    /// album. Three fields only.
    async fn find_by_search_text(
        &self,
        q: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    /// Enumerate all records, newest first.
    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    /// AND-compose any subset of the filter fields and return matching
    /// outpoints, newest first. An empty filter is equivalent to
    /// [`Self::find_all`].
    ///
    /// This is the primary query path used by `ls_sonicstar` so that
    /// multi-criterion TS Mongo queries (`{artistName, genre}`,
    /// `{txid, searchText}`, ...) work end-to-end. D1 backends can push
    /// the filter to SQL; the in-memory backend filters in place.
    async fn find_records(
        &self,
        filter: &SonicstarFilter,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError>;

    /// Same filter semantics as [`Self::find_records`] but returns the
    /// full [`SonicstarRecord`] payload instead of just outpoints.
    /// Backs the `/sonicstar/records` route, which mirrors the rich
    /// `records[]` shape Ruth's TS reference returns alongside her
    /// `outpoints[]` in `/api/overlay-parity/lookup`.
    async fn find_records_full(
        &self,
        filter: &SonicstarFilter,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<SonicstarRecord>, SonicstarStorageError>;
}

/// Composable filter for [`SonicstarStorage::find_records`]. Any field
/// left as `None` is ignored; set fields combine via AND.
#[derive(Debug, Default, Clone)]
pub struct SonicstarFilter {
    /// Exact-match `txid`.
    pub txid: Option<String>,
    /// Case-insensitive substring against `artist_name`.
    pub artist_name_contains: Option<String>,
    /// Exact-match `genre` (case sensitive, matches Mongo equality).
    pub genre_eq: Option<String>,
    /// Case-insensitive substring across `song_title`, `artist_name`,
    /// `album`. Three fields only, matches the TS code (not the
    /// out-of-date TS docstring that claims four).
    pub search_text: Option<String>,
}

impl SonicstarFilter {
    /// `true` when every filter field is `None`. Lookup callers use this
    /// to short-circuit to a `findAll`-style enumeration.
    pub fn is_empty(&self) -> bool {
        self.txid.is_none()
            && self.artist_name_contains.is_none()
            && self.genre_eq.is_none()
            && self.search_text.is_none()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SonicstarStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (used by unit tests + non-CF deployments)
// ============================================================================

#[derive(Debug, Default)]
pub struct MemorySonicstarStorage {
    rows: std::sync::Mutex<Vec<SonicstarRecord>>,
}

impl MemorySonicstarStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.rows.lock().unwrap().len()
    }
}

/// Sort by `admitted_at` descending, then by `output_index` ascending as a
/// stable tiebreaker. A `Vec` clone is fine at the scale we run at; if the
/// in-memory backend ever grew past tens of thousands of rows we would
/// switch to a sorted index, but that is well beyond the test surface.
fn sort_desc_by_admitted_at(rows: &mut [SonicstarRecord]) {
    rows.sort_by(|a, b| {
        b.admitted_at
            .cmp(&a.admitted_at)
            .then_with(|| a.output_index.cmp(&b.output_index))
    });
}

fn page<T>(items: Vec<T>, limit: Option<u32>, skip: Option<u32>) -> Vec<T> {
    let skip = skip.unwrap_or(0) as usize;
    items
        .into_iter()
        .skip(skip)
        .take(limit.map_or(usize::MAX, |l| l as usize))
        .collect()
}

fn to_outpoint(r: &SonicstarRecord) -> UTXOReference {
    UTXOReference {
        txid: r.txid.clone(),
        output_index: r.output_index,
    }
}

/// AND-composed match against a [`SonicstarFilter`]. Mirrors Ruth's TS
/// Mongo filter object semantics: every set field must hold for the
/// record to be included.
fn matches_filter(record: &SonicstarRecord, filter: &SonicstarFilter) -> bool {
    if let Some(t) = &filter.txid {
        if record.txid != *t {
            return false;
        }
    }
    if let Some(needle) = &filter.artist_name_contains {
        if !record
            .artist_name
            .to_lowercase()
            .contains(&needle.to_lowercase())
        {
            return false;
        }
    }
    if let Some(g) = &filter.genre_eq {
        if record.genre.as_deref() != Some(g.as_str()) {
            return false;
        }
    }
    if let Some(q) = &filter.search_text {
        let needle = q.to_lowercase();
        let in_title = record.song_title.to_lowercase().contains(&needle);
        let in_artist = record.artist_name.to_lowercase().contains(&needle);
        let in_album = record
            .album
            .as_deref()
            .is_some_and(|a| a.to_lowercase().contains(&needle));
        if !(in_title || in_artist || in_album) {
            return false;
        }
    }
    true
}

#[async_trait(?Send)]
impl SonicstarStorage for MemorySonicstarStorage {
    async fn has_duplicate_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<bool, SonicstarStorageError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.txid == txid && r.output_index == output_index))
    }

    async fn store_record(
        &self,
        record: &SonicstarRecord,
    ) -> Result<(), SonicstarStorageError> {
        let mut rows = self.rows.lock().unwrap();
        // Upsert: drop any prior row at the same outpoint before push.
        rows.retain(|r| !(r.txid == record.txid && r.output_index == record.output_index));
        rows.push(record.clone());
        Ok(())
    }

    async fn delete_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<(), SonicstarStorageError> {
        self.rows
            .lock()
            .unwrap()
            .retain(|r| !(r.txid == txid && r.output_index == output_index));
        Ok(())
    }

    async fn find_by_outpoint(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError> {
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .iter()
            .filter(|r| r.txid == txid && r.output_index == output_index)
            .map(to_outpoint)
            .collect())
    }

    async fn find_by_txid(
        &self,
        txid: &str,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError> {
        // Note: find_by_txid is documented as unpaginated (one tx has at
        // most a handful of admissible outputs). Pass None/None to match.
        let filter = SonicstarFilter {
            txid: Some(txid.to_string()),
            ..Default::default()
        };
        self.find_records(&filter, None, None).await
    }

    async fn find_by_artist_name(
        &self,
        name_substr: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError> {
        let filter = SonicstarFilter {
            artist_name_contains: Some(name_substr.to_string()),
            ..Default::default()
        };
        self.find_records(&filter, limit, skip).await
    }

    async fn find_by_genre(
        &self,
        genre: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError> {
        let filter = SonicstarFilter {
            genre_eq: Some(genre.to_string()),
            ..Default::default()
        };
        self.find_records(&filter, limit, skip).await
    }

    async fn find_by_search_text(
        &self,
        q: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError> {
        let filter = SonicstarFilter {
            search_text: Some(q.to_string()),
            ..Default::default()
        };
        self.find_records(&filter, limit, skip).await
    }

    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError> {
        self.find_records(&SonicstarFilter::default(), limit, skip)
            .await
    }

    async fn find_records(
        &self,
        filter: &SonicstarFilter,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, SonicstarStorageError> {
        let mut hits: Vec<SonicstarRecord> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|r| matches_filter(r, filter))
            .cloned()
            .collect();
        sort_desc_by_admitted_at(&mut hits);
        Ok(page(hits.iter().map(to_outpoint).collect(), limit, skip))
    }

    async fn find_records_full(
        &self,
        filter: &SonicstarFilter,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<SonicstarRecord>, SonicstarStorageError> {
        let mut hits: Vec<SonicstarRecord> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|r| matches_filter(r, filter))
            .cloned()
            .collect();
        sort_desc_by_admitted_at(&mut hits);
        Ok(page(hits, limit, skip))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(txid: &str, output_index: u32, admitted_at_ms: i64) -> SonicstarRecord {
        SonicstarRecord {
            song_title: "Default Title".into(),
            artist_name: "Default Artist".into(),
            artist_identity_key: String::new(),
            description: None,
            duration: 0,
            song_file_url: "uhrp://example".into(),
            art_file_url: None,
            preview_url: None,
            genre: None,
            album: None,
            release_date: None,
            price_per_play: 1000,
            royalty_rate: 75,
            txid: txid.into(),
            output_index,
            satoshis: 1,
            admitted_at: admitted_at_ms,
        }
    }

    fn record_with(
        txid: &str,
        admitted_at_ms: i64,
        artist: &str,
        title: &str,
        genre: Option<&str>,
        album: Option<&str>,
    ) -> SonicstarRecord {
        let mut r = make_record(txid, 0, admitted_at_ms);
        r.artist_name = artist.into();
        r.song_title = title.into();
        r.genre = genre.map(str::to_string);
        r.album = album.map(str::to_string);
        r
    }

    #[tokio::test]
    async fn store_and_find_by_outpoint() {
        let store = MemorySonicstarStorage::new();
        store.store_record(&make_record("tx1", 0, 100)).await.unwrap();
        store.store_record(&make_record("tx2", 0, 200)).await.unwrap();

        let hits = store.find_by_outpoint("tx1", 0).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].txid, "tx1");
        assert_eq!(hits[0].output_index, 0);

        let misses = store.find_by_outpoint("tx3", 0).await.unwrap();
        assert!(misses.is_empty());
    }

    #[tokio::test]
    async fn has_duplicate_record_uses_outpoint() {
        let store = MemorySonicstarStorage::new();
        assert!(!store.has_duplicate_record("tx1", 0).await.unwrap());

        store.store_record(&make_record("tx1", 0, 100)).await.unwrap();
        assert!(store.has_duplicate_record("tx1", 0).await.unwrap());
        assert!(!store.has_duplicate_record("tx1", 1).await.unwrap());
        assert!(!store.has_duplicate_record("tx2", 0).await.unwrap());
    }

    #[tokio::test]
    async fn store_record_upserts_existing_outpoint() {
        let store = MemorySonicstarStorage::new();
        store.store_record(&make_record("tx1", 0, 100)).await.unwrap();

        let mut updated = make_record("tx1", 0, 999);
        updated.song_title = "Updated".into();
        store.store_record(&updated).await.unwrap();

        assert_eq!(store.record_count(), 1, "upsert must not create a duplicate");
        // The single remaining row should be the updated one.
        let rows = store.find_all(None, None).await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn delete_record_removes_only_matching_outpoint() {
        let store = MemorySonicstarStorage::new();
        store.store_record(&make_record("tx1", 0, 100)).await.unwrap();
        store.store_record(&make_record("tx1", 1, 200)).await.unwrap();

        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 1);
        assert!(store.find_by_outpoint("tx1", 0).await.unwrap().is_empty());
        assert_eq!(store.find_by_outpoint("tx1", 1).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn find_by_txid_returns_all_outputs() {
        let store = MemorySonicstarStorage::new();
        store.store_record(&make_record("tx1", 0, 100)).await.unwrap();
        store.store_record(&make_record("tx1", 1, 200)).await.unwrap();
        store.store_record(&make_record("tx2", 0, 300)).await.unwrap();

        let hits = store.find_by_txid("tx1").await.unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|r| r.txid == "tx1"));
    }

    #[tokio::test]
    async fn find_by_artist_name_is_case_insensitive_substring() {
        let store = MemorySonicstarStorage::new();
        store
            .store_record(&record_with("tx1", 100, "Adele", "Hello", None, None))
            .await
            .unwrap();
        store
            .store_record(&record_with("tx2", 200, "ADELE Smith", "Skyfall", None, None))
            .await
            .unwrap();
        store
            .store_record(&record_with("tx3", 300, "Beatles", "Hey Jude", None, None))
            .await
            .unwrap();

        let hits = store
            .find_by_artist_name("adele", None, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 2);

        let hits = store
            .find_by_artist_name("BEATLES", None, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].txid, "tx3");
    }

    #[tokio::test]
    async fn find_by_genre_is_exact_case_sensitive() {
        let store = MemorySonicstarStorage::new();
        store
            .store_record(&record_with("tx1", 100, "a", "t1", Some("Pop"), None))
            .await
            .unwrap();
        store
            .store_record(&record_with("tx2", 200, "a", "t2", Some("pop"), None))
            .await
            .unwrap();
        store
            .store_record(&record_with("tx3", 300, "a", "t3", Some("Jazz"), None))
            .await
            .unwrap();

        let hits = store.find_by_genre("Pop", None, None).await.unwrap();
        assert_eq!(hits.len(), 1, "only exact-case Pop should match");
        assert_eq!(hits[0].txid, "tx1");
    }

    #[tokio::test]
    async fn find_by_search_text_covers_three_fields_only() {
        let store = MemorySonicstarStorage::new();
        // Match in song_title.
        store
            .store_record(&record_with("tx1", 100, "Adele", "Hello World", None, None))
            .await
            .unwrap();
        // Match in artist_name.
        store
            .store_record(&record_with("tx2", 200, "Hello Friend", "Skyfall", None, None))
            .await
            .unwrap();
        // Match in album.
        store
            .store_record(&record_with("tx3", 300, "X", "Y", None, Some("Hello Tour")))
            .await
            .unwrap();
        // Match should NOT come from genre or any other field.
        store
            .store_record(&record_with("tx4", 400, "X", "Y", Some("Hello Genre"), None))
            .await
            .unwrap();

        let hits = store
            .find_by_search_text("hello", None, None)
            .await
            .unwrap();
        let txids: Vec<&str> = hits.iter().map(|h| h.txid.as_str()).collect();
        assert_eq!(txids.len(), 3);
        assert!(txids.contains(&"tx1"));
        assert!(txids.contains(&"tx2"));
        assert!(txids.contains(&"tx3"));
        assert!(
            !txids.contains(&"tx4"),
            "searchText must not match against `genre`"
        );
    }

    #[tokio::test]
    async fn find_all_sorted_admitted_at_desc() {
        let store = MemorySonicstarStorage::new();
        // Insert out of order.
        store.store_record(&make_record("tx-mid", 0, 200)).await.unwrap();
        store.store_record(&make_record("tx-old", 0, 100)).await.unwrap();
        store.store_record(&make_record("tx-new", 0, 300)).await.unwrap();

        let hits = store.find_all(None, None).await.unwrap();
        let order: Vec<&str> = hits.iter().map(|h| h.txid.as_str()).collect();
        assert_eq!(order, vec!["tx-new", "tx-mid", "tx-old"]);
    }

    #[tokio::test]
    async fn pagination_clamps_via_skip_and_limit() {
        let store = MemorySonicstarStorage::new();
        for i in 0..7i64 {
            // Insert with strictly increasing admitted_at so sort order is deterministic.
            let ts = 1000 + i;
            let r = make_record(&format!("tx{i}"), 0, ts);
            store.store_record(&r).await.unwrap();
        }

        let page1 = store.find_all(Some(3), Some(0)).await.unwrap();
        let page2 = store.find_all(Some(3), Some(3)).await.unwrap();
        let page3 = store.find_all(Some(3), Some(6)).await.unwrap();

        assert_eq!(page1.len(), 3);
        assert_eq!(page2.len(), 3);
        assert_eq!(page3.len(), 1);

        // Newest first: tx6 (ts=1006) ... tx0 (ts=1000).
        assert_eq!(page1[0].txid, "tx6");
        assert_eq!(page1[2].txid, "tx4");
        assert_eq!(page2[0].txid, "tx3");
        assert_eq!(page3[0].txid, "tx0");
    }

    #[tokio::test]
    async fn empty_store_returns_empty_collections() {
        let store = MemorySonicstarStorage::new();
        assert!(store.find_all(None, None).await.unwrap().is_empty());
        assert!(store.find_by_txid("tx1").await.unwrap().is_empty());
        assert!(
            store
                .find_by_artist_name("any", None, None)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .find_by_genre("Pop", None, None)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .find_by_search_text("any", None, None)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn filter_is_empty_helper() {
        assert!(SonicstarFilter::default().is_empty());
        assert!(!SonicstarFilter {
            txid: Some("x".into()),
            ..Default::default()
        }
        .is_empty());
        assert!(!SonicstarFilter {
            search_text: Some("y".into()),
            ..Default::default()
        }
        .is_empty());
    }

    #[tokio::test]
    async fn find_records_empty_filter_returns_all() {
        let store = MemorySonicstarStorage::new();
        store.store_record(&make_record("tx1", 0, 100)).await.unwrap();
        store.store_record(&make_record("tx2", 0, 200)).await.unwrap();

        let hits = store
            .find_records(&SonicstarFilter::default(), None, None)
            .await
            .unwrap();
        assert_eq!(hits.len(), 2);
        // Newest first.
        assert_eq!(hits[0].txid, "tx2");
    }

    #[tokio::test]
    async fn find_records_combines_artist_and_genre_via_and() {
        let store = MemorySonicstarStorage::new();
        // Same artist, different genre.
        store
            .store_record(&record_with("tx1", 100, "Adele", "t1", Some("Pop"), None))
            .await
            .unwrap();
        store
            .store_record(&record_with("tx2", 200, "Adele", "t2", Some("Jazz"), None))
            .await
            .unwrap();
        // Different artist, matching genre.
        store
            .store_record(&record_with("tx3", 300, "Beatles", "t3", Some("Pop"), None))
            .await
            .unwrap();

        let filter = SonicstarFilter {
            artist_name_contains: Some("adele".into()),
            genre_eq: Some("Pop".into()),
            ..Default::default()
        };
        let hits = store.find_records(&filter, None, None).await.unwrap();
        assert_eq!(hits.len(), 1, "AND must reject non-Pop Adele and non-Adele Pop");
        assert_eq!(hits[0].txid, "tx1");
    }

    #[tokio::test]
    async fn find_records_combines_txid_and_search_text() {
        let store = MemorySonicstarStorage::new();
        // Same txid, different titles.
        let mut a = make_record("txA", 0, 100);
        a.song_title = "Hello World".into();
        store.store_record(&a).await.unwrap();
        let mut b = make_record("txA", 1, 200);
        b.song_title = "Goodbye Moon".into();
        store.store_record(&b).await.unwrap();
        // Different txid, matching search.
        let mut c = make_record("txB", 0, 300);
        c.song_title = "Hello Friend".into();
        store.store_record(&c).await.unwrap();

        let filter = SonicstarFilter {
            txid: Some("txA".into()),
            search_text: Some("hello".into()),
            ..Default::default()
        };
        let hits = store.find_records(&filter, None, None).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].txid, "txA");
        assert_eq!(hits[0].output_index, 0);
    }

    #[tokio::test]
    async fn find_records_no_match_returns_empty() {
        let store = MemorySonicstarStorage::new();
        store
            .store_record(&record_with("tx1", 100, "Adele", "Hello", Some("Pop"), None))
            .await
            .unwrap();

        let filter = SonicstarFilter {
            artist_name_contains: Some("missing-artist".into()),
            ..Default::default()
        };
        assert!(store.find_records(&filter, None, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn find_records_full_returns_full_records_with_filter_and_paging() {
        let store = MemorySonicstarStorage::new();
        store
            .store_record(&record_with("tx1", 100, "Adele", "Hello", Some("Pop"), None))
            .await
            .unwrap();
        store
            .store_record(&record_with("tx2", 200, "Beatles", "Hey Jude", Some("Rock"), None))
            .await
            .unwrap();
        store
            .store_record(&record_with("tx3", 300, "Adele", "Skyfall", Some("Pop"), None))
            .await
            .unwrap();

        let filter = SonicstarFilter {
            artist_name_contains: Some("adele".into()),
            ..Default::default()
        };
        let hits = store
            .find_records_full(&filter, Some(10), Some(0))
            .await
            .unwrap();
        assert_eq!(hits.len(), 2);
        // Newest first by admitted_at DESC.
        assert_eq!(hits[0].txid, "tx3");
        assert_eq!(hits[1].txid, "tx1");
        // Records carry the full payload, not just outpoints.
        assert_eq!(hits[0].artist_name, "Adele");
        assert_eq!(hits[0].song_title, "Skyfall");
    }

    #[tokio::test]
    async fn find_records_paginates_after_filtering() {
        let store = MemorySonicstarStorage::new();
        // 5 records, all with artist "Adele", strictly increasing admittedAt.
        for i in 0..5i64 {
            let r = record_with(&format!("tx{i}"), 1000 + i, "Adele", "song", None, None);
            store.store_record(&r).await.unwrap();
        }
        // 1 record with a different artist; must be excluded by the filter
        // before pagination so page sizes still match.
        store
            .store_record(&record_with("noise", 1003, "Beatles", "song", None, None))
            .await
            .unwrap();

        let filter = SonicstarFilter {
            artist_name_contains: Some("adele".into()),
            ..Default::default()
        };
        let page1 = store.find_records(&filter, Some(2), Some(0)).await.unwrap();
        let page2 = store.find_records(&filter, Some(2), Some(2)).await.unwrap();
        let page3 = store.find_records(&filter, Some(2), Some(4)).await.unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page2.len(), 2);
        assert_eq!(page3.len(), 1);
        // Newest first across the Adele subset.
        assert_eq!(page1[0].txid, "tx4");
    }

    #[tokio::test]
    async fn record_serde_round_trip_camel_case() {
        let r = SonicstarRecord {
            song_title: "Hello".into(),
            artist_name: "Adele".into(),
            artist_identity_key: String::new(),
            description: Some("a song".into()),
            duration: 240,
            song_file_url: "uhrp://abc".into(),
            art_file_url: Some("uhrp://art".into()),
            preview_url: None,
            genre: Some("Pop".into()),
            album: None,
            release_date: Some("2025-04-25".into()),
            price_per_play: 1000,
            royalty_rate: 75,
            txid: "deadbeef".into(),
            output_index: 2,
            satoshis: 1,
            admitted_at: 1_700_000_000_000,
        };

        let json = serde_json::to_string(&r).unwrap();
        // Confirm camelCase key names match the TS reference.
        assert!(json.contains("\"songTitle\":\"Hello\""));
        assert!(json.contains("\"songFileURL\":\"uhrp://abc\""));
        assert!(json.contains("\"artFileURL\":\"uhrp://art\""));
        assert!(json.contains("\"outputIndex\":2"));
        assert!(json.contains("\"admittedAt\":1700000000000"));
        // None-valued optional fields are dropped (matches TS spread of meta).
        assert!(!json.contains("previewURL"));
        assert!(!json.contains("\"album\""));

        let back: SonicstarRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.song_title, "Hello");
        assert_eq!(back.output_index, 2);
        assert_eq!(back.admitted_at, 1_700_000_000_000);
    }
}
