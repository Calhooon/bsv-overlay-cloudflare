//! HAND Storage trait — backend-agnostic storage for showdown-hand marker
//! records (bsv-low #382).
//!
//! One row per marker OUTPOINT `(txid, outputIndex)` (`hand_markers` in D1).
//! The concrete implementation (D1, in-memory) is provided by the deployment
//! crate; [`MemoryHandStorage`] here backs the unit tests.
//!
//! **Every admitted marker is kept** — the collected/result-v2 outpoint-key
//! discipline verbatim (epoch Rules 2 and 3: `(gameId, identity)` are both
//! public claimable names and admission is byte-format-only, so any per-pair
//! slot would be a pre-emptive censorship primitive; the outpoint is the only
//! self-owned key). [`HandStorage::store_record`] is insert-if-absent on the
//! outpoint (D1 `INSERT OR IGNORE`), rows are NEVER deleted, and the READER
//! separates genuine from junk by verifying each row's sig under its own
//! named identity ('anyone' ProtoWallet round-trip, client-side only).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A hand-marker record as stored in the index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandRecord {
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The publisher's compressed identity pubkey (33 bytes, lowercase hex).
    pub identity: String,
    /// The pot funding txid the hand is bound to (32 bytes, lowercase hex).
    #[serde(rename = "potTxid")]
    pub pot_txid: String,
    /// The five card ordinals as 10 lowercase hex chars (wire order).
    #[serde(rename = "cardsHex")]
    pub cards_hex: String,
    /// The txid carrying the marker OP_RETURN — half the primary key.
    pub txid: String,
    /// The marker output's index within `txid` — the other half of the key.
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// The marker's DER signature push (lowercase hex) — verified
    /// CLIENT-side only, under `[1,'low hand']` / keyID = gameId /
    /// counterparty 'anyone'.
    #[serde(rename = "sigHex")]
    pub sig_hex: Option<String>,
}

/// `ls_hand` query shapes — tagged JSON, e.g.
/// `{"type":"handsForGames","gameIds":["<hex>",…]}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HandQuery {
    /// "Every stored hand marker for these games" — the history drill-down's
    /// question. Both seats' rows (and any junk rows) come back; the client's
    /// per-row signature verify separates them. No identity filter on
    /// purpose: the drill-down wants BOTH hands, and the rows are public
    /// on-chain facts either way.
    #[serde(rename = "handsForGames")]
    HandsForGames {
        #[serde(rename = "gameIds")]
        game_ids: Vec<String>,
    },
}

/// Rows kept per (game, identity) by every hand-marker read window.
///
/// The honest population per (game, seat) is ONE row; 4 leaves headroom for a
/// republish without unbounded growth. The RESIDUAL an identity-spray leaves —
/// K distinct junk identities → up to K×4 rows returned for the game — is
/// verify-rejected NOISE (dropped by `hand::validity::row_valid`, latched at
/// admission since brain-cutover M1); the two genuine seats' rows are ALWAYS
/// returned (their partitions are never evicted), so the feature never breaks.
/// Eviction is FAIL-SAFE regardless: a pushed-out row only omits a hand from
/// the drill-down (display-only, no money path reads this index).
///
/// SHARED (brain-cutover M2): the overlay's `ls_hand` window and the
/// app-layer's `/results` hands join must be the SAME window, or two reads of
/// one store disagree (epoch Rule 16) — so the constant lives here rather
/// than as a literal in each.
pub const HAND_ROWS_PER_SEAT: usize = 4;

/// Backend-agnostic storage for hand-marker records.
#[async_trait(?Send)]
pub trait HandStorage {
    /// Store a record keyed by the OUTPOINT `(txid, output_index)` —
    /// insert-if-absent (D1 `INSERT OR IGNORE`): a replay of the SAME output
    /// is a no-op, but a marker for the same `(gameId, identity)` from a
    /// different tx is a NEW row. Never overwrites, never deletes.
    async fn store_record(&self, record: &HandRecord) -> Result<(), HandStorageError>;

    /// The stored markers for `game_id` — empty when none was. A set, not a
    /// winner: junk and genuine rows coexist; the CALLER's signature verify
    /// decides between them.
    ///
    /// SHIPPING BACKEND SEMANTICS: the D1 impl returns a per-(game, identity)
    /// WINDOW (the newest [`HAND_ROWS_PER_SEAT`](crate) rows per identity — the
    /// censorship-isolation window, see `d1_discovery`), NOT literally every
    /// admitted row; the two genuine seats' rows are always present, and an
    /// identity-spray's excess is bounded per identity. `MemoryHandStorage`
    /// (tests only) returns everything in insertion order — enough for the
    /// storage-shape cells, but the WINDOW semantics are proven only by the
    /// real-SQLite tests over the shipped SQL.
    async fn get_records_for_game(
        &self,
        game_id: &str,
    ) -> Result<Vec<HandRecord>, HandStorageError>;

    /// Batched [`get_records_for_game`](Self::get_records_for_game): the
    /// result is ALIGNED index-for-index with `game_ids` (an EMPTY vec where
    /// no marker exists). This default loops the single-game method; the D1
    /// backend overrides it with one `gameId IN (…)` query per chunk.
    async fn get_records(
        &self,
        game_ids: &[String],
    ) -> Result<Vec<Vec<HandRecord>>, HandStorageError> {
        let mut out = Vec::with_capacity(game_ids.len());
        for game_id in game_ids {
            out.push(self.get_records_for_game(game_id).await?);
        }
        Ok(out)
    }
}

/// HAND storage errors.
#[derive(Debug, thiserror::Error)]
pub enum HandStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory HAND storage for testing.
#[derive(Debug, Default)]
pub struct MemoryHandStorage {
    records: std::sync::Mutex<Vec<HandRecord>>,
}

impl MemoryHandStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl HandStorage for MemoryHandStorage {
    async fn store_record(&self, record: &HandRecord) -> Result<(), HandStorageError> {
        let mut records = self.records.lock().unwrap();
        let exists = records
            .iter()
            .any(|r| r.txid == record.txid && r.output_index == record.output_index);
        if !exists {
            records.push(record.clone());
        }
        Ok(())
    }

    async fn get_records_for_game(
        &self,
        game_id: &str,
    ) -> Result<Vec<HandRecord>, HandStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.game_id == game_id)
            .cloned()
            .collect())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn rec_at(
        game_id: &str,
        identity: &str,
        txid: &str,
        output_index: u32,
        cards_hex: &str,
    ) -> HandRecord {
        HandRecord {
            game_id: game_id.into(),
            identity: identity.into(),
            pot_txid: "33".repeat(32),
            cards_hex: cards_hex.into(),
            txid: txid.into(),
            output_index,
            sig_hex: Some("3045ab".into()),
        }
    }

    #[tokio::test]
    async fn store_then_get_roundtrips() {
        let store = MemoryHandStorage::new();
        let gid = "11".repeat(32);
        store
            .store_record(&rec_at(&gid, "02aa", "tx1", 0, "000c1a2733"))
            .await
            .unwrap();
        let rows = store.get_records_for_game(&gid).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cards_hex, "000c1a2733");
    }

    /// BOTH seats' rows for one game coexist — the whole point of the index.
    #[tokio::test]
    async fn both_seats_rows_coexist() {
        let store = MemoryHandStorage::new();
        let gid = "11".repeat(32);
        store
            .store_record(&rec_at(&gid, "02seatA", "txA", 0, "000c1a2733"))
            .await
            .unwrap();
        store
            .store_record(&rec_at(&gid, "02seatB", "txB", 0, "0102030405"))
            .await
            .unwrap();
        let rows = store.get_records_for_game(&gid).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    /// A junk row naming a victim identity cannot censor the genuine one —
    /// the collected-module outpoint-key property, re-asserted here.
    #[tokio::test]
    async fn a_squat_cannot_censor_the_genuine_marker() {
        let store = MemoryHandStorage::new();
        let gid = "11".repeat(32);
        store
            .store_record(&rec_at(&gid, "02victim", "txSQUAT", 0, "0001020304"))
            .await
            .unwrap();
        store
            .store_record(&rec_at(&gid, "02victim", "txGENUINE", 0, "000c1a2733"))
            .await
            .unwrap();
        let rows = store.get_records_for_game(&gid).await.unwrap();
        assert_eq!(rows.len(), 2, "both rows must coexist — no per-pair slot");
    }

    #[tokio::test]
    async fn replaying_the_same_outpoint_is_a_noop() {
        let store = MemoryHandStorage::new();
        let gid = "11".repeat(32);
        store
            .store_record(&rec_at(&gid, "02aa", "txA", 0, "000c1a2733"))
            .await
            .unwrap();
        store
            .store_record(&rec_at(&gid, "02aa", "txA", 0, "0102030405"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);
        let rows = store.get_records_for_game(&gid).await.unwrap();
        assert_eq!(rows[0].cards_hex, "000c1a2733", "same outpoint never overwrites");
    }

    #[tokio::test]
    async fn batched_get_is_input_aligned() {
        let store = MemoryHandStorage::new();
        let g1 = "11".repeat(32);
        let g2 = "22".repeat(32);
        store
            .store_record(&rec_at(&g2, "02aa", "tx1", 0, "000c1a2733"))
            .await
            .unwrap();
        let out = store.get_records(&[g1.clone(), g2.clone()]).await.unwrap();
        assert_eq!(out.len(), 2);
        assert!(out[0].is_empty(), "absent game answers EMPTY, never omitted");
        assert_eq!(out[1].len(), 1);
    }

    #[test]
    fn query_json_shape() {
        let q: HandQuery = serde_json::from_value(serde_json::json!({
            "type": "handsForGames",
            "gameIds": ["11".repeat(32), "22".repeat(32)]
        }))
        .unwrap();
        let HandQuery::HandsForGames { game_ids } = q;
        assert_eq!(game_ids.len(), 2);
        assert!(serde_json::from_value::<HandQuery>(serde_json::json!({"type": "nope"})).is_err());
    }
}
