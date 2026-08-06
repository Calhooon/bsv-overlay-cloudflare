//! COLLECTED Storage trait — backend-agnostic storage for collected-marker
//! records.
//!
//! One row per marker OUTPOINT `(txid, outputIndex)` (`collected_markers_v2` in
//! D1). The concrete implementation (D1, in-memory) is provided by the
//! deployment crate; [`MemoryCollectedStorage`] here backs the unit tests.
//!
//! **Every admitted marker is kept (bsv-low #327 S8).**
//! [`CollectedStorage::store_record`] is insert-if-absent on the OUTPOINT (D1
//! `INSERT OR IGNORE` on the primary key), so a replayed submit of the same
//! output is a harmless no-op — but markers for the same `(identity, gameId)`
//! from DIFFERENT transactions ALL coexist, and rows are NEVER deleted.
//!
//! ## Why the key moved (epoch Rules 2 and 3)
//!
//! The superseded shape keyed `(identity, gameId)` with first-marker-wins. Both
//! halves of that key are PUBLIC and CLAIMABLE — the identity appears on every
//! `ls_result` row and the gameId on `tm_result`/`tm_pot` — and admission is
//! byte-format-only, with the `identityKey` push being arbitrary
//! attacker-supplied bytes whose `sig` is never verified server-side. So one
//! submit naming a VICTIM could occupy that victim's slot at deal time, long
//! before they ever collected, and their genuine marker was then silently
//! `INSERT OR IGNORE`d away — permanent, pre-emptive censorship.
//!
//! **Exclusivity was the bug** (Rule 3: an index is a set, not a slot). Keying
//! on the outpoint does not merely patch it — the collision stops existing,
//! because a squatter can only ever occupy the worthless outpoint it actually
//! fabricated. There is no adjudication, no first-writer-wins, and no tie-break
//! left to get wrong.
//!
//! The reader separates genuine from junk exactly as before, by verifying the
//! sig under its OWN identity — and that client-side hardening
//! (`app/src/lib/collected.ts` groupByKey + selectVerified) only becomes REAL
//! with this re-key: against the old schema the genuine sibling row could never
//! be stored, so the multi-row response it was written for was unreachable
//! (Rule 18). A row's PRESENCE still proves nothing: verify the sig, or do not
//! read it.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A collected-marker record as stored in the index.
///
/// Keyed by the marker OUTPOINT `(txid, output_index)` — unforgeable and
/// self-owned, unlike the claimable `(identity, gameId)` pair it replaced.
/// `sig_hex` is carried back verbatim to querying clients (which verify it
/// under their own wallet); it is `Option` to mirror the nullable D1 column,
/// though the admit path always stores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectedRecord {
    /// The publisher's compressed identity pubkey (33 bytes, lowercase hex).
    pub identity: String,
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The txid carrying the marker OP_RETURN. Part of the primary key, so
    /// unlike the superseded shape it is NOT nullable.
    pub txid: String,
    /// The marker output's index within `txid`. The other half of the key.
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// The marker's DER signature push (lowercase hex) — verified
    /// CLIENT-side only.
    #[serde(rename = "sigHex")]
    pub sig_hex: Option<String>,
}

/// `ls_collected` query shapes — tagged JSON, e.g.
/// `{"type":"collectedFor","identity":"<hex>","gameIds":["<hex>",…]}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CollectedQuery {
    /// "Which of these games has this identity already collected?" — the
    /// client's card-gather question. The answer is an input-ordered array,
    /// one entry per requested gameId.
    #[serde(rename = "collectedFor")]
    CollectedFor {
        identity: String,
        #[serde(rename = "gameIds")]
        game_ids: Vec<String>,
    },
}

/// Backend-agnostic storage for collected-marker records.
#[async_trait(?Send)]
pub trait CollectedStorage {
    /// Store a record keyed by the OUTPOINT `(txid, output_index)` —
    /// insert-if-absent: a replay of the SAME output is a no-op, but a marker
    /// for the same `(identity, gameId)` from a different tx is a NEW row.
    /// Mirrors the D1 `INSERT OR IGNORE`. Never overwrites, never deletes.
    async fn store_record(&self, record: &CollectedRecord) -> Result<(), CollectedStorageError>;

    /// EVERY marker admitted for `(identity, gameId)` — empty when none was.
    ///
    /// Returns a set, not a winner: a squatted row and the victim's genuine row
    /// coexist here, and the CALLER's signature verify decides between them.
    async fn get_records_for(
        &self,
        identity: &str,
        game_id: &str,
    ) -> Result<Vec<CollectedRecord>, CollectedStorageError>;

    /// Batched [`get_records_for`](Self::get_records_for) for one identity over
    /// many gameIds (bsv-low #289): the result is ALIGNED index-for-index with
    /// `game_ids` (an EMPTY vec where no marker exists), so the caller's
    /// fail-safe `present: false` semantics are unchanged. This default loops
    /// the single-pair method; the D1 backend overrides it with one
    /// `gameId IN (…)` query per chunk instead of a round trip per game.
    async fn get_records(
        &self,
        identity: &str,
        game_ids: &[String],
    ) -> Result<Vec<Vec<CollectedRecord>>, CollectedStorageError> {
        let mut out = Vec::with_capacity(game_ids.len());
        for game_id in game_ids {
            out.push(self.get_records_for(identity, game_id).await?);
        }
        Ok(out)
    }
}

/// COLLECTED storage errors.
#[derive(Debug, thiserror::Error)]
pub enum CollectedStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory COLLECTED storage for testing.
#[derive(Debug, Default)]
pub struct MemoryCollectedStorage {
    records: std::sync::Mutex<Vec<CollectedRecord>>,
}

impl MemoryCollectedStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl CollectedStorage for MemoryCollectedStorage {
    async fn store_record(&self, record: &CollectedRecord) -> Result<(), CollectedStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent on the OUTPOINT, matching D1's INSERT OR IGNORE on
        // the (txid, outputIndex) primary key. Two markers for one
        // (identity, gameId) from different txs are two rows, by design.
        let exists = records
            .iter()
            .any(|r| r.txid == record.txid && r.output_index == record.output_index);
        if !exists {
            records.push(record.clone());
        }
        Ok(())
    }

    async fn get_records_for(
        &self,
        identity: &str,
        game_id: &str,
    ) -> Result<Vec<CollectedRecord>, CollectedStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.identity == identity && r.game_id == game_id)
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

    fn record(identity: &str, game_id: &str, txid: &str, sig_hex: &str) -> CollectedRecord {
        rec_at(identity, game_id, txid, 0, sig_hex)
    }

    fn rec_at(
        identity: &str,
        game_id: &str,
        txid: &str,
        output_index: u32,
        sig_hex: &str,
    ) -> CollectedRecord {
        CollectedRecord {
            identity: identity.into(),
            game_id: game_id.into(),
            txid: txid.into(),
            output_index,
            sig_hex: Some(sig_hex.into()),
        }
    }

    #[tokio::test]
    async fn store_then_get_roundtrips() {
        let store = MemoryCollectedStorage::new();
        store
            .store_record(&record("02aa", &"11".repeat(32), "tx1", "3045ab"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);

        let rows = store
            .get_records_for("02aa", &"11".repeat(32))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        assert_eq!(rows[0].sig_hex.as_deref(), Some("3045ab"));
    }

    #[tokio::test]
    async fn get_unknown_pair_is_empty() {
        let store = MemoryCollectedStorage::new();
        assert!(store
            .get_records_for("02aa", &"11".repeat(32))
            .await
            .unwrap()
            .is_empty());
        store
            .store_record(&record("02aa", &"11".repeat(32), "tx1", "3045ab"))
            .await
            .unwrap();
        // Same identity, different game → still unknown.
        assert!(store
            .get_records_for("02aa", &"22".repeat(32))
            .await
            .unwrap()
            .is_empty());
        // Same game, different identity → still unknown.
        assert!(store
            .get_records_for("02bb", &"11".repeat(32))
            .await
            .unwrap()
            .is_empty());
    }

    /// #327 S8 — the INVERSION of the old `first_marker_wins_never_overwritten`.
    ///
    /// That cell pinned the defect as if it were the contract: a second marker
    /// for one (identity, gameId) was DROPPED, which is exactly how a squatter
    /// censored the victim's genuine marker forever. Under the outpoint key the
    /// intended behaviour is the opposite — both rows coexist and the caller's
    /// signature verify picks the genuine one (Rule 11: a seam must assert the
    /// INTENDED behaviour, not the observed one).
    #[tokio::test]
    async fn a_squat_can_no_longer_censor_the_genuine_marker() {
        let store = MemoryCollectedStorage::new();
        let gid = "11".repeat(32);
        // The SQUATTER files first, at deal time, naming the victim.
        store
            .store_record(&record("02victim", &gid, "txSQUAT", "sigJUNK"))
            .await
            .unwrap();
        // The victim's GENUINE marker lands later, from a different tx.
        store
            .store_record(&record("02victim", &gid, "txGENUINE", "sigREAL"))
            .await
            .unwrap();

        let rows = store.get_records_for("02victim", &gid).await.unwrap();
        assert_eq!(
            rows.len(),
            2,
            "both rows must coexist — exclusivity was the bug"
        );
        let txids: Vec<&str> = rows.iter().map(|r| r.txid.as_str()).collect();
        assert!(txids.contains(&"txSQUAT"));
        assert!(
            txids.contains(&"txGENUINE"),
            "the genuine marker must survive a pre-emptive squat"
        );
    }

    /// A replay of the SAME outpoint is still a no-op — the property that makes
    /// the re-key safe under a re-submitting client.
    #[tokio::test]
    async fn replaying_the_same_outpoint_is_a_noop() {
        let store = MemoryCollectedStorage::new();
        let gid = "11".repeat(32);
        store
            .store_record(&record("02aa", &gid, "txA", "sigFIRST"))
            .await
            .unwrap();
        store
            .store_record(&record("02aa", &gid, "txA", "sigSECOND"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);
        let rows = store.get_records_for("02aa", &gid).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].sig_hex.as_deref(),
            Some("sigFIRST"),
            "same outpoint never overwrites"
        );
    }

    /// Two markers in ONE transaction are two distinct rows — the outputIndex
    /// half of the key is load-bearing, not decorative.
    #[tokio::test]
    async fn two_outputs_of_one_tx_are_distinct_rows() {
        let store = MemoryCollectedStorage::new();
        let gid = "11".repeat(32);
        store
            .store_record(&rec_at("02aa", &gid, "txA", 0, "s0"))
            .await
            .unwrap();
        store
            .store_record(&rec_at("02aa", &gid, "txA", 1, "s1"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 2);
        assert_eq!(store.get_records_for("02aa", &gid).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn distinct_pairs_tracked_independently() {
        let store = MemoryCollectedStorage::new();
        store
            .store_record(&record("02aa", &"11".repeat(32), "tx1", "s1"))
            .await
            .unwrap();
        store
            .store_record(&record("02aa", &"22".repeat(32), "tx2", "s2"))
            .await
            .unwrap();
        store
            .store_record(&record("02bb", &"11".repeat(32), "tx3", "s3"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 3);

        let rows = store
            .get_records_for("02bb", &"11".repeat(32))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx3");
    }

    #[test]
    fn query_json_shape() {
        let q: CollectedQuery = serde_json::from_value(serde_json::json!({
            "type": "collectedFor",
            "identity": "02".to_string() + &"a1".repeat(32),
            "gameIds": ["11".repeat(32), "22".repeat(32)]
        }))
        .unwrap();
        let CollectedQuery::CollectedFor { identity, game_ids } = q;
        assert_eq!(identity.len(), 66);
        assert_eq!(game_ids.len(), 2);

        // Unknown type is an error.
        assert!(
            serde_json::from_value::<CollectedQuery>(serde_json::json!({"type": "nope"})).is_err()
        );
    }
}
