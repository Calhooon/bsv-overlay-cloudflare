//! POTPARTY Storage trait — backend-agnostic storage for potparty-marker
//! records.
//!
//! One row per marker OUTPOINT — `(txid, outputIndex)` (`potparty_records`
//! in D1). The concrete implementation (D1, in-memory) is provided by the
//! deployment crate; [`MemoryPotpartyStorage`] here backs the unit tests.
//! Structure mirrors `result::storage`:
//!
//! **Keyed by outpoint, NOT by `(identity, gameId)` — every admitted marker
//! is kept.** Admission is byte-format-only (no sig check). Keying on the
//! outpoint keeps a garbage front-run from occupying an identity's slot and
//! censoring a genuine marker (the `tm_result` censorship lesson); it also
//! makes a replayed / duplicate SUBMIT of the same output a harmless no-op.
//!
//! [`PotpartyStorage::store_record`] is insert-if-absent on the outpoint
//! (D1 `INSERT OR IGNORE` on the `(txid, outputIndex)` primary key) — rows
//! are NEVER deleted (a pot-participation fact is permanent recovery
//! history, like a pot or reveal record).
//!
//! `created_at` is assigned by the STORAGE layer at insert (D1 stamps the
//! unix time, the memory impl an insertion counter) — the value on the
//! record passed to `store_record` is ignored. The window ordering rides on
//! it: `list_for_identity` is newest-POT-first over at most one row per pot,
//! `list_for_pot` is OLDEST-first — both dust-DoS bounds, see the trait
//! methods and bsv-low #281.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A potparty-marker record as stored in the index.
///
/// Keyed by the marker OUTPOINT `(txid, outputIndex)` — every admitted
/// marker is kept. Byte fields are carried back VERBATIM to querying
/// clients (which may verify the `sig` themselves — the overlay never
/// does).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PotpartyRecord {
    /// The publishing seat's compressed identity pubkey (33 bytes, lowercase
    /// hex).
    pub identity: String,
    /// The opponent seat's compressed identity pubkey (33 bytes, lowercase
    /// hex).
    #[serde(rename = "opponentIdentity")]
    pub opponent_identity: String,
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The pot funding txid (32 bytes, lowercase hex).
    #[serde(rename = "potTxid")]
    pub pot_txid: String,
    /// The pot output index within `pot_txid`.
    #[serde(rename = "potVout")]
    pub pot_vout: u32,
    /// The pre-signed refund's recovery height.
    #[serde(rename = "recoveryHeight")]
    pub recovery_height: u32,
    /// The seat's DER signature push (lowercase hex) — preserved verbatim,
    /// verified CLIENT-side only (the overlay never verifies it).
    #[serde(rename = "sigHex")]
    pub sig_hex: String,
    /// The txid carrying the marker OP_RETURN — half of the primary key.
    pub txid: String,
    /// The marker output's index within `txid` — the other half of the
    /// primary key.
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Unix seconds at insert — assigned by the storage layer (the value
    /// passed to `store_record` is ignored); recency ordering rides on it.
    #[serde(rename = "createdAt")]
    pub created_at: i64,
}

/// `ls_potparty` query shapes — tagged JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PotpartyQuery {
    /// "Which pots is this identity a party to?" — the recovery question.
    /// At most one row per pot, newest pot first (bsv-low #281).
    #[serde(rename = "partyFor")]
    PartyFor { identity: String, limit: Option<u32> },
    /// "Who are the two parties to this pot outpoint?" — returns every
    /// marker naming the pot (each seat publishes its own). Oldest first
    /// (bsv-low #281).
    #[serde(rename = "byPot")]
    ByPot {
        #[serde(rename = "potTxid")]
        pot_txid: String,
        #[serde(rename = "potVout")]
        pot_vout: u32,
        limit: Option<u32>,
    },
}

/// Backend-agnostic storage for potparty-marker records.
#[async_trait(?Send)]
pub trait PotpartyStorage {
    /// Store a record keyed by its OUTPOINT `(txid, outputIndex)` —
    /// insert-if-absent: a replayed / duplicate SUBMIT of the same output
    /// is a no-op, but markers for the same identity from DIFFERENT txs are
    /// ALL kept. Mirrors the D1 `INSERT OR IGNORE`. Never overwrites, never
    /// deletes. `created_at` is assigned here (the record's value is
    /// ignored).
    async fn store_record(&self, record: &PotpartyRecord) -> Result<(), PotpartyStorageError>;

    /// Up to `limit` records whose `identity` is `identity` — **at most ONE
    /// per pot** (`pot_txid`), newest pot first.
    ///
    /// The per-pot collapse is a DUST-DoS BOUND, not an optimisation
    /// (bsv-low #281): admission is byte-format-only, so anyone can file
    /// markers naming any identity for a dust `OP_RETURN`, and a flat
    /// newest-first row window let `limit` junk rows push a victim's real
    /// pots — the pots it may be owed money from — out of the answer
    /// entirely. `limit` therefore counts POTS, which is also exactly what
    /// this query is asking for: an identity has one genuine marker per pot.
    /// The representative row for a pot is the OLDEST marker naming it (the
    /// honest seat publishes at funding, before an attacker can know the pot
    /// txid). A backend that can see the pot index SHOULD additionally sort
    /// rows naming a pot it has never heard of last — the D1 implementation
    /// does; the in-memory one has no pot table and cannot.
    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError>;

    /// Up to `limit` records naming the pot outpoint `(pot_txid, pot_vout)`,
    /// **oldest first** — the two parties (one marker each).
    ///
    /// Oldest-first is likewise a DoS bound (bsv-low #281): the pot outpoint
    /// is public from the moment funding lands, so under newest-first `limit`
    /// dust markers naming the pot buried BOTH honest seat markers. The
    /// honest markers are published AT funding, so oldest-first puts them
    /// permanently at the head of the window — an attacker cannot spam its
    /// way in front of them after the fact.
    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError>;
}

/// POTPARTY storage errors.
#[derive(Debug, thiserror::Error)]
pub enum PotpartyStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory POTPARTY storage for testing. Insertion order IS recency order
/// (newest = last pushed); `created_at` is stamped with an insertion
/// counter so answers expose a monotone `createdAt` like D1's unix stamp.
#[derive(Debug, Default)]
pub struct MemoryPotpartyStorage {
    records: std::sync::Mutex<Vec<PotpartyRecord>>,
}

impl MemoryPotpartyStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl PotpartyStorage for MemoryPotpartyStorage {
    async fn store_record(&self, record: &PotpartyRecord) -> Result<(), PotpartyStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent on the OUTPOINT (txid, outputIndex) — a replayed
        // submit of the same output is a no-op, matching D1's INSERT OR
        // IGNORE on the primary key.
        let exists = records
            .iter()
            .any(|r| r.txid == record.txid && r.output_index == record.output_index);
        if !exists {
            let mut r = record.clone();
            r.created_at = records.len() as i64;
            records.push(r);
        }
        Ok(())
    }

    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError> {
        let records = self.records.lock().unwrap();
        // Per-pot collapse (bsv-low #281) — mirrors D1's
        // `ROW_NUMBER() OVER (PARTITION BY potTxid ORDER BY createdAt ASC) = 1`:
        // walk in INSERTION order (oldest first) so the first marker seen for
        // a pot is the one that represents it.
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut kept: Vec<&PotpartyRecord> = Vec::new();
        for r in records.iter().filter(|r| r.identity == identity) {
            if seen.insert(r.pot_txid.as_str()) {
                kept.push(r);
            }
        }
        // Newest pot first. `created_at` is a unique insertion counter here,
        // so the order is total — deterministic, like the D1 window's
        // `rowid` tiebreak.
        kept.sort_by_key(|r| std::cmp::Reverse(r.created_at));
        Ok(kept.into_iter().take(limit).cloned().collect())
    }

    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter() // OLDEST first (bsv-low #281) — insertion order
            .filter(|r| r.pot_txid == pot_txid && r.pot_vout == pot_vout)
            .take(limit)
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

    fn record(identity: &str, opponent: &str, txid: &str) -> PotpartyRecord {
        PotpartyRecord {
            identity: identity.into(),
            opponent_identity: opponent.into(),
            game_id: "11".repeat(32),
            pot_txid: "22".repeat(32),
            pot_vout: 0,
            recovery_height: 850_000,
            sig_hex: "3045ab".into(),
            txid: txid.into(),
            output_index: 0,
            created_at: 0, // ignored — storage assigns
        }
    }

    #[tokio::test]
    async fn store_then_list_roundtrips() {
        let store = MemoryPotpartyStorage::new();
        store
            .store_record(&record("02aa", "03bb", "tx1"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);

        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        assert_eq!(rows[0].opponent_identity, "03bb");
        assert_eq!(rows[0].recovery_height, 850_000);
    }

    #[tokio::test]
    async fn list_for_identity_filters_by_identity_only() {
        let store = MemoryPotpartyStorage::new();
        store
            .store_record(&record("02aa", "03bb", "tx1"))
            .await
            .unwrap();
        // The opponent's OWN marker (seats flipped) is a different row.
        store
            .store_record(&record("03bb", "02aa", "tx2"))
            .await
            .unwrap();

        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        assert!(store
            .list_for_identity("02cc", 100)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn list_for_pot_returns_both_parties() {
        let store = MemoryPotpartyStorage::new();
        // Same pot outpoint, two markers (one per seat).
        store
            .store_record(&record("02aa", "03bb", "txA"))
            .await
            .unwrap();
        store
            .store_record(&record("03bb", "02aa", "txB"))
            .await
            .unwrap();
        // A different pot vout is NOT matched.
        let mut other = record("02aa", "03bb", "txC");
        other.pot_vout = 1;
        store.store_record(&other).await.unwrap();

        let rows = store
            .list_for_pot(&"22".repeat(32), 0, 100)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2, "both parties to vout 0");
        // OLDEST first (bsv-low #281) — later dust naming this pot can never
        // push the honest seat markers out of the window.
        assert_eq!(rows[0].txid, "txA", "oldest first");
        assert_eq!(rows[1].txid, "txB");
    }

    #[tokio::test]
    async fn list_for_identity_is_newest_pot_first_and_respects_limit() {
        let store = MemoryPotpartyStorage::new();
        // FIVE DISTINCT POTS — since bsv-low #281 the window counts pots, so
        // a fixture that reused one pot txid would (correctly) collapse to a
        // single row and prove nothing about the limit.
        for i in 0..5u8 {
            let mut r = record("02aa", "03bb", &format!("tx{i}"));
            r.pot_txid = format!("{i:02x}").repeat(32);
            store.store_record(&r).await.unwrap();
        }
        let rows = store.list_for_identity("02aa", 3).await.unwrap();
        assert_eq!(rows.len(), 3, "limit respected");
        assert_eq!(rows[0].txid, "tx4", "newest pot first");
        assert!(rows[0].created_at > rows[1].created_at);
    }

    /// bsv-low #281 — the per-pot collapse, at the trait's own level: many
    /// markers naming ONE pot occupy ONE slot, so they cannot crowd a
    /// victim's other pots out of the recovery window.
    #[tokio::test]
    async fn many_markers_for_one_pot_consume_one_slot() {
        let store = MemoryPotpartyStorage::new();
        // The victim's honest marker for its real pot, published FIRST.
        let mut honest = record("02aa", "03bb", "txHONEST");
        honest.pot_txid = "aa".repeat(32);
        store.store_record(&honest).await.unwrap();
        // 120 replays of a marker naming ANOTHER pot, all naming the victim.
        for i in 0..120u32 {
            let mut junk = record("02aa", "03cc", &format!("txJUNK{i}"));
            junk.pot_txid = "bb".repeat(32);
            store.store_record(&junk).await.unwrap();
        }
        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 2, "121 rows, 2 pots ⇒ 2 slots");
        assert!(
            rows.iter().any(|r| r.txid == "txHONEST"),
            "the honest pot survives the flood"
        );
    }

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let store = MemoryPotpartyStorage::new();
        store
            .store_record(&record("02aa", "03bb", "txSAME"))
            .await
            .unwrap();
        // A replayed / duplicate SUBMIT of the SAME output — ignored.
        let replay = record("02aa", "03cc", "txSAME");
        store.store_record(&replay).await.unwrap();

        assert_eq!(store.record_count(), 1);
        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(
            rows[0].opponent_identity, "03bb",
            "first insert for the outpoint kept"
        );
    }

    #[test]
    fn query_json_shapes() {
        let q: PotpartyQuery = serde_json::from_value(serde_json::json!({
            "type": "partyFor",
            "identity": "02".to_string() + &"a1".repeat(32),
            "limit": 50
        }))
        .unwrap();
        match q {
            PotpartyQuery::PartyFor { identity, limit } => {
                assert_eq!(identity.len(), 66);
                assert_eq!(limit, Some(50));
            }
            other => panic!("expected PartyFor, got {other:?}"),
        }

        // limit optional; byPot shape.
        let q: PotpartyQuery = serde_json::from_value(serde_json::json!({
            "type": "byPot",
            "potTxid": "22".repeat(32),
            "potVout": 3
        }))
        .unwrap();
        match q {
            PotpartyQuery::ByPot {
                pot_txid,
                pot_vout,
                limit,
            } => {
                assert_eq!(pot_txid.len(), 64);
                assert_eq!(pot_vout, 3);
                assert_eq!(limit, None);
            }
            other => panic!("expected ByPot, got {other:?}"),
        }

        assert!(
            serde_json::from_value::<PotpartyQuery>(serde_json::json!({"type": "nope"})).is_err()
        );
    }
}
