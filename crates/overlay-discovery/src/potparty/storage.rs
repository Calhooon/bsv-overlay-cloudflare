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
//! it: `list_for_identity` is newest-POT-first over at most two rows per pot
//! (the oldest v1 and the oldest v2 marker), `list_for_pot` is OLDEST-first —
//! both dust-DoS bounds, see the trait methods and bsv-low #281.

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
    /// v2 only (bsv-low #230): the seat's SETTLE pubkey (33 bytes, lowercase
    /// hex) — the key the covenant lock commits for this seat. `None` for a
    /// v1 marker (NULL column).
    #[serde(rename = "seatSettlePubkey", default)]
    pub seat_settle_pubkey: Option<String>,
    /// v2 only: the settle key's DER signature over the seat-binding
    /// preimage (lowercase hex) — carried verbatim, verified by READERS
    /// (the app-layer / clients), never here. `None` for v1.
    #[serde(rename = "seatSigHex", default)]
    pub seat_sig_hex: Option<String>,
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
    /// `limit` counts POTS; up to two rows per pot (v1 + v2), newest pot
    /// first (bsv-low #281).
    #[serde(rename = "partyFor")]
    PartyFor {
        identity: String,
        limit: Option<u32>,
    },
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
        /// Page start (rows to skip in the oldest-first total order),
        /// default 0 — the `ls_potrefund byPot` shape (bsv-low #291 gate
        /// M2), added here by bsv-low#354/#356.
        ///
        /// This window is NOT identity-scoped: `potTxid`/`potVout` are
        /// CLAIMS inside the marker payload, so a stranger can file
        /// unlimited markers naming a victim's (public) pot outpoint from
        /// its own transactions. Without a page cursor a client read page 0
        /// forever and ~100 free rows stamped before the honest seats
        /// evicted BOTH of them from the caller's attribution fold —
        /// permanently, since rows are never deleted and `createdAt` is
        /// server-assigned. Paging is what makes every admitted row
        /// REACHABLE while each response stays payload-bounded.
        #[serde(default)]
        offset: Option<u32>,
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

    /// Records whose `identity` is `identity`, for up to `limit` POTS —
    /// newest pot first, a BOUNDED SUPERSET per pot outpoint: several v1
    /// markers AND several v2 (seat-binding) markers.
    ///
    /// The per-pot collapse is a DUST-DoS BOUND, not an optimisation
    /// (bsv-low #281): admission is byte-format-only, so anyone can file
    /// markers naming any identity for a dust `OP_RETURN`, and a flat
    /// newest-first row window let `limit` junk rows push a victim's real
    /// pots — the pots it may be owed money from — out of the answer
    /// entirely. `limit` therefore counts POTS, which is also exactly what
    /// this query is asking for.
    ///
    /// It does NOT collapse each group to one row. **Verification must happen
    /// BEFORE collapse: a layer that cannot verify signatures must never
    /// choose which row is real.** Admission is byte-format-only, so an
    /// attacker can file a marker with an EARLIER `created_at` than yours —
    /// the publish completes on a later visit and backfills historical pots,
    /// so the pot txid has been public for a long time. Collapsing to the
    /// oldest row therefore handed the consumer a forgery, which it then
    /// dropped for failing its signature check, erasing the pot. The window
    /// bounds COST; the verifying consumer decides truth.
    ///
    /// BOTH groups must be returned. Returning only the v1 row leaves a
    /// client unable to latch `v2Indexed`, so it republishes a PAID marker
    /// forever; returning only the v2 row can erase the pot outright, because
    /// `lookupPotParty` verifies v2 signatures client-side and DROPS a row
    /// that fails, falling back on the v1 sibling for discovery.
    ///
    /// A backend that can see the pot index SHOULD additionally sort rows
    /// naming a pot it has never heard of behind the rest, while still
    /// serving them (a strict filter would erase a pot whose admission is
    /// merely in flight) — the D1 implementation does; the in-memory one has
    /// no pot table and cannot.
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
    ///
    /// …unless it was there FIRST, which is the case this window could not
    /// answer until bsv-low#354/#356. Markers can be filed before a pot is
    /// funded (the outpoint is a payload claim, not this row's own txid), and
    /// `createdAt` is server-stamped, so a stranger's pre-funding flood sits
    /// permanently at the head of the order. `offset` (the `ls_potrefund`
    /// shape, bsv-low #291 gate M2) is what makes every admitted row
    /// REACHABLE: the caller pages `offset += limit` past the flood instead
    /// of the cap silently amputating the honest tail. The total order is
    /// append-only (`createdAt ASC, rowid ASC`), so pages are stable — a
    /// concurrent insert can never shift a row across a boundary already
    /// fetched.
    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
        offset: usize,
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
        // Per-pot-OUTPOINT collapse, ONE ROW PER (pot, v1/v2) GROUP —
        // mirrors D1's `ROW_NUMBER() OVER (PARTITION BY potTxid, potVout,
        // has-seat-key ORDER BY createdAt ASC) = 1`. Walking in INSERTION
        // order (oldest first) makes the first marker seen for a group the
        // one that represents it.
        //
        // Both groups matter (bsv-low #281 F2): the v2 row is the ONLY thing
        // that lets a client latch `v2Indexed` (else it republishes a paid
        // OP_RETURN forever), and the v1 row is what `lookupPotParty` falls
        // back on for discovery when a v2 row fails its client-side verify.
        let mut seen: std::collections::HashSet<(&str, u32, bool)> =
            std::collections::HashSet::new();
        let mut kept: Vec<&PotpartyRecord> = Vec::new();
        for r in records.iter().filter(|r| r.identity == identity) {
            let is_v2 = r.seat_settle_pubkey.is_some();
            if seen.insert((r.pot_txid.as_str(), r.pot_vout, is_v2)) {
                kept.push(r);
            }
        }
        // `limit` counts POTS, so take that many distinct pot outpoints
        // (newest first) and keep every kept row belonging to them. The
        // in-memory backend has no pot table, so it cannot apply the D1
        // window's pot-EXISTENCE tier — documented on the trait.
        kept.sort_by_key(|r| std::cmp::Reverse(r.created_at));
        let mut pots: Vec<(&str, u32)> = Vec::new();
        for r in &kept {
            let key = (r.pot_txid.as_str(), r.pot_vout);
            if !pots.contains(&key) {
                pots.push(key);
            }
        }
        pots.truncate(limit);
        Ok(kept
            .into_iter()
            .filter(|r| pots.contains(&(r.pot_txid.as_str(), r.pot_vout)))
            .cloned()
            .collect())
    }

    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter() // OLDEST first (bsv-low #281) — insertion order
            .filter(|r| r.pot_txid == pot_txid && r.pot_vout == pot_vout)
            .skip(offset) // page start (#354/#356) — mirrors D1's OFFSET
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
            seat_settle_pubkey: None,
            seat_sig_hex: None,
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
            .list_for_pot(&"22".repeat(32), 0, 100, 0)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2, "both parties to vout 0");
        // OLDEST first (bsv-low #281) — later dust naming this pot can never
        // push the honest seat markers out of the window.
        assert_eq!(rows[0].txid, "txA", "oldest first");
        assert_eq!(rows[1].txid, "txB");
    }

    /// bsv-low#354/#356 — the byPot window is not identity-scoped, so a
    /// stranger's PRE-FUNDING flood sits permanently at the head of the
    /// oldest-first order and page 0 is all junk. `offset` is what makes the
    /// honest seat markers reachable at all.
    #[tokio::test]
    async fn list_for_pot_pages_past_a_flood_at_the_head_of_the_order() {
        let store = MemoryPotpartyStorage::new();
        // The flood lands FIRST — markers may be filed before the pot is
        // funded, and `createdAt` is server-stamped, so this order is not
        // something the honest seats can beat.
        for i in 0..8u8 {
            store
                .store_record(&record("02ee", "03ff", &format!("txJUNK{i}")))
                .await
                .unwrap();
        }
        store
            .store_record(&record("02aa", "03bb", "txSEATA"))
            .await
            .unwrap();
        store
            .store_record(&record("03bb", "02aa", "txSEATB"))
            .await
            .unwrap();

        let pot = "22".repeat(32);
        // Page 0 at the flood's size: BOTH honest seats are invisible. This
        // is the state the caller is in today, pinned from the unsafe side.
        let page0 = store.list_for_pot(&pot, 0, 8, 0).await.unwrap();
        assert_eq!(page0.len(), 8);
        assert!(
            !page0.iter().any(|r| r.txid.starts_with("txSEAT")),
            "the honest markers are past the cap"
        );
        // …and page 1 reaches them, which is the whole point.
        let page1 = store.list_for_pot(&pot, 0, 8, 8).await.unwrap();
        assert_eq!(
            page1.iter().map(|r| r.txid.as_str()).collect::<Vec<_>>(),
            vec!["txSEATA", "txSEATB"],
        );
        // Pages do not overlap and do not skip: the order is a total one.
        let all: Vec<String> = page0
            .iter()
            .chain(page1.iter())
            .map(|r| r.txid.clone())
            .collect();
        assert_eq!(all.len(), 10);
        assert_eq!(
            all,
            store
                .list_for_pot(&pot, 0, 100, 0)
                .await
                .unwrap()
                .iter()
                .map(|r| r.txid.clone())
                .collect::<Vec<_>>(),
            "paging reconstructs the unpaged answer exactly"
        );
        // An offset past the end is an empty page, never an error.
        assert!(store
            .list_for_pot(&pot, 0, 8, 10_000)
            .await
            .unwrap()
            .is_empty());
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
                offset,
            } => {
                assert_eq!(pot_txid.len(), 64);
                assert_eq!(pot_vout, 3);
                assert_eq!(limit, None);
                // #354/#356: absent on the wire → the head of the order, so
                // every already-deployed caller keeps exactly today's answer.
                assert_eq!(offset, None);
            }
            other => panic!("expected ByPot, got {other:?}"),
        }

        // …and a caller that DOES page is parsed (the half the flood victim
        // needs — without it "page past the junk" is advice, not a mechanism).
        let q: PotpartyQuery = serde_json::from_value(serde_json::json!({
            "type": "byPot",
            "potTxid": "22".repeat(32),
            "potVout": 0,
            "limit": 500,
            "offset": 500
        }))
        .unwrap();
        match q {
            PotpartyQuery::ByPot { limit, offset, .. } => {
                assert_eq!((limit, offset), (Some(500), Some(500)));
            }
            other => panic!("expected ByPot, got {other:?}"),
        }

        assert!(
            serde_json::from_value::<PotpartyQuery>(serde_json::json!({"type": "nope"})).is_err()
        );
    }
}
