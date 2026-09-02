//! HOPPARTY Storage trait — backend-agnostic storage for hopparty-marker
//! records (bsv-low #315).
//!
//! One row per marker OUTPOINT — `(txid, outputIndex)` (`hopparty_records`
//! in D1). The concrete implementation (D1, in-memory) is provided by the
//! deployment crate; [`MemoryHoppartyStorage`] here backs the unit tests.
//! Structure mirrors `potparty::storage`:
//!
//! **Keyed by outpoint, NOT by `(identity, gameId)` — every admitted marker
//! is kept.** Admission is byte-format-only (no sig check). Keying on the
//! outpoint keeps a garbage front-run from occupying an identity's slot and
//! censoring a genuine marker (the `tm_result` censorship lesson); it also
//! makes a replayed / duplicate SUBMIT of the same output a harmless no-op.
//!
//! [`HoppartyStorage::store_record`] is insert-if-absent on the outpoint
//! (D1 `INSERT OR IGNORE` on the `(txid, outputIndex)` primary key) — rows
//! are NEVER deleted (a hop-in-flight fact is permanent recovery history;
//! the OP_RETURN is provably unspendable).
//!
//! `created_at` is assigned by the STORAGE layer at insert (D1 stamps the
//! unix time, the memory impl an insertion counter) — the value on the
//! record passed to `store_record` is ignored. `list_for_identity` is
//! newest-HOP-first over at most [`HOPSFOR_ROWS_PER_OUTPOINT`] rows per hop
//! outpoint; `list_for_hop` is OLDEST-first — the #281 dust-DoS bounds,
//! inherited verbatim from the potparty family.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Rows kept per hop OUTPOINT by the identity-scoped window — the SUPERSET
/// size (the #281 lesson, single group: hopparty has ONE version, so there
/// is no v1/v2 split).
///
/// # Why this is not 1
///
/// **Verification must happen BEFORE collapse — a layer that cannot verify
/// signatures must never choose which row is real.** Admission is
/// byte-format-only, so an attacker can file a hopparty marker naming any
/// identity + any hop outpoint for one dust OP_RETURN, and it can land with
/// an EARLIER `createdAt` than the honest marker (the hop txid is public
/// from broadcast). A per-outpoint window of 1 would hand the verifying
/// reader (`/hops-view`, the client) the forgery and erase the honest row —
/// the exact potparty rn=1 bug. The window bounds COST; the verifying
/// consumer decides truth. Same threshold arithmetic as
/// `PARTYFOR_ROWS_PER_GROUP` (4 = one honest marker + republish headroom;
/// eviction costs an attacker 4 earlier-stamped rows per hop — a
/// mitigation, not a closure, exactly as documented there).
pub const HOPSFOR_ROWS_PER_OUTPOINT: usize = 4;

/// A hopparty-marker record as stored in the index.
///
/// Keyed by the marker OUTPOINT `(txid, outputIndex)` — every admitted
/// marker is kept. Byte fields are carried back VERBATIM to querying
/// clients; the overlay never verifies either signature (the app-layer
/// `/hops-view` verifies at READ as a display filter; clients verify before
/// acting).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoppartyRecord {
    /// The publishing seat's compressed identity pubkey (33 bytes,
    /// lowercase hex).
    pub identity: String,
    /// The opponent seat's compressed identity pubkey (33 bytes, lowercase
    /// hex).
    #[serde(rename = "opponentIdentity")]
    pub opponent_identity: String,
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The hop output index within the marker's OWN CONTAINING TX. The hop
    /// outpoint is `(txid, hop_vout)` — there is no separate hop txid
    /// because the container IS the hop tx (see the module docs).
    #[serde(rename = "hopVout")]
    pub hop_vout: u32,
    /// The hop output's satoshi value (u64 — 8 bytes LE on the wire).
    #[serde(rename = "hopSats")]
    pub hop_sats: u64,
    /// The seat's `[2,'low settle']` pubkey (33 bytes, lowercase hex) — the
    /// key the hop output pays.
    #[serde(rename = "seatSettlePubkey")]
    pub seat_settle_pubkey: String,
    /// The settle key's DER signature over the hopparty seatsig preimage
    /// (lowercase hex) — carried verbatim, verified by READERS, never here.
    #[serde(rename = "seatSigHex")]
    pub seat_sig_hex: String,
    /// The identity's DER signature over the hopparty identity challenge
    /// (lowercase hex) — carried verbatim, verified by READERS, never here.
    #[serde(rename = "identitySigHex")]
    pub identity_sig_hex: String,
    // ── the CONTAINER's own facts, decoded ONCE at admission (#310) ──────
    // These are NOT claims: they are read out of the very transaction that
    // carries the marker, which the engine hands the lookup service in
    // whole-tx admission mode. Storing them typed at WRITE is what lets
    // the read path compare claim-vs-chain with no BLOB re-parse and no
    // dependency on a lifecycle-managed `outputs` row.
    /// The locking script (lowercase hex) of the containing tx's output at
    /// `hop_vout`. `None` IFF the container has no such output (provable
    /// from `container_outputs`), which REFUTES the marker.
    #[serde(rename = "hopLockHex")]
    pub hop_lock_hex: Option<String>,
    /// That output's satoshi value as the CHAIN records it. `None` in the
    /// same absent case as `hop_lock_hex`.
    #[serde(rename = "hopSatsOnChain")]
    pub hop_sats_on_chain: Option<u64>,
    /// How many outputs the containing tx has — makes "no output at
    /// `hop_vout`" a PROVEN absence rather than a NULL anyone must guess
    /// about (the unknown-vs-refuted distinction the reader depends on).
    #[serde(rename = "containerOutputs")]
    pub container_outputs: u32,
    // ── P4 slice 2 (2026-09-02): the container's OWN size + exact fee, read
    // once at admission from the admitted BEEF (`tx_facts`). Display tier —
    // the money views list them; nothing money-gating reads them. `None` on
    // pre-slice-2 rows and when the BEEF lacked an input's source tx.
    #[serde(rename = "sizeBytes", default)]
    pub size_bytes: Option<u64>,
    #[serde(rename = "feeSats", default)]
    pub fee_sats: Option<u64>,
    /// The txid carrying the marker OP_RETURN — half of the primary key,
    /// and (since the 2026-08-04 revision) the HOP TXID itself: the marker
    /// rides the hop transaction, so one txid names both.
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

/// `ls_hopparty` query shapes — tagged JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HoppartyQuery {
    /// "Which hops is this identity a party to?" — the hops-in-flight
    /// question. `limit` counts HOP OUTPOINTS; up to
    /// [`HOPSFOR_ROWS_PER_OUTPOINT`] rows per outpoint, newest outpoint
    /// first.
    #[serde(rename = "hopsFor")]
    HopsFor {
        identity: String,
        limit: Option<u32>,
    },
    /// "Which markers name this hop outpoint?" — oldest first (the honest
    /// marker rides the hop tx itself, so later dust can never spam its way
    /// in front of it — the #281 byPot rationale). `hopTxid` is the
    /// marker's CONTAINING txid (one tx).
    #[serde(rename = "byHop")]
    ByHop {
        #[serde(rename = "hopTxid")]
        hop_txid: String,
        #[serde(rename = "hopVout")]
        hop_vout: u32,
        limit: Option<u32>,
    },
}

/// Backend-agnostic storage for hopparty-marker records.
#[async_trait(?Send)]
pub trait HoppartyStorage {
    /// Store a record keyed by its OUTPOINT `(txid, outputIndex)` —
    /// insert-if-absent: a replayed / duplicate SUBMIT of the same output
    /// is a no-op, but markers for the same identity from DIFFERENT txs are
    /// ALL kept. Mirrors the D1 `INSERT OR IGNORE`. Never overwrites, never
    /// deletes. `created_at` is assigned here (the record's value is
    /// ignored).
    async fn store_record(&self, record: &HoppartyRecord) -> Result<(), HoppartyStorageError>;

    /// Records whose `identity` is `identity`, for up to `limit` HOP
    /// OUTPOINTS — newest outpoint first, a BOUNDED SUPERSET of up to
    /// [`HOPSFOR_ROWS_PER_OUTPOINT`] OLDEST rows per outpoint (see the
    /// constant's doc for why the window never collapses to one row: the
    /// verifying reader decides truth, this layer only bounds cost).
    ///
    /// A backend that can see the hop index (`pot_records`, written by
    /// `tm_lowfund`) SHOULD additionally sort rows naming a hop it has
    /// never indexed behind the rest, while still serving them — the D1
    /// implementation does; the in-memory one has no hop table and cannot.
    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<HoppartyRecord>, HoppartyStorageError>;

    /// Up to `limit` records naming the hop outpoint `(hop_txid,
    /// hop_vout)`, **oldest first** (the honest marker is published ON the
    /// hop tx itself, so oldest-first puts it permanently at the head of
    /// the window).
    async fn list_for_hop(
        &self,
        hop_txid: &str,
        hop_vout: u32,
        limit: usize,
    ) -> Result<Vec<HoppartyRecord>, HoppartyStorageError>;
}

/// HOPPARTY storage errors.
#[derive(Debug, thiserror::Error)]
pub enum HoppartyStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory HOPPARTY storage for testing. Insertion order IS recency order
/// (newest = last pushed); `created_at` is stamped with an insertion
/// counter so answers expose a monotone `createdAt` like D1's unix stamp.
#[derive(Debug, Default)]
pub struct MemoryHoppartyStorage {
    records: std::sync::Mutex<Vec<HoppartyRecord>>,
}

impl MemoryHoppartyStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl HoppartyStorage for MemoryHoppartyStorage {
    async fn store_record(&self, record: &HoppartyRecord) -> Result<(), HoppartyStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent on the OUTPOINT (txid, outputIndex) — matches
        // D1's INSERT OR IGNORE on the primary key.
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
    ) -> Result<Vec<HoppartyRecord>, HoppartyStorageError> {
        let records = self.records.lock().unwrap();
        // Per-outpoint SUPERSET: walking in INSERTION order (oldest first)
        // keeps the OLDEST [`HOPSFOR_ROWS_PER_OUTPOINT`] rows per hop
        // outpoint — the only order an attacker cannot improve on by
        // publishing MORE later (it bounds cost without picking truth).
        let mut per_outpoint: std::collections::HashMap<(&str, u32), usize> =
            std::collections::HashMap::new();
        let mut kept: Vec<&HoppartyRecord> = Vec::new();
        for r in records.iter().filter(|r| r.identity == identity) {
            // The hop outpoint is (containing txid, hopVout).
            let n = per_outpoint
                .entry((r.txid.as_str(), r.hop_vout))
                .or_insert(0);
            if *n < HOPSFOR_ROWS_PER_OUTPOINT {
                *n += 1;
                kept.push(r);
            }
        }
        // `limit` counts HOP OUTPOINTS: take that many distinct outpoints
        // newest-first (by each outpoint's newest kept row) and keep every
        // kept row belonging to them. The in-memory backend has no hop
        // table, so it cannot apply the D1 window's existence tier
        // (documented on the trait).
        let mut newest_of: std::collections::HashMap<(&str, u32), i64> =
            std::collections::HashMap::new();
        for r in &kept {
            let e = newest_of
                .entry((r.txid.as_str(), r.hop_vout))
                .or_insert(i64::MIN);
            *e = (*e).max(r.created_at);
        }
        let mut outpoints: Vec<((&str, u32), i64)> = newest_of.into_iter().collect();
        outpoints.sort_by_key(|(_, newest)| std::cmp::Reverse(*newest));
        outpoints.truncate(limit);
        let allowed: std::collections::HashSet<(&str, u32)> =
            outpoints.iter().map(|(k, _)| *k).collect();
        let mut out: Vec<HoppartyRecord> = kept
            .into_iter()
            .filter(|r| allowed.contains(&(r.txid.as_str(), r.hop_vout)))
            .cloned()
            .collect();
        // Newest OUTPOINT first; within an outpoint oldest marker first
        // (mirrors the D1 ORDER BY).
        let rank: std::collections::HashMap<(String, u32), usize> = outpoints
            .iter()
            .enumerate()
            .map(|(i, ((t, v), _))| ((t.to_string(), *v), i))
            .collect();
        out.sort_by_key(|r| {
            (
                *rank
                    .get(&(r.txid.clone(), r.hop_vout))
                    .unwrap_or(&usize::MAX),
                r.created_at,
            )
        });
        Ok(out)
    }

    async fn list_for_hop(
        &self,
        hop_txid: &str,
        hop_vout: u32,
        limit: usize,
    ) -> Result<Vec<HoppartyRecord>, HoppartyStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter() // OLDEST first — insertion order
            .filter(|r| r.txid == hop_txid && r.hop_vout == hop_vout)
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

    fn record(identity: &str, opponent: &str, txid: &str) -> HoppartyRecord {
        let settle = format!("03{}", "c4".repeat(32));
        HoppartyRecord {
            identity: identity.into(),
            opponent_identity: opponent.into(),
            game_id: "11".repeat(32),
            hop_vout: 0,
            hop_sats: 80_800,
            seat_settle_pubkey: settle,
            seat_sig_hex: "3045ab".into(),
            identity_sig_hex: "3045cd".into(),
            hop_lock_hex: Some(format!("76a914{}88ac", "d4".repeat(20))),
            hop_sats_on_chain: Some(80_800),
            container_outputs: 2,
            size_bytes: None,
            fee_sats: None,
            txid: txid.into(),
            output_index: 0,
            created_at: 0, // ignored — storage assigns
        }
    }

    #[tokio::test]
    async fn store_then_list_roundtrips() {
        let store = MemoryHoppartyStorage::new();
        store
            .store_record(&record("02aa", "03bb", "tx1"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);

        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        assert_eq!(rows[0].opponent_identity, "03bb");
        assert_eq!(rows[0].hop_sats, 80_800);
        assert_eq!(rows[0].seat_settle_pubkey, format!("03{}", "c4".repeat(32)));
        // The container's decoded facts survive the round-trip.
        assert_eq!(rows[0].hop_sats_on_chain, Some(80_800));
        assert_eq!(rows[0].container_outputs, 2);
    }

    #[tokio::test]
    async fn list_for_identity_filters_by_identity_only() {
        let store = MemoryHoppartyStorage::new();
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
    async fn list_for_hop_filters_the_exact_outpoint() {
        let store = MemoryHoppartyStorage::new();
        // Two markers on ONE container (hop tx): one naming vout 0, one
        // naming vout 1 — different hop outpoints of the same tx.
        store
            .store_record(&record("02aa", "03bb", "txA"))
            .await
            .unwrap();
        let mut other_vout = record("02aa", "03bb", "txA");
        other_vout.hop_vout = 1;
        other_vout.output_index = 2; // a distinct marker outpoint (the PK)
        store.store_record(&other_vout).await.unwrap();
        // A marker on a DIFFERENT container.
        store
            .store_record(&record("03bb", "02aa", "txB"))
            .await
            .unwrap();

        let rows = store.list_for_hop("txA", 0, 100).await.unwrap();
        assert_eq!(rows.len(), 1, "exact outpoint (txid, vout), not txid alone");
        assert_eq!(rows[0].hop_vout, 0);
        assert_eq!(store.list_for_hop("txA", 1, 100).await.unwrap().len(), 1);
        assert!(store.list_for_hop("txA", 9, 100).await.unwrap().is_empty());
        assert_eq!(store.list_for_hop("txB", 0, 100).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn list_for_identity_is_newest_outpoint_first_and_respects_limit() {
        let store = MemoryHoppartyStorage::new();
        for i in 0..5u8 {
            // Each marker rides its own hop tx ⇒ its own hop outpoint.
            store
                .store_record(&record("02aa", "03bb", &format!("tx{i}")))
                .await
                .unwrap();
        }
        let rows = store.list_for_identity("02aa", 3).await.unwrap();
        assert_eq!(rows.len(), 3, "limit counts outpoints");
        assert_eq!(rows[0].txid, "tx4", "newest outpoint first");
        assert!(rows[0].created_at > rows[1].created_at);
    }

    /// The #281 bound at the trait's own level: many markers naming ONE hop
    /// outpoint occupy ONE outpoint slot (bounded superset), so they cannot
    /// crowd a victim's other hops out of the window — and the honest
    /// OLDEST marker survives inside the superset.
    #[tokio::test]
    async fn many_markers_for_one_hop_consume_one_slot_and_keep_the_oldest() {
        let store = MemoryHoppartyStorage::new();
        // The victim's honest marker on its real hop tx, published FIRST.
        store
            .store_record(&record("02aa", "03bb", "txHONEST"))
            .await
            .unwrap();
        // 120 later markers on ONE attacker container (one hop outpoint),
        // each a distinct marker outpoint via outputIndex.
        for i in 0..120u32 {
            let mut junk = record("02aa", "03cc", "txJUNK");
            junk.output_index = i + 1;
            store.store_record(&junk).await.unwrap();
        }
        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        // 2 outpoints: the junk one contributes at most the superset, the
        // honest one its single row.
        assert_eq!(rows.len(), 1 + HOPSFOR_ROWS_PER_OUTPOINT);
        assert!(
            rows.iter().any(|r| r.txid == "txHONEST"),
            "the honest hop survives the flood"
        );
        // Within the junk outpoint, the OLDEST rows were kept.
        assert!(rows.iter().any(|r| r.output_index == 1));
        assert!(!rows.iter().any(|r| r.output_index == 120));
    }

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let store = MemoryHoppartyStorage::new();
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
        let q: HoppartyQuery = serde_json::from_value(serde_json::json!({
            "type": "hopsFor",
            "identity": "02".to_string() + &"a1".repeat(32),
            "limit": 50
        }))
        .unwrap();
        match q {
            HoppartyQuery::HopsFor { identity, limit } => {
                assert_eq!(identity.len(), 66);
                assert_eq!(limit, Some(50));
            }
            other => panic!("expected HopsFor, got {other:?}"),
        }

        let q: HoppartyQuery = serde_json::from_value(serde_json::json!({
            "type": "byHop",
            "hopTxid": "22".repeat(32),
            "hopVout": 3
        }))
        .unwrap();
        match q {
            HoppartyQuery::ByHop {
                hop_txid,
                hop_vout,
                limit,
            } => {
                assert_eq!(hop_txid.len(), 64);
                assert_eq!(hop_vout, 3);
                assert_eq!(limit, None);
            }
            other => panic!("expected ByHop, got {other:?}"),
        }

        assert!(
            serde_json::from_value::<HoppartyQuery>(serde_json::json!({"type": "partyFor"}))
                .is_err(),
            "a potparty query shape is not a hopparty query"
        );
    }
}
