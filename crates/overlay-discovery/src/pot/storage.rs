//! POT Storage trait — backend-agnostic storage for pot-spend records.
//!
//! One row per admitted `tm_pot` covenant UTXO (`pot_records` in D1). The
//! concrete implementation (D1, in-memory) is provided by the deployment
//! crate; [`MemoryPotStorage`] here backs the unit tests.
//!
//! # The ONE difference from `reveal` storage
//!
//! A reveal record is write-once (admit, then never touched). A pot record
//! is written on admission (`spent = false`) and UPDATED on spend (`spent =
//! true` + the `spendingTxid`). Records are NEVER deleted — a spent pot is
//! the permanent landing proof a client asks for. Two invariants make this
//! safe under replay / out-of-order delivery:
//!
//! - [`PotStorage::store_record`] inserts only if the outpoint is absent; it
//!   NEVER clobbers a spent row back to unspent (a re-admission of an
//!   already-recorded spend must not erase the spender).
//! - [`PotStorage::mark_spent`] updates an existing row only (mirrors the D1
//!   `UPDATE ... WHERE`); an outpoint must be admitted before it can be
//!   marked spent.
//! - **Prefer-confirmed / never-clobber-with-unconfirmed** (the `/submit`
//!   surface is PUBLIC and `historical-tx-no-spv` skips SPV, so an arbitrary
//!   submitter can claim to spend a pot): a spend marked `confirmed` (SPV
//!   verified against a pinned chain tracker) ALWAYS wins; an UNCONFIRMED
//!   spend claim can never overwrite a confirmed pointer. Last-writer-wins
//!   among unconfirmed claims is deliberately preserved so an honest later
//!   submit can still set the pointer.
//!
//! # The BEEF store (`pot_beefs`)
//!
//! Alongside the spend records, this trait durably stores the BEEF of every
//! pot funding AND every pot-spending (settle/refund/sweep) tx, keyed by that
//! tx's own txid. It exists because the engine's `transactions` table is
//! LIFECYCLE-MANAGED: a BEEF row is only written by `insert_output` (a
//! settle, which admits no outputs, never gets one) and is DELETED by the
//! deep-delete when a spent unretained coin is cleaned up. `pot_beefs` is
//! OURS — never deleted — and is the durable source `low-app-layer`'s
//! `/beef/:txid` serves.
//!
//! Store rule (the "vanishing table" lesson — see the engine's
//! `insert_output` BEEF upsert): [`PotStorage::store_beef`] NEVER overwrites
//! an existing row with a shorter/empty beef — it writes only when no row
//! exists or the new beef is LONGER.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A pot-spend record as stored in the index.
///
/// Keyed by `(txid, outputIndex)` = the pot funding outpoint. `spent` /
/// `spending_txid` carry the landing proof once the settle/refund/sweep is
/// seen by the engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PotRecord {
    /// The pot funding txid (the SPENT output's txid).
    pub txid: String,
    /// The pot vout (the SPENT output's index).
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Whether the pot output has been spent (a spender tx was seen).
    pub spent: bool,
    /// The txid that spent the pot (the settle / refund / sweep). `None`
    /// until the spend is recorded.
    #[serde(rename = "spendingTxid")]
    pub spending_txid: Option<String>,
    /// Whether the recorded spend was SPV-CONFIRMED (the spending tx carried
    /// a merkle path whose root the chain tracker validated) when it was
    /// recorded. A confirmed pointer is chain truth: an unconfirmed claim can
    /// never overwrite it (see [`PotStorage::mark_spent`]). `serde(default)`
    /// keeps pre-upgrade rows/payloads readable (absent → `false`).
    #[serde(rename = "spentConfirmed", default)]
    pub spent_confirmed: bool,
}

/// One outpoint in a `spentStatus` query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutpointJson {
    pub txid: String,
    pub vout: u32,
}

/// `ls_pot` query shapes — tagged JSON, e.g.
/// `{"type":"spentStatus","outpoints":[{"txid":"<hex>","vout":0}]}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PotQuery {
    /// Ask the spent status of a batch of pot outpoints. The answer is an
    /// input-ordered array, one entry per requested outpoint.
    #[serde(rename = "spentStatus")]
    SpentStatus { outpoints: Vec<OutpointJson> },
}

/// Backend-agnostic storage for pot-spend records.
#[async_trait(?Send)]
pub trait PotStorage {
    /// Record an admitted pot outpoint (called with `spent = false`).
    ///
    /// Insert-if-absent: if a row for `(txid, outputIndex)` already exists it
    /// is left untouched — in particular a row already marked spent is NOT
    /// clobbered back to unspent. Mirrors the D1 `INSERT OR IGNORE`.
    async fn store_record(&self, record: &PotRecord) -> Result<(), PotStorageError>;

    /// Mark an admitted outpoint spent by `spending_txid`.
    ///
    /// Prefer-confirmed / never-clobber-with-unconfirmed semantics:
    ///
    /// - `confirmed == true` → ALWAYS write: `spent = true`,
    ///   `spending_txid = <new>`, `spent_confirmed = true`. A confirmed
    ///   spend is chain truth; last-confirmed-wins.
    /// - `confirmed == false` → write `spent = true`,
    ///   `spending_txid = <new>` ONLY IF the existing row has
    ///   `spent_confirmed = false`. An unconfirmed claim must NEVER clobber
    ///   a confirmed pointer; last-writer-wins among unconfirmed claims is
    ///   deliberately preserved so an honest later submit can still set the
    ///   pointer. `spent_confirmed` is never touched in this branch.
    ///
    /// Still UPDATE-only (mirrors D1 `UPDATE ... WHERE`): a nonexistent
    /// outpoint is a no-op (an output must be admitted before it can be
    /// spent). Never deletes.
    async fn mark_spent(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        confirmed: bool,
    ) -> Result<(), PotStorageError>;

    /// The record for an outpoint, or `None` if we never admitted it.
    async fn get_spent_status(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Option<PotRecord>, PotStorageError>;

    /// Spent-but-UNCONFIRMED pot records — the spend-confirmation chaser's
    /// candidate set (#186).
    ///
    /// LOW settles submit 0-conf (no merkle bump at submit time), so the spend
    /// is recorded `spent = true, spentConfirmed = false` and NOTHING upgrades
    /// it (the cron does ad-sync/GASP only). This surfaces those rows so a
    /// bounded completion pass can fetch+chaintracks-verify the SPENDING tx's
    /// bump and latch `spentConfirmed` via [`mark_spent`](Self::mark_spent) with
    /// `confirmed = true`.
    ///
    /// Backends that enumerate answer with
    /// `WHERE spent = 1 AND spentConfirmed = 0 ORDER BY RANDOM() LIMIT n`
    /// (RANDOM defeats head-of-queue starvation — the same shape as
    /// [`find_pot_beefs_for_proof_check`](Self::find_pot_beefs_for_proof_check)).
    /// Every returned row carries a `spending_txid` (a spent row always has
    /// one). Backends that can't enumerate return an empty `Vec` via this
    /// default → the chaser is a no-op.
    ///
    /// `min_age_secs` is the PUSH-PRIMARY BACKSTOP gate (bsv-low #228 /
    /// arcade#259): rows whose spend was recorded less than `min_age_secs`
    /// ago are EXCLUDED — the spending tx's proof is expected via the Arcade
    /// MINED webhook (`/arc-ingest`, which latches `spentConfirmed` directly).
    /// `0` disables the gate; a row whose spend-record time is UNKNOWN
    /// (pre-migration `NULL`) MUST be treated as old/eligible — the fail-safe
    /// direction is to poll MORE, never to starve a row of its backstop. The
    /// D1 backend anchors the age on a `spentAt` stamp written by
    /// [`mark_spent`](Self::mark_spent).
    async fn find_spent_unconfirmed(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        let _ = (limit, min_age_secs);
        Ok(Vec::new())
    }

    /// Spent-but-UNCONFIRMED pot records whose recorded spender is
    /// `spending_txid` — the PUSH consumer's lookup (bsv-low #228): when
    /// `/arc-ingest` receives (and chaintracks-verifies) the merkle proof for
    /// a settle/refund/sweep tx, it confirms every pot outpoint that spend
    /// covers via [`mark_spent`](Self::mark_spent)`(confirmed = true)`, so the
    /// #186 poll chaser skips them entirely. Backends that can't enumerate
    /// return an empty `Vec` via this default → the push pass is a no-op and
    /// the poll backstop still covers the row.
    async fn find_unconfirmed_by_spending_txid(
        &self,
        spending_txid: &str,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        let _ = spending_txid;
        Ok(Vec::new())
    }

    /// Durably store `beef` under `txid` (the stored tx's OWN txid — the
    /// funding txid for a funding beef, the SETTLE txid for a settle beef).
    ///
    /// Longer-wins, never-clobber (the "vanishing table" lesson): the write
    /// happens only when no row exists or the new beef is strictly LONGER
    /// than the stored one; an empty `beef` is rejected (no-op). A good row
    /// is therefore never replaced by a shorter/empty one.
    async fn store_beef(&self, txid: &str, beef: &[u8]) -> Result<(), PotStorageError>;

    /// The stored BEEF for `txid`, or `None` if we never stored one.
    async fn get_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, PotStorageError>;

    /// Return a bounded page of PROOFLESS stored pot BEEFs (`(txid, beef)`) for
    /// the proof-completion cron (#192/#193). A row is "proofless" when its
    /// stored BEEF does NOT yet carry a chaintracks-verified merkle BUMP for its
    /// OWN txid.
    ///
    /// Backends that track a `has_proof` flag answer with
    /// `WHERE has_proof = 0 ORDER BY RANDOM() LIMIT n` (RANDOM defeats
    /// head-of-queue starvation — a never-mineable head must not starve the
    /// tail). Backends that can't enumerate (or have nothing to complete) may
    /// return an empty `Vec` via this default → proof completion is a no-op.
    ///
    /// `min_age_secs` is the PUSH-PRIMARY BACKSTOP gate (bsv-low #228 /
    /// arcade#259): rows stored less than `min_age_secs` ago are EXCLUDED —
    /// their proof is expected via `/arc-ingest` (which stitches + compacts
    /// the pot BEEF directly). `0` disables the gate; unknown-age rows MUST
    /// stay eligible (fail-safe). The D1 backend anchors on `createdAt`.
    async fn find_pot_beefs_for_proof_check(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<(String, Vec<u8>)>, PotStorageError> {
        let _ = (limit, min_age_secs);
        Ok(Vec::new())
    }

    /// Overwrite the stored BEEF for `txid` with a PROOF-BEARING `new_beef`,
    /// BYPASSING the longer-wins guard of [`store_beef`](Self::store_beef) — a
    /// bumped BEEF is authoritative even when SHORTER (its proven ancestry has
    /// been trimmed). The write happens ONLY when `new_beef` actually proves
    /// `txid` (its own BUMP is present, which also guarantees self-containment —
    /// `find_txid(txid)` is `Some`); otherwise it is a NO-OP (fail-closed).
    /// Backends that don't compact may use this no-op default.
    async fn compact_pot_beef(&self, txid: &str, new_beef: &[u8]) -> Result<(), PotStorageError> {
        let _ = (txid, new_beef);
        Ok(())
    }
}

/// Whether `beef` carries a merkle proof for `txid`'s OWN tx (not an
/// ancestor's). Unparseable/absent → `false` (treated as proofless / a compact
/// no-op — fail-closed). Shared by the in-memory and D1 pot stores so the
/// candidate query and the compaction write agree on "proven".
pub fn pot_beef_has_proof(txid: &str, beef: &[u8]) -> bool {
    bsv_rs::transaction::Beef::from_binary(beef)
        .ok()
        .and_then(|b| {
            b.find_txid(txid)
                .map(bsv_rs::transaction::BeefTx::has_proof)
        })
        .unwrap_or(false)
}

/// POT storage errors.
#[derive(Debug, thiserror::Error)]
pub enum PotStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory POT storage for testing.
#[derive(Debug, Default)]
pub struct MemoryPotStorage {
    records: std::sync::Mutex<Vec<PotRecord>>,
    beefs: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
    /// Deterministic logical clock (seconds) for the push-primary backstop
    /// age gates (#228) — models the D1 backend's `unixepoch()`. Tests
    /// advance it via [`Self::advance_clock`]; no wall clock is ever read.
    clock_secs: std::sync::Mutex<u64>,
    /// First-store stamp (clock secs) per beef txid — models `pot_beefs.createdAt`.
    beef_created_at: std::sync::Mutex<std::collections::HashMap<String, u64>>,
    /// Spend-record stamp (clock secs) per `(txid, vout)` — models
    /// `pot_records.spentAt` (written by `mark_spent`).
    spent_at: std::sync::Mutex<std::collections::HashMap<(String, u32), u64>>,
}

impl MemoryPotStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }

    pub fn beef_count(&self) -> usize {
        self.beefs.lock().unwrap().len()
    }

    /// Advance the deterministic logical clock by `secs` (test hook for the
    /// #228 push-primary backstop age gates).
    pub fn advance_clock(&self, secs: u64) {
        *self.clock_secs.lock().unwrap() += secs;
    }

    fn now(&self) -> u64 {
        *self.clock_secs.lock().unwrap()
    }

    /// Whether a stamp clears the age gate: unknown (None) is OLD/eligible
    /// (fail-safe); otherwise `clock - stamp >= min_age_secs`.
    fn gate_open(&self, stamp: Option<u64>, min_age_secs: u64) -> bool {
        if min_age_secs == 0 {
            return true;
        }
        match stamp {
            None => true,
            Some(s) => self.now().saturating_sub(s) >= min_age_secs,
        }
    }
}

#[async_trait(?Send)]
impl PotStorage for MemoryPotStorage {
    async fn store_record(&self, record: &PotRecord) -> Result<(), PotStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent: an existing row (spent or not) is never clobbered.
        let exists = records
            .iter()
            .any(|r| r.txid == record.txid && r.output_index == record.output_index);
        if !exists {
            records.push(record.clone());
        }
        Ok(())
    }

    async fn mark_spent(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        confirmed: bool,
    ) -> Result<(), PotStorageError> {
        let now = self.now();
        let mut records = self.records.lock().unwrap();
        // UPDATE-only: touch an existing row; absent outpoint is a no-op.
        for r in records.iter_mut() {
            if r.txid == txid && r.output_index == output_index {
                let wrote = if confirmed {
                    // Chain truth: always write, latch spent_confirmed
                    // (last-confirmed-wins).
                    r.spent = true;
                    r.spending_txid = Some(spending_txid.to_string());
                    r.spent_confirmed = true;
                    true
                } else if !r.spent_confirmed {
                    // Unconfirmed claim: only allowed while no confirmed
                    // pointer exists (last-writer among unconfirmed);
                    // spent_confirmed is never touched here.
                    r.spent = true;
                    r.spending_txid = Some(spending_txid.to_string());
                    true
                } else {
                    // Unconfirmed claim vs confirmed pointer → REFUSED.
                    false
                };
                // Stamp the spend-record time on every accepted write (#228
                // backstop age anchor): a NEW spend pointer resets the clock
                // so its own push gets its chance before the poll backstop.
                if wrote {
                    self.spent_at
                        .lock()
                        .unwrap()
                        .insert((txid.to_string(), output_index), now);
                }
            }
        }
        Ok(())
    }

    async fn get_spent_status(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Option<PotRecord>, PotStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.txid == txid && r.output_index == output_index)
            .cloned())
    }

    async fn store_beef(&self, txid: &str, beef: &[u8]) -> Result<(), PotStorageError> {
        // Empty is rejected — never store unusable bytes.
        if beef.is_empty() {
            return Ok(());
        }
        let now = self.now();
        let mut beefs = self.beefs.lock().unwrap();
        // Longer-wins: write only when absent or strictly longer (a good row
        // is never clobbered by a shorter one).
        match beefs.get(txid) {
            Some(existing) if existing.len() >= beef.len() => {}
            _ => {
                beefs.insert(txid.to_string(), beef.to_vec());
                // First-store stamp only (#228 age anchor): a longer-beef
                // rewrite keeps the original age real.
                self.beef_created_at
                    .lock()
                    .unwrap()
                    .entry(txid.to_string())
                    .or_insert(now);
            }
        }
        Ok(())
    }

    async fn get_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, PotStorageError> {
        Ok(self.beefs.lock().unwrap().get(txid).cloned())
    }

    async fn find_pot_beefs_for_proof_check(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<(String, Vec<u8>)>, PotStorageError> {
        // Model the D1 `WHERE has_proof = 0` candidate set by re-deriving the
        // flag from the stored bytes (the memory store keeps no flag column).
        // The #228 backstop age gate excludes rows younger than min_age_secs
        // (their proof is expected via /arc-ingest); unknown age = eligible.
        let candidates: Vec<(String, Vec<u8>)> = self
            .beefs
            .lock()
            .unwrap()
            .iter()
            .filter(|(txid, beef)| !pot_beef_has_proof(txid, beef))
            .map(|(txid, beef)| (txid.clone(), beef.clone()))
            .collect();
        Ok(candidates
            .into_iter()
            .filter(|(txid, _)| {
                let stamp = self.beef_created_at.lock().unwrap().get(txid).copied();
                self.gate_open(stamp, min_age_secs)
            })
            .take(limit as usize)
            .collect())
    }

    async fn find_spent_unconfirmed(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        // Spent rows still awaiting SPV confirmation. The D1 store carries the
        // anti-starvation `ORDER BY RANDOM()`; the memory store need not
        // randomize (tests are deterministic). The #228 backstop age gate
        // excludes rows whose spend was recorded less than min_age_secs ago
        // (the spending tx's push is still expected); unknown age = eligible.
        let candidates: Vec<PotRecord> = self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.spent && !r.spent_confirmed)
            .cloned()
            .collect();
        Ok(candidates
            .into_iter()
            .filter(|r| {
                let stamp = self
                    .spent_at
                    .lock()
                    .unwrap()
                    .get(&(r.txid.clone(), r.output_index))
                    .copied();
                self.gate_open(stamp, min_age_secs)
            })
            .take(limit as usize)
            .collect())
    }

    async fn find_unconfirmed_by_spending_txid(
        &self,
        spending_txid: &str,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| {
                r.spent
                    && !r.spent_confirmed
                    && r.spending_txid.as_deref() == Some(spending_txid)
            })
            .cloned()
            .collect())
    }

    async fn compact_pot_beef(&self, txid: &str, new_beef: &[u8]) -> Result<(), PotStorageError> {
        // Fail-closed: overwrite ONLY when the new beef actually proves txid
        // (its own BUMP is present ⇒ self-contained). BYPASS the longer-wins
        // guard — a bumped BEEF wins even when shorter.
        if !pot_beef_has_proof(txid, new_beef) {
            return Ok(());
        }
        self.beefs
            .lock()
            .unwrap()
            .insert(txid.to_string(), new_beef.to_vec());
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn pot_record(txid: &str, vout: u32) -> PotRecord {
        PotRecord {
            txid: txid.into(),
            output_index: vout,
            spent: false,
            spending_txid: None,
            spent_confirmed: false,
        }
    }

    #[tokio::test]
    async fn store_then_get_returns_unspent_record() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        assert_eq!(store.record_count(), 1);

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(!r.spent);
        assert_eq!(r.spending_txid, None);
    }

    #[tokio::test]
    async fn get_unknown_outpoint_is_none() {
        let store = MemoryPotStorage::new();
        assert!(store.get_spent_status("nope", 0).await.unwrap().is_none());
        // A different vout of a stored txid is still unknown.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        assert!(store.get_spent_status("potA", 1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mark_spent_sets_spender() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settleTx", false).await.unwrap();

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent);
        assert_eq!(r.spending_txid.as_deref(), Some("settleTx"));
        assert!(!r.spent_confirmed, "unconfirmed spend must not latch the flag");
        // No new row was created.
        assert_eq!(store.record_count(), 1);
    }

    #[tokio::test]
    async fn store_is_idempotent_per_outpoint() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        assert_eq!(store.record_count(), 1);
    }

    #[tokio::test]
    async fn store_never_clobbers_a_spent_row_back_to_unspent() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settleTx", false).await.unwrap();

        // A re-admission (e.g. GASP replay) must NOT erase the spender.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent, "spent status must survive re-admission");
        assert_eq!(r.spending_txid.as_deref(), Some("settleTx"));
        assert_eq!(store.record_count(), 1);
    }

    #[tokio::test]
    async fn mark_spent_on_unknown_outpoint_is_noop() {
        let store = MemoryPotStorage::new();
        // No admission first → mark_spent creates nothing (mirrors D1 UPDATE),
        // whether confirmed or not.
        store.mark_spent("ghost", 0, "settleTx", false).await.unwrap();
        store.mark_spent("ghost", 0, "settleTx", true).await.unwrap();
        assert_eq!(store.record_count(), 0);
        assert!(store.get_spent_status("ghost", 0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn distinct_outpoints_tracked_independently() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.store_record(&pot_record("potB", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settleA", false).await.unwrap();

        let a = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        let b = store.get_spent_status("potB", 0).await.unwrap().unwrap();
        assert!(a.spent);
        assert!(!b.spent, "spending potA must not affect potB");
    }

    // ── Prefer-confirmed / never-clobber-with-unconfirmed matrix ─────────

    #[tokio::test]
    async fn unconfirmed_overwrites_unconfirmed_last_writer_wins() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();

        // First unconfirmed claim on an unspent row → recorded.
        store.mark_spent("potA", 0, "claim1", false).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent);
        assert_eq!(r.spending_txid.as_deref(), Some("claim1"));
        assert!(!r.spent_confirmed);

        // A second unconfirmed claim by a DIFFERENT spender overwrites —
        // last-writer-wins among unconfirmed is deliberately preserved so an
        // honest later submit can still set the pointer.
        store.mark_spent("potA", 0, "claim2", false).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("claim2"));
        assert!(!r.spent_confirmed);
    }

    #[tokio::test]
    async fn confirmed_spend_latches_flag() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settleTx", true).await.unwrap();

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent);
        assert_eq!(r.spending_txid.as_deref(), Some("settleTx"));
        assert!(r.spent_confirmed);
    }

    #[tokio::test]
    async fn unconfirmed_never_clobbers_confirmed_pointer() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "realSettle", true).await.unwrap();

        // An attacker's unconfirmed claim must be REFUSED: pointer AND flag
        // unchanged.
        store.mark_spent("potA", 0, "forgedSpend", false).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent);
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("realSettle"),
            "unconfirmed claim must never clobber a confirmed pointer"
        );
        assert!(r.spent_confirmed, "the confirmed flag must survive");
    }

    #[tokio::test]
    async fn confirmed_overwrites_confirmed_last_confirmed_wins() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settle1", true).await.unwrap();

        // A later CONFIRMED spend (e.g. reorg / better proof) still writes —
        // chain truth is last-confirmed-wins.
        store.mark_spent("potA", 0, "settle2", true).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("settle2"));
        assert!(r.spent_confirmed);
    }

    #[tokio::test]
    async fn confirmed_overwrites_unconfirmed_claim() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "unconfirmedClaim", false).await.unwrap();

        // The confirmed spend replaces the unconfirmed pointer and latches
        // the flag.
        store.mark_spent("potA", 0, "realSettle", true).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("realSettle"));
        assert!(r.spent_confirmed);
    }

    #[tokio::test]
    async fn store_never_clobbers_confirmed_flag_on_readmission() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settleTx", true).await.unwrap();

        // A re-admission (GASP replay) must not erase the confirmed flag.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent);
        assert!(r.spent_confirmed);
        assert_eq!(r.spending_txid.as_deref(), Some("settleTx"));
    }

    // ── BEEF store ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn store_beef_then_get_roundtrips() {
        let store = MemoryPotStorage::new();
        store.store_beef("fundingTx", &[1, 2, 3]).await.unwrap();
        assert_eq!(store.beef_count(), 1);
        assert_eq!(
            store.get_beef("fundingTx").await.unwrap().as_deref(),
            Some(&[1u8, 2, 3][..])
        );
        // A txid we never stored is None.
        assert!(store.get_beef("ghost").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn store_beef_longer_wins() {
        let store = MemoryPotStorage::new();
        store.store_beef("tx", &[1, 2]).await.unwrap();
        // A strictly longer beef replaces the stored one (re-hydration).
        store.store_beef("tx", &[9, 9, 9, 9]).await.unwrap();
        assert_eq!(
            store.get_beef("tx").await.unwrap().as_deref(),
            Some(&[9u8, 9, 9, 9][..])
        );
    }

    #[tokio::test]
    async fn store_beef_shorter_never_clobbers() {
        let store = MemoryPotStorage::new();
        store.store_beef("tx", &[1, 2, 3, 4]).await.unwrap();
        // Shorter must NOT clobber (the "vanishing table" lesson)…
        store.store_beef("tx", &[7]).await.unwrap();
        // …and equal-length must not either (write only when strictly longer).
        store.store_beef("tx", &[7, 7, 7, 7]).await.unwrap();
        assert_eq!(
            store.get_beef("tx").await.unwrap().as_deref(),
            Some(&[1u8, 2, 3, 4][..])
        );
    }

    #[tokio::test]
    async fn store_beef_empty_rejected() {
        let store = MemoryPotStorage::new();
        // Empty on a fresh key stores nothing…
        store.store_beef("tx", &[]).await.unwrap();
        assert_eq!(store.beef_count(), 0);
        assert!(store.get_beef("tx").await.unwrap().is_none());
        // …and empty never erases a good row.
        store.store_beef("tx", &[1, 2, 3]).await.unwrap();
        store.store_beef("tx", &[]).await.unwrap();
        assert_eq!(
            store.get_beef("tx").await.unwrap().as_deref(),
            Some(&[1u8, 2, 3][..])
        );
    }

    #[tokio::test]
    async fn store_beef_distinct_txids_independent() {
        let store = MemoryPotStorage::new();
        store.store_beef("funding", &[1]).await.unwrap();
        store.store_beef("settle", &[2, 2]).await.unwrap();
        assert_eq!(store.beef_count(), 2);
        assert_eq!(store.get_beef("funding").await.unwrap().as_deref(), Some(&[1u8][..]));
        assert_eq!(store.get_beef("settle").await.unwrap().as_deref(), Some(&[2u8, 2][..]));
    }

    #[test]
    fn record_deserializes_without_spent_confirmed_field() {
        // Backward-compat: a pre-upgrade payload without `spentConfirmed`
        // still deserializes (serde default → false).
        let r: PotRecord = serde_json::from_value(serde_json::json!({
            "txid": "potA", "outputIndex": 0, "spent": true, "spendingTxid": "settleTx"
        }))
        .unwrap();
        assert!(!r.spent_confirmed);
    }

    // ── compact_pot_beef (#192/#193 FIX 5) ───────────────────────────────

    /// Two distinct valid mainnet raw txs, used to build real BEEF fixtures.
    const RAW_A: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";
    const RAW_B: &str = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff2803dc7e0e0499170e6a0003cf341b017e0000152f476f72696c6c61506f6f6c2e696f20f09fa68d2f0000000003000000000000000032006a0547504f4f4c08dc7e0e0000000000200158a2360a03939451e72c3a9302f5d48712bf54a5b2edf8f3c69aed35a668e312236000000000001976a914068a58835bb93b152c901ffb18f6578824f9d5b788ac6eb66612000000001976a91402fd5a91155231d5799e2d22c490d1664cde62cb88ac00000000";

    /// A PROOFLESS BEEF carrying `raw` (+ optional filler ancestor to make it
    /// longer than a trimmed proven BEEF). Returns `(beef_bytes, subject_txid)`.
    fn proofless_beef_with_filler(raw: &str, filler: Option<&str>) -> (Vec<u8>, String) {
        use bsv_rs::transaction::{Beef, Transaction};
        let tx = Transaction::from_hex(raw).unwrap();
        let txid = tx.id();
        let mut beef = Beef::new();
        if let Some(f) = filler {
            beef.merge_transaction(Transaction::from_hex(f).unwrap());
        }
        beef.merge_transaction(tx);
        (beef.to_binary(), txid)
    }

    /// A PROVEN (single-leaf bump), trimmed BEEF for `raw`. Returns
    /// `(beef_bytes, subject_txid)`. `pot_beef_has_proof(txid, beef)` is `true`.
    fn proven_beef(raw: &str) -> (Vec<u8>, String) {
        use bsv_rs::transaction::{MerklePath, MerklePathLeaf, Transaction};
        let mut tx = Transaction::from_hex(raw).unwrap();
        let txid = tx.id();
        let bump = MerklePath::new(800_000, vec![vec![MerklePathLeaf::new_txid(0, txid.clone())]])
            .expect("valid single-leaf merkle path");
        tx.merkle_path = Some(bump);
        (tx.to_beef(true).unwrap(), txid)
    }

    #[tokio::test]
    async fn compact_pot_beef_shorter_proven_overwrites_longer_proofless() {
        // Model real compaction: a proofless-with-ancestry BEEF is stored, then
        // the trimmed PROVEN BEEF (which is SHORTER) must overwrite it —
        // bypassing the longer-wins guard that a plain store_beef enforces.
        let store = MemoryPotStorage::new();
        let (proofless_long, txid) = proofless_beef_with_filler(RAW_A, Some(RAW_B));
        let (proven_short, txid2) = proven_beef(RAW_A);
        assert_eq!(txid, txid2, "same subject tx");
        assert!(!pot_beef_has_proof(&txid, &proofless_long), "fixture is proofless");
        assert!(pot_beef_has_proof(&txid, &proven_short), "fixture is proven");
        assert!(
            proven_short.len() < proofless_long.len(),
            "the proven+trimmed BEEF must be shorter to exercise the bypass"
        );

        store.store_beef(&txid, &proofless_long).await.unwrap();

        // A plain store_beef of the shorter proven is REJECTED (longer-wins).
        store.store_beef(&txid, &proven_short).await.unwrap();
        assert_eq!(
            store.get_beef(&txid).await.unwrap().as_deref(),
            Some(proofless_long.as_slice()),
            "longer-wins blocks a plain shorter write"
        );

        // compact_pot_beef BYPASSES longer-wins → the shorter proven overwrites.
        store.compact_pot_beef(&txid, &proven_short).await.unwrap();
        assert_eq!(
            store.get_beef(&txid).await.unwrap().as_deref(),
            Some(proven_short.as_slice()),
            "compact_pot_beef overwrites with the shorter proven BEEF"
        );
    }

    #[tokio::test]
    async fn compact_pot_beef_proofless_is_a_noop() {
        // Fail-closed: compacting with a BEEF that does NOT prove txid must not
        // touch the stored row (never trims on an unproven BEEF).
        let store = MemoryPotStorage::new();
        let (proofless_long, txid) = proofless_beef_with_filler(RAW_A, Some(RAW_B));
        store.store_beef(&txid, &proofless_long).await.unwrap();

        let (proofless_other, _) = proofless_beef_with_filler(RAW_A, None);
        assert!(!pot_beef_has_proof(&txid, &proofless_other));
        store.compact_pot_beef(&txid, &proofless_other).await.unwrap();
        assert_eq!(
            store.get_beef(&txid).await.unwrap().as_deref(),
            Some(proofless_long.as_slice()),
            "a proofless compact is a no-op"
        );
    }

    #[tokio::test]
    async fn find_pot_beefs_for_proof_check_returns_only_proofless() {
        // The candidate query must surface ONLY proofless rows — a proven row
        // must not be re-fetched.
        let store = MemoryPotStorage::new();
        let (proofless, proofless_txid) = proofless_beef_with_filler(RAW_A, None);
        let (proven, proven_txid) = proven_beef(RAW_B);
        assert_ne!(proofless_txid, proven_txid);

        store.store_beef(&proofless_txid, &proofless).await.unwrap();
        store.store_beef(&proven_txid, &proven).await.unwrap();
        assert_eq!(store.beef_count(), 2);

        let cands = store.find_pot_beefs_for_proof_check(10, 0).await.unwrap();
        assert_eq!(cands.len(), 1, "only the proofless row is a candidate");
        assert_eq!(cands[0].0, proofless_txid);
    }

    // ── find_spent_unconfirmed / spend-confirmation chaser (#186) ─────────

    #[tokio::test]
    async fn find_spent_unconfirmed_surfaces_only_spent_unconfirmed() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.store_record(&pot_record("potB", 0)).await.unwrap();
        store.store_record(&pot_record("potC", 0)).await.unwrap();

        // potA: spent, unconfirmed → a candidate.
        store.mark_spent("potA", 0, "settleA", false).await.unwrap();
        // potB: spent, confirmed → NOT a candidate.
        store.mark_spent("potB", 0, "settleB", true).await.unwrap();
        // potC: never spent → NOT a candidate.

        let cands = store.find_spent_unconfirmed(10, 0).await.unwrap();
        assert_eq!(cands.len(), 1, "only the spent-unconfirmed row is a candidate");
        assert_eq!(cands[0].txid, "potA");
        assert_eq!(
            cands[0].spending_txid.as_deref(),
            Some("settleA"),
            "a candidate always carries its spending txid"
        );
    }

    #[tokio::test]
    async fn find_spent_unconfirmed_empty_when_none() {
        let store = MemoryPotStorage::new();
        assert!(store.find_spent_unconfirmed(10, 0).await.unwrap().is_empty());
        // An unspent admitted row is still not a candidate.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        assert!(store.find_spent_unconfirmed(10, 0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn spend_confirmation_upgrade_and_never_downgrade() {
        // Frames the mark_spent invariant through the candidate query: the
        // chaser's confirmed upgrade removes the row from the candidate set and
        // a later unconfirmed claim can never downgrade it.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();

        // 0-conf spend recorded → appears as a candidate.
        store.mark_spent("potA", 0, "settle", false).await.unwrap();
        assert_eq!(store.find_spent_unconfirmed(10, 0).await.unwrap().len(), 1);

        // The chaser's upgrade (a chaintracks-verified spend).
        store.mark_spent("potA", 0, "settle", true).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed, "confirmed spend latches the flag");
        assert!(
            store.find_spent_unconfirmed(10, 0).await.unwrap().is_empty(),
            "a confirmed row is no longer a candidate"
        );

        // A later unconfirmed (forged) claim must NOT downgrade the row back
        // into the candidate set.
        store.mark_spent("potA", 0, "forged", false).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed, "confirmed flag survives");
        assert_eq!(r.spending_txid.as_deref(), Some("settle"), "pointer unchanged");
        assert!(
            store.find_spent_unconfirmed(10, 0).await.unwrap().is_empty(),
            "an unconfirmed claim never re-surfaces a confirmed row"
        );
    }

    #[tokio::test]
    async fn find_spent_unconfirmed_respects_limit() {
        let store = MemoryPotStorage::new();
        for i in 0..5u32 {
            let txid = format!("pot{i}");
            store.store_record(&pot_record(&txid, 0)).await.unwrap();
            store.mark_spent(&txid, 0, "settle", false).await.unwrap();
        }
        assert_eq!(store.find_spent_unconfirmed(2, 0).await.unwrap().len(), 2);
        assert_eq!(store.find_spent_unconfirmed(10, 0).await.unwrap().len(), 5);
    }

    // ── #228: push-consumer lookup + backstop age gates ──────────────────

    #[tokio::test]
    async fn find_unconfirmed_by_spending_txid_returns_only_that_spenders_rows() {
        let store = MemoryPotStorage::new();
        for (pot, spender) in [("potA", "settleX"), ("potB", "settleX"), ("potC", "settleY")] {
            store.store_record(&pot_record(pot, 0)).await.unwrap();
            store.mark_spent(pot, 0, spender, false).await.unwrap();
        }
        // A CONFIRMED settleX row is not a candidate (nothing left to latch).
        store.store_record(&pot_record("potD", 0)).await.unwrap();
        store.mark_spent("potD", 0, "settleX", true).await.unwrap();
        // An unspent row never appears.
        store.store_record(&pot_record("potE", 0)).await.unwrap();

        let rows = store.find_unconfirmed_by_spending_txid("settleX").await.unwrap();
        let mut pots: Vec<&str> = rows.iter().map(|r| r.txid.as_str()).collect();
        pots.sort_unstable();
        assert_eq!(pots, vec!["potA", "potB"]);
        assert!(store
            .find_unconfirmed_by_spending_txid("settleZ")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn spend_age_gate_anchors_on_the_spend_not_the_admission() {
        // A pot admitted LONG ago but spent JUST now must still wait out the
        // backstop window — the age anchor is the spend record (its push is
        // what gets first chance), never the pot admission time.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.advance_clock(100_000); // pot ages far past any gate
        store.mark_spent("potA", 0, "settle", false).await.unwrap();

        assert!(
            store.find_spent_unconfirmed(10, 1800).await.unwrap().is_empty(),
            "a fresh spend on an old pot still waits for its push"
        );
        store.advance_clock(1800);
        assert_eq!(store.find_spent_unconfirmed(10, 1800).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn spend_age_gate_resets_when_a_new_spender_overwrites() {
        // Last-writer-wins among unconfirmed claims: the NEW pointer's push
        // deserves its own window, so an accepted overwrite resets the age.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "claim1", false).await.unwrap();
        store.advance_clock(1800); // claim1 is now old enough
        assert_eq!(store.find_spent_unconfirmed(10, 1800).await.unwrap().len(), 1);

        store.mark_spent("potA", 0, "claim2", false).await.unwrap();
        assert!(
            store.find_spent_unconfirmed(10, 1800).await.unwrap().is_empty(),
            "the new pointer restarts the backstop window"
        );
    }

    #[tokio::test]
    async fn zero_min_age_disables_both_gates() {
        // min_age_secs = 0 is the pre-#228 behaviour: everything eligible
        // immediately (also the escape hatch if the gate must be turned off).
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settle", false).await.unwrap();
        store.store_beef("beefTx", &[1, 2, 3]).await.unwrap();

        assert_eq!(store.find_spent_unconfirmed(10, 0).await.unwrap().len(), 1);
        assert_eq!(store.find_pot_beefs_for_proof_check(10, 0).await.unwrap().len(), 1);
    }

    #[test]
    fn query_json_shape() {
        let q: PotQuery = serde_json::from_value(serde_json::json!({
            "type": "spentStatus",
            "outpoints": [{"txid": "ab".repeat(32), "vout": 0}, {"txid": "cd".repeat(32), "vout": 1}]
        }))
        .unwrap();
        let PotQuery::SpentStatus { outpoints } = q;
        assert_eq!(outpoints.len(), 2);
        assert_eq!(outpoints[1].vout, 1);

        // Unknown type is an error.
        assert!(serde_json::from_value::<PotQuery>(serde_json::json!({"type": "nope"})).is_err());
    }
}
