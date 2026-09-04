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
///
/// # The #284 decoded columns
///
/// The `lock_kind` / param / `pot_sats` / `verdict` fields are the
/// DECODE-ONCE denormalization (bsv-low #284): pure re-presentations of
/// bytes already admitted (the funding lock's committed param pushes, the
/// funding output value, the exact template-match of the recorded spend),
/// decoded by [`crate::pot::covenant`]. All `Option` + `serde(default)` so
/// every pre-#284 serialized form still deserializes (absent → `None` /
/// `false`), mirroring the `spent_confirmed` precedent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// `'covenant'` | `'bare'` | `'p2pkh'`; `None` = not decoded, OR decode
    /// attempted on an unrecognized shape (`params_decoded` disambiguates).
    #[serde(rename = "lockKind", default)]
    pub lock_kind: Option<String>,
    /// Committed settle keys, 66-hex lowercase (covenant locks only).
    #[serde(rename = "pubA", default)]
    pub pub_a: Option<String>,
    #[serde(rename = "pubB", default)]
    pub pub_b: Option<String>,
    #[serde(rename = "pubTower", default)]
    pub pub_tower: Option<String>,
    /// Committed payout/rake homes, 40-hex lowercase.
    #[serde(rename = "payPkhA", default)]
    pub pay_pkh_a: Option<String>,
    #[serde(rename = "payPkhB", default)]
    pub pay_pkh_b: Option<String>,
    #[serde(rename = "rakePkh", default)]
    pub rake_pkh: Option<String>,
    /// Committed amounts/height.
    #[serde(rename = "stakeA", default)]
    pub stake_a: Option<u64>,
    #[serde(rename = "stakeB", default)]
    pub stake_b: Option<u64>,
    #[serde(rename = "feeSats", default)]
    pub fee_sats: Option<u64>,
    #[serde(rename = "recoveryHeight", default)]
    pub recovery_height: Option<u64>,
    /// The funding output's satoshi value (from the admitted BEEF's parsed
    /// tx) — the stake-conservation anchor (`stakeA + stakeB == potSats`).
    #[serde(rename = "potSats", default)]
    pub pot_sats: Option<u64>,
    /// `false` = decode not yet attempted (a backfill candidate); `true` =
    /// attempted + recorded (`lock_kind` says what it was).
    #[serde(rename = "paramsDecoded", default)]
    pub params_decoded: bool,
    /// The template-match verdict of the recorded spend (wire strings of
    /// [`crate::pot::covenant::PotVerdict`]). Meaningful ONLY when
    /// `verdict_txid == spending_txid` — a later pointer overwrite leaves a
    /// stale verdict behind on purpose (the reader's equality check guards).
    #[serde(rename = "verdict", default)]
    pub verdict: Option<String>,
    #[serde(rename = "verdictTxid", default)]
    pub verdict_txid: Option<String>,
    /// Block height from the SPV-verified BUMP at spend-confirm time.
    #[serde(rename = "spentHeight", default)]
    pub spent_height: Option<u64>,
    /// bsv-low #371: whether the RECORDED spender's own bytes parse as FINAL
    /// (`!(lockTime > 0 && any input sequence < 0xffffffff)`). A fact about
    /// the spender the pointer names, so it RIDES THE POINTER exactly like
    /// `spent_height` (same-pointer: `Some` overwrites / `None` keeps;
    /// pointer change: reset to the incoming value, including `None`).
    /// `None` = recorded before this shipped, or the writer had no parse —
    /// readers fall back to the merkle bar. A tower-parked NON-FINAL refund
    /// is `Some(false)` and keeps the #323 confirmed-only bar verbatim.
    #[serde(rename = "spenderFinal", default)]
    pub spender_final: Option<bool>,
    /// bsv-low #406: WHO SIGNED the recorded spend — wire strings of
    /// [`crate::pot::SettleSigners`] (`'coop'` = the two seats, `'tower-a'` /
    /// `'tower-b'` = the tower + that seat), derived by verifying the spend's
    /// signatures against the committed key triple over the network's own
    /// BIP-143 digest. Part of the VERDICT GROUP: written only alongside
    /// `verdict` (same statement / same CAS), so it shares `verdict_txid`'s
    /// lineage and is meaningful ONLY when `verdict_txid == spending_txid`.
    /// `None` = not established (pre-#406 row awaiting backfill, or no pair
    /// verified) — readers must degrade to "not established", never guess.
    /// DISPLAY-TIER: feeds ending narration, never a count/rank/credit.
    #[serde(rename = "settleSigners", default)]
    pub settle_signers: Option<String>,
    /// bsv-low P4 slice 2 (2026-09-02): the FUNDING tx's own serialized size
    /// and exact fee (Σ inputs − Σ outputs), read once at admission from the
    /// admitted BEEF by [`crate::tx_facts::facts_from_atomic_beef`]. The fee
    /// is `None` when the BEEF does not carry every parent output (never an
    /// estimate); the size is `None` only when the BEEF did not name the
    /// subject at all. DISPLAY-TIER (the receipt's money section) — never a
    /// count, rank, credit or WHERE. Stored-wins on re-admission like every
    /// other decoded column.
    #[serde(rename = "fundingSizeBytes", default)]
    pub funding_size_bytes: Option<u64>,
    #[serde(rename = "fundingFeeSats", default)]
    pub funding_fee_sats: Option<u64>,
    /// bsv-low P4 slice 2: the recorded SPENDER's own size + exact fee,
    /// read at spend-record time from the spending BEEF (the fee falls back
    /// to `potSats − Σ outputs` for a single-input pot spend — the pot value
    /// is the admitted funding output's, never a caller claim). Keyed by
    /// `spender_facts_txid` and written under a CAS on the live pointer
    /// ([`PotStorage::store_spender_facts`]), so the pair is meaningful ONLY
    /// when `spender_facts_txid == spending_txid` — a later pointer overwrite
    /// leaves a stale pair behind on purpose (the reader's equality check
    /// guards, exactly like `verdict_txid`). DISPLAY-TIER.
    #[serde(rename = "spenderFactsTxid", default)]
    pub spender_facts_txid: Option<String>,
    #[serde(rename = "spenderSizeBytes", default)]
    pub spender_size_bytes: Option<u64>,
    #[serde(rename = "spenderFeeSats", default)]
    pub spender_fee_sats: Option<u64>,
}

/// The #284 verdict group as ONE write value (bsv-low #406): the verdict
/// string plus the optional #406 signer classification. A signers value
/// cannot exist WITHOUT a verdict by construction — both ride the same
/// statement and share `verdict_txid`'s pointer lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerdictWrite<'a> {
    /// [`crate::pot::PotVerdict::as_str`] wire string.
    pub verdict: &'a str,
    /// [`crate::pot::SettleSigners::as_str`] wire string, or `None` when no
    /// signature pair verified (stored NULL — "not established").
    pub settle_signers: Option<&'a str>,
}

impl<'a> VerdictWrite<'a> {
    /// A verdict with no signer classification ("not established").
    pub fn bare(verdict: &'a str) -> Self {
        VerdictWrite {
            verdict,
            settle_signers: None,
        }
    }
}

impl PotRecord {
    /// Rebuild the committed [`CovenantParams`] from this row's decoded
    /// columns. `Some` ONLY when the row is a decoded covenant lock with
    /// every param present and well-formed (strict hex/length validation in
    /// [`covenant_params_from_hex`]) — a malformed stored value degrades to
    /// `None` (the caller falls back to the BEEF parse / classifies
    /// nothing), never a trust-shortcut.
    pub fn decoded_covenant_params(&self) -> Option<crate::pot::covenant::CovenantParams> {
        if self.lock_kind.as_deref() != Some("covenant") {
            return None;
        }
        crate::pot::covenant_params_from_hex(
            self.pub_a.as_deref()?,
            self.pub_b.as_deref()?,
            self.pub_tower.as_deref()?,
            self.pay_pkh_a.as_deref()?,
            self.pay_pkh_b.as_deref()?,
            self.rake_pkh.as_deref()?,
            self.stake_a?,
            self.stake_b?,
            self.fee_sats?,
            self.recovery_height?,
        )
    }
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
    /// Insert-if-absent for the SPEND fields, decoded-column upsert for the
    /// #284 fields (mirrors the D1 `INSERT ... ON CONFLICT DO UPDATE`):
    ///
    /// - a row already marked spent is NEVER clobbered back to unspent — the
    ///   conflict update must not touch `spent` / `spending_txid` /
    ///   `spent_confirmed` / `verdict` / `verdict_txid` / `spent_height` /
    ///   creation stamps (re-admission must never regress spend state);
    /// - the DECODED columns backfill COALESCE-style: an incoming `Some`
    ///   fills an absent stored value, an incoming `None` (a replay lacking
    ///   data) never nulls a stored one; `params_decoded` only ever latches
    ///   `false → true`.
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
    ///
    /// # #284 verdict + height (atomic with the pointer)
    ///
    /// - `verdict = Some(v)` writes `verdict = v.verdict, verdict_txid =
    ///   spending_txid, settle_signers = v.settle_signers` IN THE SAME
    ///   statement as the spend pointer — the verdict group can never point
    ///   at a different spender than the pointer it rode in with (#406: the
    ///   signer classification is part of the group, typed so it cannot be
    ///   written without a verdict). `verdict = None` leaves ALL THREE
    ///   columns UNCHANGED (a confirm-only caller with no spender raw must
    ///   not null a stored verdict); if the pointer changes under `None`,
    ///   the stale group deliberately remains and is neutralized by the
    ///   reader's `verdict_txid == spending_txid` equality check.
    /// - `spent_height` is honored ONLY on the `confirmed = true` branch (a
    ///   height is a fact of the verified BUMP), and it RIDES THE POINTER
    ///   exactly like the verdict does (gate finding LOW-1, 2026-07-28):
    ///   when the write keeps the SAME `spending_txid`, `None` keeps the
    ///   stored value (COALESCE); when the write CHANGES the pointer, the
    ///   height is RESET to the incoming value — including `None`, so a
    ///   reorg-confirmed S2 whose bump yielded no height can never inherit
    ///   S1's height and serve it as its own `at.height`. The unconfirmed
    ///   branch never touches it (an accepted unconfirmed write always meets
    ///   a NULL height — heights only ride confirmed writes, which latch the
    ///   flag that then refuses unconfirmed writers).
    /// - `spender_final` (bsv-low #371) is the spender's bytes-finality and
    ///   RIDES THE POINTER on BOTH branches (unlike `spent_height`, which
    ///   only rides confirmed writes — an unconfirmed final settle is exactly
    ///   the row the #371 third arm exists for): same pointer ⇒ `Some`
    ///   overwrites / `None` keeps; pointer change ⇒ RESET to the incoming
    ///   value (a new spender never inherits the old spender's finality).
    #[allow(clippy::too_many_arguments)] // the write is atomic by design: every field rides the pointer
    async fn mark_spent(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        confirmed: bool,
        verdict: Option<VerdictWrite<'_>>,
        spent_height: Option<u64>,
        spender_final: Option<bool>,
    ) -> Result<(), PotStorageError>;

    /// bsv-low P4 slice 2: record the SPENDER's own size + fee under a CAS on
    /// the pointer they were computed for (`spending_txid` must be the LIVE
    /// pointer, else no-op — the `mark_verdict_for_spender` idiom). Same
    /// pointer already keyed ⇒ stored-wins per value; a different
    /// `spender_facts_txid` ⇒ the whole pair is RESET to the incoming values
    /// (a new spender never inherits the old spender's facts). DISPLAY-TIER,
    /// best-effort at the call site: a failure here never fails the spend
    /// record it follows. The default refuses so a real store cannot forget
    /// it silently; the in-memory and D1 stores implement it.
    async fn store_spender_facts(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        facts: crate::tx_facts::TxFacts,
    ) -> Result<(), PotStorageError> {
        let _ = (txid, output_index, spending_txid, facts);
        Err(PotStorageError::Other(
            "store_spender_facts is not implemented by this store".into(),
        ))
    }

    /// The record for an outpoint, or `None` if we never admitted it.
    async fn get_spent_status(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Option<PotRecord>, PotStorageError>;

    /// Batched [`get_spent_status`](Self::get_spent_status) (bsv-low #289):
    /// answers a whole outpoint set at once. The result is ALIGNED
    /// index-for-index with the input — `None` for an outpoint we never
    /// admitted — so the caller's per-entry fail-safe semantics
    /// (`known: false`, never "unspent") are unchanged. This default loops
    /// the single-row method; the D1 backend overrides it with one
    /// `IN (VALUES …)` query per chunk instead of one round trip per
    /// outpoint (`ls_pot` is the money landing-proof path, polled by every
    /// crediting client).
    async fn get_spent_statuses(
        &self,
        outpoints: &[(String, u32)],
    ) -> Result<Vec<Option<PotRecord>>, PotStorageError> {
        let mut out = Vec::with_capacity(outpoints.len());
        for (txid, output_index) in outpoints {
            out.push(self.get_spent_status(txid, *output_index).await?);
        }
        Ok(out)
    }

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
    /// [`find_pot_beefs_for_proof_check`](Self::find_pot_beefs_for_proof_check)),
    /// excluding rows whose CURRENT pointer is latched structurally
    /// unprovable (bsv-low handoff #2b — chasing a proof that conflicts
    /// with a recorded confirmed spend wastes the budget forever; an
    /// unknown/unlatched pointer stays a full candidate).
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

    /// bsv-low W4 (2026-09-04): pots the index still marks UNSPENT that are at
    /// least `min_age_secs` old, OLDEST first, at most `limit`. The candidate
    /// set of [`discover_missing_spends`]: eight pre-heal-era rows on beta sat
    /// `spent = 0` while `/spent-any` proved them spent with known spenders —
    /// nothing ever asked the couriers about a pot nobody submitted a spend
    /// for. Backends that can't enumerate return an empty `Vec` (no-op).
    async fn find_unspent_stale(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        let _ = (limit, min_age_secs);
        Ok(Vec::new())
    }

    /// bsv-low (2026-09-04): the same candidates WITH each row's `createdAt`
    /// (unix seconds, `None` when the backend has no stamp), so the discovery
    /// pass can tell a NEW-era pot (admitted after the pointer-heal shipped)
    /// from the legacy backlog. Default: the age-less query, every stamp `None`.
    async fn find_unspent_stale_with_age(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<(PotRecord, Option<u64>)>, PotStorageError> {
        Ok(self
            .find_unspent_stale(limit, min_age_secs)
            .await?
            .into_iter()
            .map(|r| (r, None))
            .collect())
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

    /// Attach a #284 verdict group to a row via GUARDED COMPARE-AND-SET (gate
    /// finding MEDIUM-2, 2026-07-28): sets `verdict` + `verdict_txid =
    /// spending_txid` + `settle_signers` (#406 — the group rides as one) ONLY
    /// when the row's CURRENT spend pointer still equals `spending_txid` —
    /// and touches NOTHING else (never the pointer, never `spent_confirmed`,
    /// never the #228 `spent_at` age anchor, never `spent_height`). This is
    /// the backfill's write: its candidate read and its write are separated
    /// by awaits, so a reorg-confirmed S2 landing in the window must make the
    /// write a NO-OP, never get displaced back to the stale S1 the verdict
    /// was computed for. Backends that can't enumerate may keep this default
    /// no-op — the read-path fallback still classifies.
    async fn mark_verdict_for_spender(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        verdict: VerdictWrite<'_>,
    ) -> Result<(), PotStorageError> {
        let _ = (txid, output_index, spending_txid, verdict);
        Ok(())
    }

    /// The DISPLACEMENT CAS (bsv-low 2026-08-18 reconcile): move the spend
    /// pointer from `from_spender` (a recorded claim that never verifiably
    /// mined) to `to_spender` (input-bound to the outpoint and
    /// chaintracks-verified MINED by the caller), latching `spentConfirmed`.
    /// GUARDED on both the from-pointer and still-unconfirmed — if the row
    /// moved or was competing-confirmed inside the caller's verify window the
    /// write is a NO-OP (`Ok(false)`) and the next tick re-evaluates (never
    /// trade a self-healing failure for a permanent one; the #301 discipline).
    /// The pointer CHANGES by definition, so `spent_height`/`spender_final`
    /// are the incoming values (a new spender never inherits the old one's);
    /// verdict columns are NEVER touched (a stale verdict stays keyed to the
    /// old txid and every reader guards `verdictTxid == spendingTxid`).
    /// A hit is a confirm moment: superseded siblings are retired, same as
    /// every other confirmed write.
    ///
    /// Default `Ok(false)` — fail-closed no-op — so implementations that do
    /// not support displacement keep compiling and simply never displace.
    async fn displace_spend_for(
        &self,
        _txid: &str,
        _output_index: u32,
        _from_spender: &str,
        _to_spender: &str,
        _spent_height: Option<u64>,
        _spender_final: Option<bool>,
    ) -> Result<bool, PotStorageError> {
        Ok(false)
    }

    /// Latch `spent_confirmed` via GUARDED COMPARE-AND-SET (bsv-low #301,
    /// the [`mark_verdict_for_spender`](Self::mark_verdict_for_spender)
    /// sibling): sets `spent = true, spent_confirmed = true` (and
    /// keeps-or-updates `spent_height` — `None` keeps the stored value,
    /// the same-pointer COALESCE semantics of
    /// [`mark_spent`](Self::mark_spent)) ONLY when the row's CURRENT spend
    /// pointer still equals `spending_txid` — the spender the caller's SPV
    /// proof was verified FOR. Touches NOTHING else: never the pointer,
    /// never `verdict`/`verdict_txid`, never the #228 `spent_at` age
    /// anchor (the confirmed row leaves the chaser's candidate set, so the
    /// anchor is moot — and a CAS-missed row keeps its true age).
    ///
    /// This is the #186 spend-confirmation chaser's write: its candidate
    /// read and its confirm are separated by awaits (the proof fetch), so
    /// a reorg-confirmed S2 landing in that window must make the write a
    /// NO-OP — the pre-#301 unguarded `mark_spent(confirmed = true)` reset
    /// the pointer back to the stale S1, and nothing ever re-chased it
    /// (the chaser only surfaces `spent_confirmed = 0` rows).
    ///
    /// Returns whether the guard HIT (`true` = confirmed written; `false`
    /// = the pointer moved under the caller — leave it, count it, let the
    /// pass's normal candidate selection re-visit). Backends that cannot
    /// CAS keep this default `Ok(false)`: fail-safe — nothing is ever
    /// confirmed on their word, the row simply stays a candidate and the
    /// miss is loud in the caller's counters.
    async fn mark_confirmed_for_spender(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        spent_height: Option<u64>,
    ) -> Result<bool, PotStorageError> {
        let _ = (txid, output_index, spending_txid, spent_height);
        Ok(false)
    }

    /// Rows whose #284 decode has never been attempted (`params_decoded =
    /// false`) — the lazy-backfill candidate set
    /// (`proof_fetcher::backfill_decoded_params`). Backends that enumerate
    /// answer `WHERE paramsDecoded = 0 ORDER BY RANDOM() LIMIT n` (RANDOM
    /// defeats head-of-queue starvation, the proof-check idiom). A row whose
    /// funding BEEF is missing stays `params_decoded = false` (retried
    /// forever, bounded per tick); a row whose decode was attempted —
    /// whatever the lock turned out to be — is `true` and NEVER rescanned.
    /// Backends that can't enumerate return an empty `Vec` via this default
    /// → the backfill is a no-op.
    async fn find_params_undecoded(&self, limit: u64) -> Result<Vec<PotRecord>, PotStorageError> {
        let _ = limit;
        Ok(Vec::new())
    }

    /// bsv-low #406 backfill candidates: DECODED covenant rows whose CURRENT
    /// verdict group lacks the signer classification (`verdict IS NOT NULL
    /// AND verdict_txid = spending_txid AND settle_signers IS NULL`). These
    /// are the rows written before #406 shipped (or whose live classify had
    /// no spender raw); `proof_fetcher::backfill_settle_signers` re-derives
    /// the signers from the STORED spender BEEF and re-attaches the whole
    /// group via [`mark_verdict_for_spender`](Self::mark_verdict_for_spender)
    /// (idempotent: same bytes ⇒ same verdict). RANDOM order per tick, same
    /// starvation rationale as [`find_params_undecoded`]. A row whose
    /// signatures never verify would re-enter forever — the backfill caller
    /// bounds that by latching the row out of the candidate set explicitly
    /// (see its docs). Backends that can't enumerate return empty → no-op.
    async fn find_settle_signers_unlatched(
        &self,
        limit: u64,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        let _ = limit;
        Ok(Vec::new())
    }

    /// Durably store `beef` under `txid` (the stored tx's OWN txid — the
    /// funding txid for a funding beef, the SETTLE txid for a settle beef).
    ///
    /// Longer-wins, never-clobber (the "vanishing table" lesson): the write
    /// happens only when no row exists or the new beef is strictly LONGER
    /// than the stored one; an empty `beef` is rejected (no-op). A good row
    /// is therefore never replaced by a shorter/empty one.
    ///
    /// KNOWN RESIDUAL SURFACE (gate finding LOW-2, 2026-07-28; pre-existing,
    /// documented not defended): "longer" is a byte-length proxy, not a
    /// usefulness proof — a submitted BEEF that is LONGER yet carries less
    /// usable data (e.g. duplicate/`TxidOnly`-encoded entries padding the
    /// length while the subject's raw bytes are absent) can displace a
    /// raw-carrying row. The failure direction is fail-safe only: every
    /// consumer hash-verifies/extracts before deriving anything, so a
    /// degraded stored BEEF makes reads degrade to unresolved/`retry` (the
    /// proof-completion and #284 backfill passes retry forever) — it can
    /// ERASE serving ability, never FABRICATE a fact.
    async fn store_beef(&self, txid: &str, beef: &[u8]) -> Result<(), PotStorageError>;

    /// The stored BEEF for `txid`, or `None` if we never stored one.
    async fn get_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, PotStorageError>;

    /// Return a bounded page of NOT-YET-VERIFIED stored pot BEEFs
    /// (`(txid, beef)`) for the proof-completion cron (#192/#193). A row is
    /// a candidate until a chaintracks-VERIFIED merkle BUMP for its OWN
    /// txid has been latched (bsv-low#304) — a STRUCTURAL bump in the
    /// stored bytes does NOT settle it: admit-path bytes are submitter-
    /// supplied with zero SPV, so a fake-bumped row must stay in the
    /// candidate set until the pass re-verifies (or replaces) its proof.
    ///
    /// Backends that track a verified latch answer with
    /// `WHERE proof_verified = 0 ORDER BY RANDOM() LIMIT n` (RANDOM defeats
    /// head-of-queue starvation — a never-mineable head must not starve the
    /// tail), additionally excluding rows LATCHED structurally unprovable
    /// (bsv-low handoff #2b: a recorded confirmed spend of the same outpoint
    /// by a different txid means the row can never mine — retired, not
    /// polled forever; NULL/unexamined stays eligible, and the verifying
    /// writers clear the latch so a reorg-proven row returns). Backends
    /// that can't enumerate (or have nothing to complete) may
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

    /// Latch `txid`'s VERIFIED-proof flag WITHOUT rewriting bytes
    /// (bsv-low#304) — called by the completion pass when the STORED BEEF's
    /// own bump has just been chaintracks-re-verified (the fast path for
    /// the honest backlog: no external fetch, no byte rewrite, the
    /// first-store age anchor stays intact). The caller is the ONLY party
    /// allowed to have verified; backends must never latch this from
    /// structure alone. Default: no-op (backends without the latch keep
    /// their candidate semantics).
    async fn mark_pot_beef_proven(&self, txid: &str) -> Result<(), PotStorageError> {
        let _ = txid;
        Ok(())
    }

    /// Batch form of [`Self::mark_pot_beef_proven`] (bsv-low#304 gate M-4):
    /// latch MANY re-verified rows in as few backend round trips as the
    /// backend can manage — the completion pass's fast path latches up to a
    /// whole candidate page per tick, and one write per row was the page's
    /// dominant op cost. Same trust contract as the single form: the caller
    /// chaintracks-verified EVERY listed txid's stored bump. All-or-nothing
    /// is NOT required — a failed chunk simply leaves those rows candidates
    /// (retried next tick). Default: loop the single form.
    async fn mark_pot_beefs_proven(&self, txids: &[String]) -> Result<(), PotStorageError> {
        for txid in txids {
            self.mark_pot_beef_proven(txid).await?;
        }
        Ok(())
    }

    /// Whether `txid`'s stored BEEF carries a VERIFIED proof latch
    /// (bsv-low#304) — i.e. one of the verifying writers latched it. NEVER
    /// derived from byte structure. Default: `false` (backends without the
    /// latch treat every row as unverified — the fail direction that only
    /// ever causes a redundant VERIFYING re-write, never a trust
    /// strengthening).
    async fn pot_beef_proof_verified(&self, txid: &str) -> Result<bool, PotStorageError> {
        let _ = txid;
        Ok(false)
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
    /// TEST HOOK (2026-09-04): unix-second creation stamps for rows the age-aware
    /// stale query should report (`with_created_at`); rows without one read `None`,
    /// exactly like a D1 row whose `createdAt` is NULL.
    pub created_at_secs: std::sync::Mutex<std::collections::HashMap<(String, u32), u64>>,
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
    /// txids whose VERIFIED-proof latch is set — models the D1
    /// `pot_beefs.proof_verified` column (bsv-low#304). Latched ONLY by
    /// `compact_pot_beef` / `mark_pot_beef_proven` (the verifying
    /// writers); RESET by any admit-path `store_beef` byte write. A
    /// structural bump in stored bytes never enters this set by itself.
    verified: std::sync::Mutex<std::collections::HashSet<String>>,
    /// txids latched STRUCTURALLY UNPROVABLE (bsv-low handoff #2b) —
    /// models the D1 `pot_beefs.structurally_unprovable` column: the
    /// stored tx's own bytes spend a pot outpoint whose confirmed spend by
    /// a DIFFERENT txid was recorded, so it can never mine. Latched ONLY
    /// at a confirm moment (`mark_spent(confirmed)` /
    /// `mark_confirmed_for_spender` hit), NEVER on an unconfirmed
    /// displacement (Rule 6); CLEARED by every verifying writer AND on the
    /// latched txid's own confirm (confirm beats the latch); PRESERVED by
    /// admit-path `store_beef` rewrites. Where the D1 backend derives the
    /// siblings from `potrefund_records.refundRawHex` (its cheapest
    /// indexed source), this backend derives the SAME fact from the bytes
    /// it actually owns: each stored beef's subject inputs.
    unprovable: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl MemoryPotStorage {
    /// TEST HOOK: stamp a row's creation time (unix seconds) for `find_unspent_stale_with_age`.
    pub fn with_created_at(self, txid: &str, output_index: u32, secs: u64) -> Self {
        self.created_at_secs
            .lock()
            .unwrap()
            .insert((txid.to_string(), output_index), secs);
        self
    }

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

    /// TEST OBSERVABILITY (#2b): whether `txid` is latched structurally
    /// unprovable (the retirement latch the poll pools exclude on).
    pub fn is_structurally_unprovable(&self, txid: &str) -> bool {
        self.unprovable.lock().unwrap().contains(txid)
    }

    /// #2b retirement at a confirm moment (the memory twin of
    /// `D1PotStorage::retire_superseded_after_confirm`): a spend of
    /// `pot_txid:output_index` by `confirmed_spending_txid` was just
    /// recorded CONFIRMED, so (1) the confirmed spender's own latch is
    /// cleared (confirm beats the latch — the reorg direction), and
    /// (2) every OTHER stored beef whose SUBJECT verifiably spends the
    /// same outpoint is latched unprovable — derived from the bytes this
    /// store actually owns (each beef's subject inputs; the D1 backend
    /// uses its indexed `potrefund_records.refundRawHex` source for the
    /// same fact). VERIFIED-proven rows are never latched (their proof is
    /// chain truth); unparseable beefs are skipped (fail-safe: no latch).
    fn retire_superseded_after_confirm(
        &self,
        pot_txid: &str,
        output_index: u32,
        confirmed_spending_txid: &str,
    ) {
        let mut unprovable = self.unprovable.lock().unwrap();
        unprovable.retain(|t| !t.eq_ignore_ascii_case(confirmed_spending_txid));
        let verified = self.verified.lock().unwrap();
        for (txid, bytes) in self.beefs.lock().unwrap().iter() {
            if txid.eq_ignore_ascii_case(confirmed_spending_txid) || verified.contains(txid) {
                continue;
            }
            // Hash-bound parse: the stored key must be the subject txid, so
            // a garbled row can never latch some other tx's identity.
            let Ok(tx) = bsv_rs::transaction::Transaction::from_beef(bytes, Some(txid)) else {
                continue;
            };
            let conflicts = tx.inputs.iter().any(|i| {
                i.source_txid
                    .as_deref()
                    .is_some_and(|t| t.eq_ignore_ascii_case(pot_txid))
                    && i.source_output_index == output_index
            });
            if conflicts {
                unprovable.insert(txid.clone());
            }
        }
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
    async fn find_unspent_stale(
        &self,
        limit: u64,
        _min_age_secs: u64,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        // The memory store carries no createdAt: every unspent row is eligible.
        // POTS FIRST (2026-09-04): a `tm_lowfund` hop/change row (`lock_kind`
        // p2pkh) sorts after every pot — on beta 3,571 hop rows stood in front
        // of 99 pots and starved the discovery pass for a day.
        let all = self
            .records
            .lock()
            .map_err(|_| PotStorageError::Other("lock".into()))?;
        let mut out: Vec<PotRecord> = all.iter().filter(|r| !r.spent).cloned().collect();
        out.sort_by(|a, b| {
            (is_hop_row(a), &a.txid, a.output_index).cmp(&(is_hop_row(b), &b.txid, b.output_index))
        });
        out.truncate(limit as usize);
        Ok(out)
    }

    async fn find_unspent_stale_with_age(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<(PotRecord, Option<u64>)>, PotStorageError> {
        let stamps = self
            .created_at_secs
            .lock()
            .map_err(|_| PotStorageError::Other("lock".into()))?
            .clone();
        Ok(self
            .find_unspent_stale(limit, min_age_secs)
            .await?
            .into_iter()
            .map(|r| {
                let stamp = stamps.get(&(r.txid.clone(), r.output_index)).copied();
                (r, stamp)
            })
            .collect())
    }

    async fn store_record(&self, record: &PotRecord) -> Result<(), PotStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent for SPEND state; decoded-column upsert for the
        // #284 fields (mirrors the D1 ON CONFLICT DO UPDATE — see the trait
        // doc): spend fields of an existing row are NEVER touched, decoded
        // Somes fill absent stored values, incoming Nones never null, and
        // params_decoded only latches false → true.
        match records
            .iter_mut()
            .find(|r| r.txid == record.txid && r.output_index == record.output_index)
        {
            None => records.push(record.clone()),
            Some(existing) => {
                fn fill<T: Clone>(slot: &mut Option<T>, incoming: &Option<T>) {
                    if slot.is_none() {
                        *slot = incoming.clone();
                    }
                }
                fill(&mut existing.lock_kind, &record.lock_kind);
                fill(&mut existing.pub_a, &record.pub_a);
                fill(&mut existing.pub_b, &record.pub_b);
                fill(&mut existing.pub_tower, &record.pub_tower);
                fill(&mut existing.pay_pkh_a, &record.pay_pkh_a);
                fill(&mut existing.pay_pkh_b, &record.pay_pkh_b);
                fill(&mut existing.rake_pkh, &record.rake_pkh);
                fill(&mut existing.stake_a, &record.stake_a);
                fill(&mut existing.stake_b, &record.stake_b);
                fill(&mut existing.fee_sats, &record.fee_sats);
                fill(&mut existing.recovery_height, &record.recovery_height);
                fill(&mut existing.pot_sats, &record.pot_sats);
                fill(&mut existing.funding_size_bytes, &record.funding_size_bytes);
                fill(&mut existing.funding_fee_sats, &record.funding_fee_sats);
                existing.params_decoded |= record.params_decoded;
                // spent / spending_txid / spent_confirmed / verdict /
                // verdict_txid / spent_height: NEVER touched here.
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // the write is atomic by design: every field rides the pointer
    async fn mark_spent(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        confirmed: bool,
        verdict: Option<VerdictWrite<'_>>,
        spent_height: Option<u64>,
        spender_final: Option<bool>,
    ) -> Result<(), PotStorageError> {
        let now = self.now();
        let mut records = self.records.lock().unwrap();
        // #2b: set when a CONFIRMED write is accepted — the retirement
        // moment (never the unconfirmed branch: Rule 6).
        let mut confirmed_written = false;
        // UPDATE-only: touch an existing row; absent outpoint is a no-op.
        for r in records.iter_mut() {
            if r.txid == txid && r.output_index == output_index {
                let same_pointer = r.spending_txid.as_deref() == Some(spending_txid);
                let wrote = if confirmed {
                    // Chain truth: always write, latch spent_confirmed
                    // (last-confirmed-wins). The height rides ONLY the
                    // confirmed branch (a fact of the verified BUMP) AND
                    // rides the POINTER (gate LOW-1): same pointer ⇒
                    // keep-or-update (None keeps); pointer change ⇒ RESET to
                    // the incoming value (a new spender never inherits the
                    // old spender's height).
                    r.spent = true;
                    r.spending_txid = Some(spending_txid.to_string());
                    r.spent_confirmed = true;
                    if same_pointer {
                        if let Some(h) = spent_height {
                            r.spent_height = Some(h);
                        }
                    } else {
                        r.spent_height = spent_height;
                    }
                    true
                } else if !r.spent_confirmed {
                    // Unconfirmed claim: only allowed while no confirmed
                    // pointer exists (last-writer among unconfirmed);
                    // spent_confirmed (and spent_height) never touched here.
                    r.spent = true;
                    r.spending_txid = Some(spending_txid.to_string());
                    true
                } else {
                    // Unconfirmed claim vs confirmed pointer → REFUSED.
                    false
                };
                if wrote {
                    // #371: the spender's bytes-finality rides the SAME
                    // accepted write as the pointer, on BOTH branches (an
                    // unconfirmed final settle is the third arm's subject):
                    // same pointer ⇒ Some overwrites / None keeps; pointer
                    // change ⇒ reset to the incoming value (incl. None).
                    if same_pointer {
                        if let Some(f) = spender_final {
                            r.spender_final = Some(f);
                        }
                    } else {
                        r.spender_final = spender_final;
                    }
                    // The verdict GROUP rides the SAME accepted write as the
                    // pointer (atomic): Some sets all three columns (#406:
                    // the signer classification is part of the group); None
                    // leaves all three UNCHANGED (a stale group is
                    // neutralized by the reader's verdict_txid ==
                    // spending_txid check).
                    if let Some(v) = verdict {
                        r.verdict = Some(v.verdict.to_string());
                        r.verdict_txid = Some(spending_txid.to_string());
                        r.settle_signers = v.settle_signers.map(str::to_string);
                    }
                    // Stamp the spend-record time on every accepted write
                    // (#228 backstop age anchor): a NEW spend pointer resets
                    // the clock so its own push gets its chance first.
                    self.spent_at
                        .lock()
                        .unwrap()
                        .insert((txid.to_string(), output_index), now);
                    if confirmed {
                        confirmed_written = true;
                    }
                }
            }
        }
        drop(records);
        // #2b: a CONFIRMED spend record is the moment every OTHER stored tx
        // spending this outpoint became structurally unprovable — retire
        // them (and clear any stale latch on the confirmed spender).
        if confirmed_written {
            self.retire_superseded_after_confirm(txid, output_index, spending_txid);
        }
        Ok(())
    }

    async fn mark_verdict_for_spender(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        verdict: VerdictWrite<'_>,
    ) -> Result<(), PotStorageError> {
        // Guarded CAS (gate MEDIUM-2): the verdict group only (verdict +
        // verdict_txid + #406 settle_signers), and only while the row's
        // CURRENT pointer still equals the one the group was computed for.
        // A moved pointer ⇒ no-op. Nothing else touched.
        let mut records = self.records.lock().unwrap();
        for r in records.iter_mut() {
            if r.txid == txid
                && r.output_index == output_index
                && r.spending_txid.as_deref() == Some(spending_txid)
            {
                r.verdict = Some(verdict.verdict.to_string());
                r.verdict_txid = Some(spending_txid.to_string());
                r.settle_signers = verdict.settle_signers.map(str::to_string);
            }
        }
        Ok(())
    }

    async fn displace_spend_for(
        &self,
        txid: &str,
        output_index: u32,
        from_spender: &str,
        to_spender: &str,
        spent_height: Option<u64>,
        spender_final: Option<bool>,
    ) -> Result<bool, PotStorageError> {
        let mut hit = false;
        {
            let mut records = self.records.lock().unwrap();
            for r in records.iter_mut() {
                if r.txid == txid
                    && r.output_index == output_index
                    && !r.spent_confirmed
                    && r.spending_txid.as_deref() == Some(from_spender)
                {
                    r.spent = true;
                    r.spending_txid = Some(to_spender.to_string());
                    r.spent_confirmed = true;
                    // Pointer change by definition: incoming values, never
                    // inherited (the mark_spent ELSE-branch doctrine).
                    r.spent_height = spent_height;
                    r.spender_final = spender_final;
                    hit = true;
                }
            }
        }
        if hit {
            // A confirmed write is a retirement moment (#2b), same as the
            // other two confirmed writers.
            self.retire_superseded_after_confirm(txid, output_index, to_spender);
        }
        Ok(hit)
    }

    async fn mark_confirmed_for_spender(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        spent_height: Option<u64>,
    ) -> Result<bool, PotStorageError> {
        // Guarded CAS confirm (#301, the mark_verdict_for_spender sibling):
        // latch the confirmed flag only while the row's CURRENT pointer
        // still equals the spender the caller's proof was verified for. A
        // moved pointer ⇒ Ok(false), nothing touched. Same-pointer height
        // semantics (None keeps the stored value); spent_at deliberately
        // untouched (the CAS idiom — a missed row keeps its true age).
        let mut records = self.records.lock().unwrap();
        let mut hit = false;
        for r in records.iter_mut() {
            if r.txid == txid
                && r.output_index == output_index
                && r.spending_txid.as_deref() == Some(spending_txid)
            {
                r.spent = true;
                r.spent_confirmed = true;
                if let Some(h) = spent_height {
                    r.spent_height = Some(h);
                }
                hit = true;
                break;
            }
        }
        drop(records);
        // #2b: a CAS HIT is a confirm moment — retire superseded siblings
        // (a MISS confirmed nothing and latches nothing).
        if hit {
            self.retire_superseded_after_confirm(txid, output_index, spending_txid);
        }
        Ok(hit)
    }

    async fn find_params_undecoded(&self, limit: u64) -> Result<Vec<PotRecord>, PotStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| !r.params_decoded)
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn find_settle_signers_unlatched(
        &self,
        limit: u64,
    ) -> Result<Vec<PotRecord>, PotStorageError> {
        // The #406 candidate bar, mirrored from the D1 query: a CURRENT
        // verdict group (verdict present, keyed to the live pointer) with no
        // signer classification yet. `'unresolved'` rows are latched OUT of
        // the set (attempted with bytes in hand, no pair verified).
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| {
                r.verdict.is_some()
                    && r.verdict_txid.is_some()
                    && r.verdict_txid == r.spending_txid
                    && r.settle_signers.is_none()
            })
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn store_spender_facts(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
        facts: crate::tx_facts::TxFacts,
    ) -> Result<(), PotStorageError> {
        let mut records = self.records.lock().unwrap();
        let Some(r) = records
            .iter_mut()
            .find(|r| r.txid == txid && r.output_index == output_index)
        else {
            return Ok(()); // never admitted — nothing to describe
        };
        if r.spending_txid.as_deref() != Some(spending_txid) {
            return Ok(()); // CAS miss: the pointer moved under us
        }
        if r.spender_facts_txid.as_deref() == Some(spending_txid) {
            // same spender already described — stored-wins per value
            if r.spender_size_bytes.is_none() {
                r.spender_size_bytes = Some(facts.size_bytes);
            }
            if r.spender_fee_sats.is_none() {
                r.spender_fee_sats = facts.fee_sats;
            }
        } else {
            r.spender_facts_txid = Some(spending_txid.to_string());
            r.spender_size_bytes = Some(facts.size_bytes);
            r.spender_fee_sats = facts.fee_sats;
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
        // bsv-low#304: a VERIFIED row is authoritative — an admit-path
        // write (untrusted, submitter-supplied bytes) must never clobber a
        // chaintracks-verified proof, even when longer. Only the verifying
        // writers (`compact_pot_beef`) may rewrite it.
        if self.verified.lock().unwrap().contains(txid) {
            return Ok(());
        }
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
        // Model the D1 `WHERE proof_verified = 0` candidate set
        // (bsv-low#304): a row is a candidate until the VERIFIED latch is
        // set — a STRUCTURAL bump in the stored bytes does NOT drop it out
        // (admit-path bytes are untrusted; the pass re-verifies them).
        // #2b: LATCHED structurally-unprovable rows are retired from the
        // pool (they can never mine; the verified writers clear the latch
        // if a reorg ever proves one). Un-latched rows stay eligible.
        // The #228 backstop age gate excludes rows younger than min_age_secs
        // (their proof is expected via /arc-ingest); unknown age = eligible.
        let verified = self.verified.lock().unwrap();
        let unprovable = self.unprovable.lock().unwrap();
        let candidates: Vec<(String, Vec<u8>)> = self
            .beefs
            .lock()
            .unwrap()
            .iter()
            .filter(|(txid, _)| !verified.contains(*txid) && !unprovable.contains(*txid))
            .map(|(txid, beef)| (txid.clone(), beef.clone()))
            .collect();
        drop(unprovable);
        drop(verified);
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
        // #2b: rows whose CURRENT pointer is latched structurally
        // unprovable are excluded (mirrors `pot_spent_unconfirmed_sql`) —
        // an un-latched or unknown pointer stays a full candidate.
        let unprovable = self.unprovable.lock().unwrap();
        let candidates: Vec<PotRecord> = self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| {
                r.spent
                    && !r.spent_confirmed
                    && !r
                        .spending_txid
                        .as_deref()
                        .is_some_and(|s| unprovable.contains(s))
            })
            .cloned()
            .collect();
        drop(unprovable);
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
                r.spent && !r.spent_confirmed && r.spending_txid.as_deref() == Some(spending_txid)
            })
            .cloned()
            .collect())
    }

    async fn compact_pot_beef(&self, txid: &str, new_beef: &[u8]) -> Result<(), PotStorageError> {
        // Fail-closed: overwrite ONLY when the new beef actually proves txid
        // (its own BUMP is present ⇒ self-contained). BYPASS the longer-wins
        // guard — a bumped BEEF wins even when shorter. This is a VERIFYING
        // writer (the caller chaintracks-verified the bump before stitching),
        // so the verified latch is set (bsv-low#304).
        if !pot_beef_has_proof(txid, new_beef) {
            return Ok(());
        }
        self.beefs
            .lock()
            .unwrap()
            .insert(txid.to_string(), new_beef.to_vec());
        self.verified.lock().unwrap().insert(txid.to_string());
        // #2b confirm-beats-latch: a chaintracks-verified proof is chain
        // truth — clear any stale supersession latch (the reorg direction;
        // mirrors POT_BEEF_VERIFIED_WRITE_SQL's explicit NULL).
        self.unprovable.lock().unwrap().remove(txid);
        Ok(())
    }

    async fn mark_pot_beef_proven(&self, txid: &str) -> Result<(), PotStorageError> {
        self.verified.lock().unwrap().insert(txid.to_string());
        // #2b confirm-beats-latch (mirrors POT_BEEF_MARK_PROVEN_SQL).
        self.unprovable.lock().unwrap().remove(txid);
        Ok(())
    }

    async fn pot_beef_proof_verified(&self, txid: &str) -> Result<bool, PotStorageError> {
        Ok(self.verified.lock().unwrap().contains(txid))
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
            ..Default::default()
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
        store
            .mark_spent("potA", 0, "settleTx", false, None, None, None)
            .await
            .unwrap();

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent);
        assert_eq!(r.spending_txid.as_deref(), Some("settleTx"));
        assert!(
            !r.spent_confirmed,
            "unconfirmed spend must not latch the flag"
        );
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
        store
            .mark_spent("potA", 0, "settleTx", false, None, None, None)
            .await
            .unwrap();

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
        store
            .mark_spent("ghost", 0, "settleTx", false, None, None, None)
            .await
            .unwrap();
        store
            .mark_spent("ghost", 0, "settleTx", true, None, None, None)
            .await
            .unwrap();
        assert_eq!(store.record_count(), 0);
        assert!(store.get_spent_status("ghost", 0).await.unwrap().is_none());
    }

    /// bsv-low P4 slice 2: the spender facts land ONLY under the live
    /// pointer, are stored-wins for the same spender, and RESET when the
    /// pointer moves (a new spender never inherits the old one's facts).
    #[tokio::test]
    async fn spender_facts_are_cas_on_the_pointer_stored_wins_and_reset_on_move() {
        use crate::tx_facts::TxFacts;
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "claim1", false, None, None, None)
            .await
            .unwrap();
        // CAS miss: describing a spender that is NOT the live pointer is a no-op.
        store
            .store_spender_facts(
                "potA",
                0,
                "claim2",
                TxFacts {
                    size_bytes: 300,
                    fee_sats: Some(10),
                },
            )
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            (
                r.spender_facts_txid.as_deref(),
                r.spender_size_bytes,
                r.spender_fee_sats
            ),
            (None, None, None)
        );
        // Live pointer: lands.
        store
            .store_spender_facts(
                "potA",
                0,
                "claim1",
                TxFacts {
                    size_bytes: 300,
                    fee_sats: None,
                },
            )
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            (
                r.spender_facts_txid.as_deref(),
                r.spender_size_bytes,
                r.spender_fee_sats
            ),
            (Some("claim1"), Some(300), None)
        );
        // Same spender again: stored-wins per value — the absent fee FILLS, the size does not move.
        store
            .store_spender_facts(
                "potA",
                0,
                "claim1",
                TxFacts {
                    size_bytes: 999,
                    fee_sats: Some(10),
                },
            )
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            (r.spender_size_bytes, r.spender_fee_sats),
            (Some(300), Some(10))
        );
        // The pointer moves: the new spender's facts RESET the pair (fee None stays None).
        store
            .mark_spent("potA", 0, "claim2", false, None, None, None)
            .await
            .unwrap();
        store
            .store_spender_facts(
                "potA",
                0,
                "claim2",
                TxFacts {
                    size_bytes: 400,
                    fee_sats: None,
                },
            )
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            (
                r.spender_facts_txid.as_deref(),
                r.spender_size_bytes,
                r.spender_fee_sats
            ),
            (Some("claim2"), Some(400), None)
        );
        // A never-admitted outpoint: no-op, no phantom row.
        store
            .store_spender_facts(
                "ghost",
                0,
                "x",
                TxFacts {
                    size_bytes: 1,
                    fee_sats: None,
                },
            )
            .await
            .unwrap();
        assert!(store.get_spent_status("ghost", 0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn distinct_outpoints_tracked_independently() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.store_record(&pot_record("potB", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "settleA", false, None, None, None)
            .await
            .unwrap();

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
        store
            .mark_spent("potA", 0, "claim1", false, None, None, None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent);
        assert_eq!(r.spending_txid.as_deref(), Some("claim1"));
        assert!(!r.spent_confirmed);

        // A second unconfirmed claim by a DIFFERENT spender overwrites —
        // last-writer-wins among unconfirmed is deliberately preserved so an
        // honest later submit can still set the pointer.
        store
            .mark_spent("potA", 0, "claim2", false, None, None, None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("claim2"));
        assert!(!r.spent_confirmed);
    }

    #[tokio::test]
    async fn confirmed_spend_latches_flag() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "settleTx", true, None, None, None)
            .await
            .unwrap();

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent);
        assert_eq!(r.spending_txid.as_deref(), Some("settleTx"));
        assert!(r.spent_confirmed);
    }

    #[tokio::test]
    async fn unconfirmed_never_clobbers_confirmed_pointer() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "realSettle", true, None, None, None)
            .await
            .unwrap();

        // An attacker's unconfirmed claim must be REFUSED: pointer AND flag
        // unchanged.
        store
            .mark_spent("potA", 0, "forgedSpend", false, None, None, None)
            .await
            .unwrap();
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
        store
            .mark_spent("potA", 0, "settle1", true, None, None, None)
            .await
            .unwrap();

        // A later CONFIRMED spend (e.g. reorg / better proof) still writes —
        // chain truth is last-confirmed-wins.
        store
            .mark_spent("potA", 0, "settle2", true, None, None, None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("settle2"));
        assert!(r.spent_confirmed);
    }

    #[tokio::test]
    async fn confirmed_overwrites_unconfirmed_claim() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "unconfirmedClaim", false, None, None, None)
            .await
            .unwrap();

        // The confirmed spend replaces the unconfirmed pointer and latches
        // the flag.
        store
            .mark_spent("potA", 0, "realSettle", true, None, None, None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("realSettle"));
        assert!(r.spent_confirmed);
    }

    #[tokio::test]
    async fn store_never_clobbers_confirmed_flag_on_readmission() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "settleTx", true, None, None, None)
            .await
            .unwrap();

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
        assert_eq!(
            store.get_beef("funding").await.unwrap().as_deref(),
            Some(&[1u8][..])
        );
        assert_eq!(
            store.get_beef("settle").await.unwrap().as_deref(),
            Some(&[2u8, 2][..])
        );
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
        let bump = MerklePath::new(
            800_000,
            vec![vec![MerklePathLeaf::new_txid(0, txid.clone())]],
        )
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
        assert!(
            !pot_beef_has_proof(&txid, &proofless_long),
            "fixture is proofless"
        );
        assert!(
            pot_beef_has_proof(&txid, &proven_short),
            "fixture is proven"
        );
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
        store
            .compact_pot_beef(&txid, &proofless_other)
            .await
            .unwrap();
        assert_eq!(
            store.get_beef(&txid).await.unwrap().as_deref(),
            Some(proofless_long.as_slice()),
            "a proofless compact is a no-op"
        );
    }

    #[tokio::test]
    async fn find_pot_beefs_candidacy_is_the_verified_latch_not_structure() {
        // bsv-low#304: an ADMIT-path row stays a completion candidate even
        // when its stored bytes STRUCTURALLY carry a bump — admit bytes are
        // submitter-supplied with zero SPV (a fake bump must not
        // self-exempt from re-verification). Only the VERIFYING writers
        // (`mark_pot_beef_proven` / `compact_pot_beef`) drop a row out.
        let store = MemoryPotStorage::new();
        let (proofless, proofless_txid) = proofless_beef_with_filler(RAW_A, None);
        let (structurally_bumped, bumped_txid) = proven_beef(RAW_B);
        assert_ne!(proofless_txid, bumped_txid);
        assert!(pot_beef_has_proof(&bumped_txid, &structurally_bumped));

        store.store_beef(&proofless_txid, &proofless).await.unwrap();
        store
            .store_beef(&bumped_txid, &structurally_bumped)
            .await
            .unwrap();
        assert_eq!(store.beef_count(), 2);

        // BOTH rows are candidates — structure alone settles nothing.
        let cands = store.find_pot_beefs_for_proof_check(10, 0).await.unwrap();
        assert_eq!(
            cands.len(),
            2,
            "a structurally-bumped admit row must stay a candidate until verified"
        );

        // The verified latch (set only after a chaintracks re-verify) drops
        // the row out without touching its bytes.
        store.mark_pot_beef_proven(&bumped_txid).await.unwrap();
        let cands = store.find_pot_beefs_for_proof_check(10, 0).await.unwrap();
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].0, proofless_txid);

        // …and the verifying compact write latches too.
        let (proven_a, txid_a) = proven_beef(RAW_A);
        assert_eq!(txid_a, proofless_txid);
        store.compact_pot_beef(&txid_a, &proven_a).await.unwrap();
        assert!(store
            .find_pot_beefs_for_proof_check(10, 0)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn admit_write_never_clobbers_a_verified_row() {
        // bsv-low#304 ("never weaken existing verified answers"): once a
        // row's proof is chaintracks-verified, an UNTRUSTED admit-path
        // store_beef — even a strictly LONGER one, which the longer-wins
        // guard would otherwise accept — must not replace the bytes.
        let store = MemoryPotStorage::new();
        let (proven_short, txid) = proven_beef(RAW_A);
        let (proofless_long, txid2) = proofless_beef_with_filler(RAW_A, Some(RAW_B));
        assert_eq!(txid, txid2);
        assert!(proofless_long.len() > proven_short.len());

        store.compact_pot_beef(&txid, &proven_short).await.unwrap();
        store.store_beef(&txid, &proofless_long).await.unwrap();
        assert_eq!(
            store.get_beef(&txid).await.unwrap().as_deref(),
            Some(proven_short.as_slice()),
            "an admit write must not replace a verified row's bytes"
        );
    }

    // ── find_spent_unconfirmed / spend-confirmation chaser (#186) ─────────

    #[tokio::test]
    async fn find_spent_unconfirmed_surfaces_only_spent_unconfirmed() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.store_record(&pot_record("potB", 0)).await.unwrap();
        store.store_record(&pot_record("potC", 0)).await.unwrap();

        // potA: spent, unconfirmed → a candidate.
        store
            .mark_spent("potA", 0, "settleA", false, None, None, None)
            .await
            .unwrap();
        // potB: spent, confirmed → NOT a candidate.
        store
            .mark_spent("potB", 0, "settleB", true, None, None, None)
            .await
            .unwrap();
        // potC: never spent → NOT a candidate.

        let cands = store.find_spent_unconfirmed(10, 0).await.unwrap();
        assert_eq!(
            cands.len(),
            1,
            "only the spent-unconfirmed row is a candidate"
        );
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
        assert!(store
            .find_spent_unconfirmed(10, 0)
            .await
            .unwrap()
            .is_empty());
        // An unspent admitted row is still not a candidate.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        assert!(store
            .find_spent_unconfirmed(10, 0)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn spend_confirmation_upgrade_and_never_downgrade() {
        // Frames the mark_spent invariant through the candidate query: the
        // chaser's confirmed upgrade removes the row from the candidate set and
        // a later unconfirmed claim can never downgrade it.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();

        // 0-conf spend recorded → appears as a candidate.
        store
            .mark_spent("potA", 0, "settle", false, None, None, None)
            .await
            .unwrap();
        assert_eq!(store.find_spent_unconfirmed(10, 0).await.unwrap().len(), 1);

        // The chaser's upgrade (a chaintracks-verified spend).
        store
            .mark_spent("potA", 0, "settle", true, None, None, None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed, "confirmed spend latches the flag");
        assert!(
            store
                .find_spent_unconfirmed(10, 0)
                .await
                .unwrap()
                .is_empty(),
            "a confirmed row is no longer a candidate"
        );

        // A later unconfirmed (forged) claim must NOT downgrade the row back
        // into the candidate set.
        store
            .mark_spent("potA", 0, "forged", false, None, None, None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed, "confirmed flag survives");
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("settle"),
            "pointer unchanged"
        );
        assert!(
            store
                .find_spent_unconfirmed(10, 0)
                .await
                .unwrap()
                .is_empty(),
            "an unconfirmed claim never re-surfaces a confirmed row"
        );
    }

    #[tokio::test]
    async fn find_spent_unconfirmed_respects_limit() {
        let store = MemoryPotStorage::new();
        for i in 0..5u32 {
            let txid = format!("pot{i}");
            store.store_record(&pot_record(&txid, 0)).await.unwrap();
            store
                .mark_spent(&txid, 0, "settle", false, None, None, None)
                .await
                .unwrap();
        }
        assert_eq!(store.find_spent_unconfirmed(2, 0).await.unwrap().len(), 2);
        assert_eq!(store.find_spent_unconfirmed(10, 0).await.unwrap().len(), 5);
    }

    // ── #228: push-consumer lookup + backstop age gates ──────────────────

    #[tokio::test]
    async fn find_unconfirmed_by_spending_txid_returns_only_that_spenders_rows() {
        let store = MemoryPotStorage::new();
        for (pot, spender) in [
            ("potA", "settleX"),
            ("potB", "settleX"),
            ("potC", "settleY"),
        ] {
            store.store_record(&pot_record(pot, 0)).await.unwrap();
            store
                .mark_spent(pot, 0, spender, false, None, None, None)
                .await
                .unwrap();
        }
        // A CONFIRMED settleX row is not a candidate (nothing left to latch).
        store.store_record(&pot_record("potD", 0)).await.unwrap();
        store
            .mark_spent("potD", 0, "settleX", true, None, None, None)
            .await
            .unwrap();
        // An unspent row never appears.
        store.store_record(&pot_record("potE", 0)).await.unwrap();

        let rows = store
            .find_unconfirmed_by_spending_txid("settleX")
            .await
            .unwrap();
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
        store
            .mark_spent("potA", 0, "settle", false, None, None, None)
            .await
            .unwrap();

        assert!(
            store
                .find_spent_unconfirmed(10, 1800)
                .await
                .unwrap()
                .is_empty(),
            "a fresh spend on an old pot still waits for its push"
        );
        store.advance_clock(1800);
        assert_eq!(
            store.find_spent_unconfirmed(10, 1800).await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn spend_age_gate_resets_when_a_new_spender_overwrites() {
        // Last-writer-wins among unconfirmed claims: the NEW pointer's push
        // deserves its own window, so an accepted overwrite resets the age.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "claim1", false, None, None, None)
            .await
            .unwrap();
        store.advance_clock(1800); // claim1 is now old enough
        assert_eq!(
            store.find_spent_unconfirmed(10, 1800).await.unwrap().len(),
            1
        );

        store
            .mark_spent("potA", 0, "claim2", false, None, None, None)
            .await
            .unwrap();
        assert!(
            store
                .find_spent_unconfirmed(10, 1800)
                .await
                .unwrap()
                .is_empty(),
            "the new pointer restarts the backstop window"
        );
    }

    #[tokio::test]
    async fn zero_min_age_disables_both_gates() {
        // min_age_secs = 0 is the pre-#228 behaviour: everything eligible
        // immediately (also the escape hatch if the gate must be turned off).
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "settle", false, None, None, None)
            .await
            .unwrap();
        store.store_beef("beefTx", &[1, 2, 3]).await.unwrap();

        assert_eq!(store.find_spent_unconfirmed(10, 0).await.unwrap().len(), 1);
        assert_eq!(
            store
                .find_pot_beefs_for_proof_check(10, 0)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    // ── #284 decoded columns: upsert / verdict atomicity / backfill scan ──

    /// A decoded covenant record for `(txid, vout)` (sample committed
    /// params, hex-encoded as admission stores them).
    fn decoded_record(txid: &str, vout: u32) -> PotRecord {
        PotRecord {
            txid: txid.into(),
            output_index: vout,
            lock_kind: Some("covenant".into()),
            pub_a: Some("02".repeat(33)),
            pub_b: Some("03".repeat(33)),
            pub_tower: Some("04".repeat(33)),
            pay_pkh_a: Some("aa".repeat(20)),
            pay_pkh_b: Some("bb".repeat(20)),
            rake_pkh: Some("cc".repeat(20)),
            stake_a: Some(2000),
            stake_b: Some(2000),
            fee_sats: Some(400),
            recovery_height: Some(956_656),
            pot_sats: Some(4000),
            params_decoded: true,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn re_store_backfills_decoded_columns_but_never_touches_spend_state() {
        let store = MemoryPotStorage::new();
        // A pre-#284 admission (no decoded columns), then a CONFIRMED spend
        // with a verdict — the exact row state a backfill upsert meets.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent(
                "potA",
                0,
                "settleTx",
                true,
                Some(VerdictWrite::bare("winner-a")),
                Some(800_000),
                None,
            )
            .await
            .unwrap();
        let before = store.get_spent_status("potA", 0).await.unwrap().unwrap();

        // The backfill/re-admission upsert: decoded columns arrive.
        store
            .store_record(&decoded_record("potA", 0))
            .await
            .unwrap();
        let after = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        // Spend state byte-identical…
        assert_eq!(after.spent, before.spent);
        assert_eq!(after.spending_txid, before.spending_txid);
        assert_eq!(after.spent_confirmed, before.spent_confirmed);
        assert_eq!(after.verdict, before.verdict);
        assert_eq!(after.verdict_txid, before.verdict_txid);
        assert_eq!(after.spent_height, before.spent_height);
        // …and the decoded columns are filled.
        assert_eq!(after.lock_kind.as_deref(), Some("covenant"));
        assert_eq!(after.stake_a, Some(2000));
        assert!(after.params_decoded);
        assert!(after.decoded_covenant_params().is_some());

        // A REPLAY lacking data (a bare pre-#284-shaped record) never nulls
        // stored decoded values and never un-latches params_decoded.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        let replayed = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(replayed, after, "a data-less replay changes NOTHING");
    }

    #[tokio::test]
    async fn verdict_rides_the_pointer_atomically_and_none_never_nulls() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();

        // Verdict Some rides the accepted write; verdict_txid = the pointer.
        store
            .mark_spent(
                "potA",
                0,
                "settle1",
                false,
                Some(VerdictWrite::bare("winner-a")),
                None,
                None,
            )
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.verdict.as_deref(), Some("winner-a"));
        assert_eq!(r.verdict_txid.as_deref(), Some("settle1"));

        // A confirm-only write (verdict None) latches the flag + height but
        // leaves the stored verdict UNCHANGED.
        store
            .mark_spent("potA", 0, "settle1", true, None, Some(800_000), None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed);
        assert_eq!(r.spent_height, Some(800_000));
        assert_eq!(r.verdict.as_deref(), Some("winner-a"), "None never nulls");
        assert_eq!(r.verdict_txid.as_deref(), Some("settle1"));
    }

    #[tokio::test]
    async fn unconfirmed_writer_cannot_displace_a_confirmed_verdict() {
        // THE #284 displacement bar: a CONFIRMED verdict for spender S1,
        // then an unconfirmed mark_spent for S2 (with its own forged
        // verdict) → pointer AND verdict AND height all unchanged.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent(
                "potA",
                0,
                "realSettle",
                true,
                Some(VerdictWrite::bare("winner-a")),
                Some(800_000),
                None,
            )
            .await
            .unwrap();

        store
            .mark_spent(
                "potA",
                0,
                "forgedSpend",
                false,
                Some(VerdictWrite::bare("winner-b")),
                Some(999_999),
                None,
            )
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("realSettle"));
        assert_eq!(r.verdict.as_deref(), Some("winner-a"));
        assert_eq!(r.verdict_txid.as_deref(), Some("realSettle"));
        assert_eq!(r.spent_height, Some(800_000));
        assert!(r.spent_confirmed);
    }

    #[tokio::test]
    async fn a_new_unconfirmed_pointer_without_verdict_leaves_a_guarded_stale_verdict() {
        // Last-writer-wins among unconfirmed: S2 displaces S1's pointer with
        // verdict None → the stale S1 verdict remains but verdict_txid no
        // longer equals spending_txid, which is exactly the reader's guard.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent(
                "potA",
                0,
                "settle1",
                false,
                Some(VerdictWrite::bare("tie")),
                None,
                None,
            )
            .await
            .unwrap();
        store
            .mark_spent("potA", 0, "settle2", false, None, None, None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("settle2"));
        assert_eq!(r.verdict.as_deref(), Some("tie"), "stale by design");
        assert_eq!(
            r.verdict_txid.as_deref(),
            Some("settle1"),
            "…and observably stale: verdict_txid ≠ spending_txid"
        );
    }

    #[tokio::test]
    async fn verdict_cas_is_a_noop_when_the_pointer_moved() {
        // 2026-07-28 gate MEDIUM-2 (memory mirror of the executed-SQL test):
        // the backfill's guarded verdict write bound to a stale pointer must
        // change NOTHING once a newer (reorg-confirmed) spender landed.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "settleS1", false, None, None, None)
            .await
            .unwrap();
        store
            .mark_spent("potA", 0, "settleS2", true, None, Some(802_000), None)
            .await
            .unwrap();
        let before = store.get_spent_status("potA", 0).await.unwrap().unwrap();

        store
            .mark_verdict_for_spender("potA", 0, "settleS1", VerdictWrite::bare("winner-a"))
            .await
            .unwrap();
        let after = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(after, before, "a stale CAS write changes NOTHING");
        assert_eq!(after.verdict, None);

        // The current-pointer write lands, touching only the verdict pair.
        store
            .mark_verdict_for_spender("potA", 0, "settleS2", VerdictWrite::bare("winner-b"))
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.verdict.as_deref(), Some("winner-b"));
        assert_eq!(r.verdict_txid.as_deref(), Some("settleS2"));
        assert_eq!(r.spent_height, Some(802_000), "nothing else touched");
        assert!(r.spent_confirmed);
    }

    #[tokio::test]
    async fn confirm_cas_is_a_noop_when_the_pointer_moved() {
        // bsv-low#301 (memory mirror of the executed-SQL test, the verdict-CAS
        // sibling): the chaser's confirmed write bound to a stale pointer
        // must change NOTHING once a newer spender landed in the window —
        // pre-#301 the unguarded confirm RESET the pointer to the stale S1.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        // The chaser "reads" the row while it points at S1 (unconfirmed)…
        store
            .mark_spent("potA", 0, "settleS1", false, None, None, None)
            .await
            .unwrap();
        // …then a reorg-CONFIRMED S2 displaces S1 before the write lands.
        store
            .mark_spent("potA", 0, "settleS2", true, None, Some(802_000), None)
            .await
            .unwrap();
        let before = store.get_spent_status("potA", 0).await.unwrap().unwrap();

        let hit = store
            .mark_confirmed_for_spender("potA", 0, "settleS1", Some(800_000))
            .await
            .unwrap();
        assert!(!hit, "a moved pointer is a CAS MISS");
        let after = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(after, before, "a stale CAS confirm changes NOTHING");
        assert_eq!(after.spending_txid.as_deref(), Some("settleS2"));
        assert_eq!(
            after.spent_height,
            Some(802_000),
            "S2 never inherits S1's height"
        );
        assert!(after.spent_confirmed);
    }

    #[tokio::test]
    async fn confirm_cas_hit_latches_and_keeps_stored_height_on_none() {
        // bsv-low#301: the non-race case — the pointer is still the spender
        // the proof was verified for, so the CAS lands: confirmed latches,
        // the height writes; a later same-pointer CAS with None KEEPS the
        // stored height (the mark_spent same-pointer COALESCE semantics).
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "settleS1", false, None, None, None)
            .await
            .unwrap();

        let hit = store
            .mark_confirmed_for_spender("potA", 0, "settleS1", Some(800_000))
            .await
            .unwrap();
        assert!(hit);
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed);
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("settleS1"),
            "pointer untouched"
        );
        assert_eq!(r.spent_height, Some(800_000));
        assert_eq!(r.verdict, None, "the CAS never touches the verdict pair");

        // Same-pointer re-confirm with no height in hand → stored survives.
        assert!(store
            .mark_confirmed_for_spender("potA", 0, "settleS1", None)
            .await
            .unwrap());
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spent_height, Some(800_000));

        // An absent outpoint is a miss, never an error / phantom row.
        assert!(!store
            .mark_confirmed_for_spender("ghost", 0, "settleS1", None)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn spent_height_rides_the_pointer_never_inherited() {
        // 2026-07-28 gate LOW-1 (memory mirror): a confirmed S2 with NO
        // height must not inherit S1's — the height resets on a pointer
        // change, exactly like the verdict binding.
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "settleS1", true, None, Some(800_000), None)
            .await
            .unwrap();
        store
            .mark_spent("potA", 0, "settleS2", true, None, None, None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("settleS2"));
        assert_eq!(r.spent_height, None, "S2 never inherits S1's height");
        // Same-pointer re-confirm with None still KEEPS a height.
        store
            .mark_spent("potA", 0, "settleS2", true, None, Some(802_000), None)
            .await
            .unwrap();
        store
            .mark_spent("potA", 0, "settleS2", true, None, None, None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spent_height, Some(802_000));
    }

    #[tokio::test]
    async fn spent_height_never_rides_an_unconfirmed_write() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store
            .mark_spent("potA", 0, "settle1", false, None, Some(777_777), None)
            .await
            .unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            r.spent_height, None,
            "a height is a fact of a verified BUMP only"
        );
    }

    #[tokio::test]
    async fn params_undecoded_scan_terminates_and_respects_limit() {
        let store = MemoryPotStorage::new();
        // Two undecoded (pre-#284) rows + one decoded.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.store_record(&pot_record("potB", 0)).await.unwrap();
        store
            .store_record(&decoded_record("potC", 0))
            .await
            .unwrap();

        let cands = store.find_params_undecoded(10).await.unwrap();
        let mut txids: Vec<&str> = cands.iter().map(|r| r.txid.as_str()).collect();
        txids.sort_unstable();
        assert_eq!(
            txids,
            vec!["potA", "potB"],
            "decoded rows are never candidates"
        );
        assert_eq!(store.find_params_undecoded(1).await.unwrap().len(), 1);

        // Once decoded (even to an UNRECOGNIZED shape: lock_kind None,
        // params_decoded latched), a row leaves the candidate set forever —
        // the enumerator terminates.
        store
            .store_record(&PotRecord {
                txid: "potA".into(),
                output_index: 0,
                params_decoded: true,
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .store_record(&decoded_record("potB", 0))
            .await
            .unwrap();
        assert!(store.find_params_undecoded(10).await.unwrap().is_empty());
    }

    #[test]
    fn decoded_covenant_params_is_strict() {
        let good = decoded_record("potA", 0);
        assert!(good.decoded_covenant_params().is_some());
        // Not a covenant → None regardless of fields.
        let bare = PotRecord {
            lock_kind: Some("bare".into()),
            ..decoded_record("potA", 0)
        };
        assert!(bare.decoded_covenant_params().is_none());
        // A missing / malformed stored value → None (fall back to the BEEF).
        let missing = PotRecord {
            stake_a: None,
            ..decoded_record("potA", 0)
        };
        assert!(missing.decoded_covenant_params().is_none());
        let malformed = PotRecord {
            pub_a: Some("02aa".into()), // truncated key
            ..decoded_record("potA", 0)
        };
        assert!(malformed.decoded_covenant_params().is_none());
    }

    #[test]
    fn record_deserializes_without_decoded_fields() {
        // Backward-compat (mirrors the spentConfirmed precedent): a
        // pre-#284 serialized form still deserializes — all decoded fields
        // default to None / false.
        let r: PotRecord = serde_json::from_value(serde_json::json!({
            "txid": "potA", "outputIndex": 0, "spent": true, "spendingTxid": "settleTx"
        }))
        .unwrap();
        assert_eq!(r.lock_kind, None);
        assert!(!r.params_decoded);
        assert_eq!(r.verdict, None);
        assert_eq!(r.spent_height, None);
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

    // ── bsv-low handoff #2b: structurally-unprovable retirement (memory twin) ──

    /// A REAL beef whose subject spends the given outpoints (one P2PKH
    /// output, value salted so distinct salts give distinct txids);
    /// returns `(beef_bytes, txid)`.
    fn spending_beef(outpoints: &[(&str, u32)], salt: u8) -> (Vec<u8>, String) {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
        let mut tx = Transaction::new();
        for (txid, vout) in outpoints {
            tx.add_input(TransactionInput::new((*txid).to_string(), *vout))
                .unwrap();
        }
        tx.add_output(TransactionOutput {
            satoshis: Some(1000 + u64::from(salt)),
            locking_script: LockingScript::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap(),
            change: false,
        })
        .unwrap();
        let txid = tx.id();
        let mut beef = Beef::new();
        beef.merge_transaction(tx);
        (beef.to_binary(), txid)
    }

    /// PIN (do not weaken): an UNCONFIRMED spend record must latch NOTHING
    /// — a displaced-unconfirmed pointer can re-win (Rule 6: latching here
    /// would trade a self-healing failure for a permanent one). Only the
    /// CONFIRMED record retires the superseded sibling.
    #[tokio::test]
    async fn unconfirmed_spend_never_latches_confirmed_spend_retires_sibling() {
        let store = MemoryPotStorage::new();
        let pot = "aa".repeat(32);
        store.store_record(&pot_record(&pot, 0)).await.unwrap();
        let (settle_beef, settle_txid) = spending_beef(&[(&pot, 0)], 1);
        let (refund_beef, refund_txid) = spending_beef(&[(&pot, 0)], 2);
        store.store_beef(&settle_txid, &settle_beef).await.unwrap();
        store.store_beef(&refund_txid, &refund_beef).await.unwrap();

        // UNCONFIRMED record (the 0-conf submit): nothing latched, both
        // spenders stay full poll candidates.
        store
            .mark_spent(&pot, 0, &settle_txid, false, None, None, None)
            .await
            .unwrap();
        assert!(
            !store.is_structurally_unprovable(&refund_txid)
                && !store.is_structurally_unprovable(&settle_txid),
            "an UNCONFIRMED displacement must never latch (Rule 6)"
        );
        assert_eq!(
            store
                .find_pot_beefs_for_proof_check(10, 0)
                .await
                .unwrap()
                .len(),
            2
        );

        // CONFIRMED record: the sibling refund is retired, the confirmed
        // spender is not, and the pool excludes exactly the retired row.
        store
            .mark_spent(&pot, 0, &settle_txid, true, None, Some(830_000), None)
            .await
            .unwrap();
        assert!(store.is_structurally_unprovable(&refund_txid));
        assert!(!store.is_structurally_unprovable(&settle_txid));
        let pool: Vec<String> = store
            .find_pot_beefs_for_proof_check(10, 0)
            .await
            .unwrap()
            .into_iter()
            .map(|(t, _)| t)
            .collect();
        assert!(!pool.contains(&refund_txid));
        assert!(pool.contains(&settle_txid));
    }

    /// The #301 CAS confirm: a HIT retires the superseded sibling; a MISS
    /// (pointer moved) confirmed nothing and must latch nothing.
    #[tokio::test]
    async fn cas_hit_retires_sibling_and_cas_miss_latches_nothing() {
        let store = MemoryPotStorage::new();
        let pot = "bb".repeat(32);
        store.store_record(&pot_record(&pot, 0)).await.unwrap();
        let (settle_beef, settle_txid) = spending_beef(&[(&pot, 0)], 1);
        let (refund_beef, refund_txid) = spending_beef(&[(&pot, 0)], 2);
        store.store_beef(&settle_txid, &settle_beef).await.unwrap();
        store.store_beef(&refund_txid, &refund_beef).await.unwrap();
        store
            .mark_spent(&pot, 0, &settle_txid, false, None, None, None)
            .await
            .unwrap();

        // MISS: the proof was verified for a spender that is no longer the
        // pointer — nothing confirmed, nothing latched.
        assert!(!store
            .mark_confirmed_for_spender(&pot, 0, &refund_txid, None)
            .await
            .unwrap());
        assert!(
            !store.is_structurally_unprovable(&settle_txid)
                && !store.is_structurally_unprovable(&refund_txid),
            "a CAS MISS confirmed nothing and must latch nothing"
        );

        // HIT: the current pointer's proof verified — the sibling retires.
        assert!(store
            .mark_confirmed_for_spender(&pot, 0, &settle_txid, Some(830_000))
            .await
            .unwrap());
        assert!(store.is_structurally_unprovable(&refund_txid));
        assert!(!store.is_structurally_unprovable(&settle_txid));
    }

    /// Confirm beats the latch, and the chaser pool honors the latch on the
    /// POINTER (the crafted multi-pot-input class): R spends potX:0 AND
    /// potY:0; potX confirms S ⇒ R latched ⇒ potY's row (still pointing at
    /// R) leaves the chaser pool; a verified writer clearing R re-admits it.
    #[tokio::test]
    async fn chaser_pool_excludes_latched_pointer_and_verified_writer_readmits() {
        let store = MemoryPotStorage::new();
        let pot_x = "cc".repeat(32);
        let pot_y = "dd".repeat(32);
        store.store_record(&pot_record(&pot_x, 0)).await.unwrap();
        store.store_record(&pot_record(&pot_y, 0)).await.unwrap();
        let (settle_beef, settle_txid) = spending_beef(&[(&pot_x, 0)], 1);
        let (r_beef, r_txid) = spending_beef(&[(&pot_x, 0), (&pot_y, 0)], 2);
        store.store_beef(&settle_txid, &settle_beef).await.unwrap();
        store.store_beef(&r_txid, &r_beef).await.unwrap();

        // potY's row points at R (unconfirmed claim).
        store
            .mark_spent(&pot_y, 0, &r_txid, false, None, None, None)
            .await
            .unwrap();
        assert_eq!(store.find_spent_unconfirmed(10, 0).await.unwrap().len(), 1);

        // potX confirms S ⇒ R (which verifiably conflicts) is latched, and
        // potY's row leaves the chaser pool with it.
        store
            .mark_spent(&pot_x, 0, &settle_txid, true, None, Some(830_000), None)
            .await
            .unwrap();
        assert!(store.is_structurally_unprovable(&r_txid));
        assert!(
            store
                .find_spent_unconfirmed(10, 0)
                .await
                .unwrap()
                .is_empty(),
            "a row pointing at a latched spender is retired from the chaser pool"
        );

        // Reorg direction: a verified writer proving R clears the latch and
        // the chaser resumes (self-healing preserved — Rule 6).
        store.mark_pot_beef_proven(&r_txid).await.unwrap();
        assert!(!store.is_structurally_unprovable(&r_txid));
        assert_eq!(
            store.find_spent_unconfirmed(10, 0).await.unwrap().len(),
            1,
            "clearing the latch re-admits the pointed-at row"
        );
    }
}

/// A `tm_lowfund` hop / change row (`lock_kind = "p2pkh"`) — indexed for the
/// hop's landing proof, never a pot. The janitor's candidate order puts these
/// LAST (`find_unspent_stale`): the money is in the covenant rows.
pub fn is_hop_row(r: &PotRecord) -> bool {
    r.lock_kind.as_deref() == Some("p2pkh")
}
