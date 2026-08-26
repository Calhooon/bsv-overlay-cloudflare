//! `ChainProofFetcher` — the god-tier courier-ladder [`AncestorFetcher`] for
//! BEEF proof completion (#192/#193).
//!
//! Per-txid, first VERIFIED wins, FAIL-CLOSED. This is the proof source the
//! engine's `complete_missing_proofs` cron (P2) and the pot-store compaction
//! tick (P3) call to turn a proofless stored BEEF into a proven one.
//!
//! ## the invariant
//!
//! > A merkle BUMP is a FACT only once its computed root is verified against our
//! > PoW-anchored header source (chaintracks). No proof is ever accepted on a
//! > courier's word — ARC/Arcade/WoC/Bitails are all re-verified. Any hiccup
//! > (no tracker / compute error / tracker error / tracker `false`) is treated
//! > as UNMINED (`proof: None`, retry next tick), never a positive.
//!
//! ## courier ladder (per docs/BEEF-COMPACTION-DESIGN.md §"the god-tier fetcher")
//!
//! Order matters: WhatsOnChain 429s on the free tier, so it is BREAK-GLASS ONLY
//! (last resort) — it must never sit on the hot path.
//!
//! 1. **Arcade** (PRIMARY — LOW broadcasts via Arcade, so Arcade has our own
//!    txs' status + free BUMP): `GET /tx/{txid}` → if `txStatus == MINED` and a
//!    `merklePath` (a ready BRC-74 BUMP) is present.
//! 2. **Bitails** (SECONDARY): `GET /tx/{txid}/proof/tsc` (TSC JSON) + height
//!    from `GET /tx/{txid}` → [`tsc_json_to_bump_hex`].
//! 3. **WhatsOnChain** (BREAK-GLASS, LAST RESORT ONLY): `GET /tx/{txid}/proof/tsc`
//!    (TSC JSON) + height from `GET /tx/hash/{txid}`.
//!
//! ## wasm safety
//!
//! Every network call goes through `worker::Fetch` — no `reqwest` / `std::time`
//! / `tokio` — so this stays `wasm32-unknown-unknown`-clean. bsv-rs is used only
//! for the wasm-clean `transaction` surface.

use std::cell::Cell;
use std::rc::Rc;

use async_trait::async_trait;
use bsv_rs::transaction::{Beef, ChainTracker, MerklePath, MerklePathLeaf, Transaction};
use overlay_engine::gasp::{AncestorFetcher, FetchedAncestor, GASPError};

/// WoC mainnet base URL (mainnet only).
pub const DEFAULT_WOC_BASE: &str = "https://api.whatsonchain.com/v1/bsv/main";

/// Bitails mainnet base URL.
pub const DEFAULT_BITAILS_BASE: &str = "https://api.bitails.io";

/// Default live Arcade V2 mainnet endpoint.
pub const DEFAULT_ARCADE_URL: &str = "https://arcade-v2-us-1.bsvblockchain.tech";
/// BananaBlocks (GorillaPool's independent explorer, JungleBus-backed with a
/// complete UTXO/spend index) — the spender-hint ladder's FIRST rung.
pub const DEFAULT_BANANABLOCKS_BASE: &str = "https://bananablocks.com/api/v1";

/// Per-tick fetch budget — bounds a single Worker invocation under the CF
/// subrequest cap. Each proofless candidate costs a handful of subrequests
/// (raw + ≤3 courier probes + a height lookup), so ~40 keeps a tick well under
/// the cap. The candidate query is `RANDOM()`-ordered upstream so a stuck head
/// never starves the queue.
pub const DEFAULT_FETCH_BUDGET: u32 = 40;

/// Candidate page for ONE pot-beef proof-completion pass (bsv-low#304 gate
/// M-2, re-derived per gate M-4). The page must be big enough that the
/// post-#304 re-verify BACKLOG (every pre-existing mined pot row re-entered
/// the candidate set when candidacy moved to the verified latch) drains in
/// HOURS, not weeks — while the backlog lasts, /tx-any's index leg defers
/// those rows to the external WoC-first leg, which must never become a warm
/// path (the 429 doctrine).
///
/// DRAIN MATH: the fast path costs ONE chaintracks service-binding read per
/// structurally-bumped candidate — no courier fetch, no byte rewrite, and
/// (M-4) the verified-latch writes are BATCHED (`mark_pot_beefs_proven`,
/// one D1 statement per 100 rows, not one per row). At 100 rows/tick × 96
/// ticks/day, a ~3,000-row backlog clears in ~30 ticks ≈ 7.5 h (vs the old
/// 20/tick: ≥150 ticks ≈ 1.6 days FLOOR, stretched to 1-2 weeks by RANDOM
/// sampling against still-unmined rows sharing the pool).
///
/// OP BUDGET (reads + writes + overhead, the conservative counting — gate
/// M-4 corrected the earlier ≈170 claim which omitted writes/migrations):
/// warm-isolate worst case ≈ 1 candidate scan + ≤100 chaintracks reads +
/// ≤1 batched latch write + ≤DEFAULT_FETCH_BUDGET(40) budgeted courier
/// candidates (each a fetch + verify) + ≤40 compact writes ≈ 182 ops; a
/// COLD isolate adds the one-time 92-statement migration pass ≈ 274. Both
/// sit under the paid 1,000-subrequest cap with the tick's other steps
/// (GASP 240 s bounded, crawl, janitor, #273 backstop ≤48 probes) sharing
/// the rest; unbatched, the same page would have cost ~100 extra writes.
/// One-shot post-deploy drains go faster via /admin/reverifyPotBeefs.
pub const POT_PROOF_PASS_LIMIT: u64 = 100;

/// Default / max candidate page for the operator-driven one-shot backlog
/// drain (`POST /admin/reverifyPotBeefs`). Its fetcher runs with budget 0
/// (chaintracks-only — never a courier fetch), so per gate M-4's
/// reads+writes+migration accounting: default 250 ≈ 1 scan + ≤250
/// chaintracks reads + ≤3 batched latch writes ≈ 254 ops warm (+92
/// migration statements on a cold isolate ≈ 346); cap 450 ≈ 456 warm /
/// ≈ 548 cold — both comfortably under the paid 1,000-subrequest wall
/// (the earlier 500/900 figures sat ~2× over it under this conservative
/// counting and were lowered).
pub const ADMIN_REVERIFY_DEFAULT_LIMIT: u64 = 250;
/// Hard cap for `/admin/reverifyPotBeefs?limit=` (see above).
pub const ADMIN_REVERIFY_MAX_LIMIT: u64 = 450;

/// Push-primary BACKSTOP age gate (bsv-low #228 / arcade#259): the poll
/// passes only touch rows OLDER than this — younger rows are expected to get
/// their proof via the Arcade MINED webhook (`/arc-ingest`), the PRIMARY
/// proof source.
///
/// ## why 30 minutes
///
/// The webhook's demonstrated push latency is ~150 ms post-MINED (#259 live
/// evidence, 2026-07-22), which is negligible — the governing timescale for
/// "the push has had its chance" is the BLOCK interval: BSV blocks are
/// Poisson with a 10-minute mean. N = 30 min = 3× the mean interval, so:
/// - P(tx still unmined at age N) = e⁻³ ≈ 5% → ≥95% of healthy txs mine AND
///   receive their pushed proof before ever becoming poll-eligible (polling
///   them earlier is pure wasted budget: unmined ⇒ no proof exists yet;
///   mined ⇒ the push already latched it and the candidate query skips it);
/// - as a safety multiple over the push latency itself it is ~12,000×, so a
///   merely-slow webhook can never lose its window to the poller;
/// - a LOST webhook (Arcade outage, dropped callback) is still recovered by
///   the backstop within N + one completion tick (~15 min) ≈ 45 min — the
///   same order as the pre-#228 all-polling latency for a typical mine.
///
/// The poll path is NEVER removed: an old-enough proofless row is always
/// polled, so total webhook loss degrades to today's behaviour (polling),
/// never to nothing — the fail-safe direction. Rows with unknown age
/// (pre-migration NULL stamps) are always eligible, same direction.
pub const PUSH_BACKSTOP_MIN_AGE_SECS: u64 = 30 * 60;

/// `AncestorFetcher` backed by the Arcade→Bitails→WoC courier ladder (WoC is
/// break-glass/last-resort) with a mandatory chaintracks re-verify before ANY
/// bump is returned.
pub struct ChainProofFetcher {
    arcade_url: String,
    bananablocks_base: String,
    woc_base: String,
    bitails_base: String,
    woc_api_key: Option<String>,
    /// PoW-anchored header source. Without it, NO bump can ever be verified →
    /// every proof is `None` (fail-closed). Never accept a proof on a courier's
    /// word.
    tracker: Option<Rc<dyn ChainTracker>>,
    /// Per-tick fetch budget (remaining).
    budget: Cell<u32>,
}

impl ChainProofFetcher {
    /// Build a fetcher over the default courier endpoints. `tracker` is the
    /// chaintracks header source used to verify every bump; `None` makes the
    /// fetcher a pure retry (no proof can ever be verified).
    pub fn new(tracker: Option<Rc<dyn ChainTracker>>) -> Self {
        Self {
            arcade_url: DEFAULT_ARCADE_URL.to_string(),
            bananablocks_base: DEFAULT_BANANABLOCKS_BASE.to_string(),
            woc_base: DEFAULT_WOC_BASE.to_string(),
            bitails_base: DEFAULT_BITAILS_BASE.to_string(),
            woc_api_key: None,
            tracker,
            budget: Cell::new(DEFAULT_FETCH_BUDGET),
        }
    }

    /// Override the Arcade endpoint (default `arcade-v2-us-1.bsvblockchain.tech`).
    #[must_use]
    pub fn with_arcade_url(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        if !url.trim().is_empty() {
            self.arcade_url = url.trim_end_matches('/').to_string();
        }
        self
    }

    /// Attach a WoC api key (lifts the free-tier rate limit).
    #[must_use]
    pub fn with_woc_api_key(mut self, key: Option<String>) -> Self {
        self.woc_api_key = key.filter(|k| !k.is_empty());
        self
    }

    /// Override the per-tick fetch budget.
    #[must_use]
    pub fn with_budget(self, budget: u32) -> Self {
        self.budget.set(budget);
        self
    }

    /// Run the courier ladder for `txid` and return the FIRST verified BUMP hex,
    /// or `None` if no courier yields a bump that verifies against chaintracks
    /// (unmined, or an unverifiable/forged proof — both fail-closed to `None`).
    /// The courier ladder. `Err` = a chaintracks READ FAULT while verifying a
    /// candidate bump (bsv-low#304 gate M-5 — one failing header read will
    /// fail for every rung, so it propagates immediately as retryable);
    /// courier fetch failures stay `Ok(None)`-shaped (honest unknown).
    async fn fetch_verified_proof(&self, txid: &str) -> Result<Option<String>, String> {
        let tracker = self.tracker.as_deref();

        // 1. Arcade — our own broadcaster's free BUMP (MINED status merklePath).
        if let Some(bump_hex) = self.arcade_merklepath(txid).await {
            if verify_bump_detailed(tracker, &bump_hex, txid).await? {
                return Ok(Some(bump_hex));
            }
            worker::console_log!("[proof] arcade bump for {txid} FAILED chaintracks verify");
        }

        // 2. Bitails TSC (secondary — tx mined outside Arcade).
        match self.bitails_tsc_bump(txid).await {
            Some(bump_hex) => {
                if verify_bump_detailed(tracker, &bump_hex, txid).await? {
                    return Ok(Some(bump_hex));
                }
                worker::console_log!("[proof] bitails bump for {txid} FAILED chaintracks verify");
            }
            None => worker::console_log!(
                "[proof] bitails returned NO bump for {txid} (tracker_present={})",
                tracker.is_some()
            ),
        }

        // 3. WoC TSC (BREAK-GLASS, last resort — WoC 429s on the free tier).
        if let Some(bump_hex) = self.woc_tsc_bump(txid).await {
            if verify_bump_detailed(tracker, &bump_hex, txid).await? {
                return Ok(Some(bump_hex));
            }
            worker::console_log!("[proof] woc bump for {txid} FAILED chaintracks verify");
        }

        Ok(None)
    }

    /// Arcade `GET /tx/{txid}` → the BUMP hex when the tx is MINED and a
    /// `merklePath` is present, else `None`.
    async fn arcade_merklepath(&self, txid: &str) -> Option<String> {
        let url = format!("{}/tx/{}", self.arcade_url, txid);
        let (status, body) = http_get(&url, None).await.ok()?;
        if !(200..300).contains(&status) {
            return None;
        }
        parse_arcade_merklepath(&body)
    }

    /// WoC `GET /tx/{txid}/proof/tsc` (TSC JSON) + height from
    /// `GET /tx/hash/{txid}` → a BRC-74 BUMP hex, else `None`.
    async fn woc_tsc_bump(&self, txid: &str) -> Option<String> {
        let height = self.woc_block_height(txid).await?;
        let url = format!("{}/tx/{}/proof/tsc", self.woc_base, txid);
        let hdr = self.woc_api_key.as_deref().map(|k| ("woc-api-key", k));
        let (status, body) = http_get(&url, hdr).await.ok()?;
        if !(200..300).contains(&status) {
            return None;
        }
        tsc_body_to_bump_hex(&body, height)
    }

    /// WoC block height for `txid` (`GET /tx/hash/{txid}` → `blockheight`), or
    /// `None` if unmined / unknown.
    async fn woc_block_height(&self, txid: &str) -> Option<u32> {
        let url = format!("{}/tx/hash/{}", self.woc_base, txid);
        let hdr = self.woc_api_key.as_deref().map(|k| ("woc-api-key", k));
        let (status, body) = http_get(&url, hdr).await.ok()?;
        if !(200..300).contains(&status) {
            return None;
        }
        let v: serde_json::Value = serde_json::from_str(&body).ok()?;
        let h = v.get("blockheight").and_then(|h| h.as_u64())?;
        if h == 0 {
            return None; // 0 → unconfirmed / mempool.
        }
        u32::try_from(h).ok()
    }

    /// Bitails `GET /tx/{txid}/proof/tsc` (same TSC shape as WoC).
    async fn bitails_tsc_bump(&self, txid: &str) -> Option<String> {
        let height = self.bitails_block_height(txid).await?;
        let url = format!("{}/tx/{}/proof/tsc", self.bitails_base, txid);
        let (status, body) = http_get(&url, None).await.ok()?;
        if !(200..300).contains(&status) {
            return None;
        }
        tsc_body_to_bump_hex(&body, height)
    }

    /// Bitails block height for `txid` (`GET /tx/{txid}` → `blockHeight`).
    async fn bitails_block_height(&self, txid: &str) -> Option<u32> {
        let url = format!("{}/tx/{}", self.bitails_base, txid);
        let (status, body) = http_get(&url, None).await.ok()?;
        if !(200..300).contains(&status) {
            return None;
        }
        let v: serde_json::Value = serde_json::from_str(&body).ok()?;
        // Bitails returns ALL-LOWERCASE `blockheight` (verified live:
        // {"txid":…,"blockhash":…,"blockheight":913691,…}). Reading only the
        // camelCase `blockHeight` made this return None for EVERY tx, which
        // short-circuited `bitails_tsc_bump` before it ever fetched the proof —
        // silently starving both the pot-beef proof pass and the #186 spend
        // chaser (both sat at 0 completions). Accept both spellings.
        let h = v
            .get("blockheight")
            .or_else(|| v.get("blockHeight"))
            .and_then(|h| h.as_u64())?;
        if h == 0 {
            return None;
        }
        u32::try_from(h).ok()
    }

    /// Fetch the raw tx hex for `txid`, content-addressed, trying Bitails FIRST
    /// and WhatsOnChain only as a LAST RESORT (WoC 429s on the free tier, so it
    /// must never sit on the hot path). Used ONLY by the GASP-sync trait path
    /// ([`AncestorFetcher::fetch_ancestor`]) where the raw genuinely is needed;
    /// the proof-completion passes take the raw-free [`Self::verified_proof_for`].
    async fn fetch_raw_hex(&self, txid: &str) -> Result<String, GASPError> {
        // 1. Bitails raw download (non-WoC primary).
        let bitails = format!("{}/download/tx/{}/hex", self.bitails_base, txid);
        if let Some(raw) = self.raw_hex_content_addressed(txid, &bitails, None).await {
            return Ok(raw);
        }
        // 2. WoC break-glass (last resort).
        let woc = format!("{}/tx/{}/hex", self.woc_base, txid);
        let hdr = self.woc_api_key.as_deref().map(|k| ("woc-api-key", k));
        if let Some(raw) = self.raw_hex_content_addressed(txid, &woc, hdr).await {
            return Ok(raw);
        }
        Err(GASPError::NodeNotFound(format!(
            "no raw tx for {txid} (bitails + woc exhausted)"
        )))
    }

    /// GET raw tx hex from `url` and accept it ONLY if it parses to a tx whose
    /// id is `txid` — content-addressing, so a garbled response or a
    /// wrong-provider body can never inject a forged ancestor and the ladder
    /// safely falls through to the next provider. `None` on any
    /// transport/status/parse/mismatch.
    async fn raw_hex_content_addressed(
        &self,
        txid: &str,
        url: &str,
        header: Option<(&str, &str)>,
    ) -> Option<String> {
        let (status, body) = http_get(url, header).await.ok()?;
        if !(200..300).contains(&status) {
            return None;
        }
        let raw = body.trim().to_string();
        let recomputed = Transaction::from_hex(&raw).ok()?.id();
        if recomputed.eq_ignore_ascii_case(txid) {
            Some(raw)
        } else {
            None
        }
    }
}

impl ChainProofFetcher {
    /// The spender HINT for an outpoint — a three-rung provider LADDER in the
    /// house order (owner, 2026-08-18: "should include bitails and
    /// bananablocks"; WoC demoted to break-glass LAST):
    ///
    ///   1. BananaBlocks `GET {base}/txo/{txid}/{vout}/spend` — GorillaPool's
    ///      explorer (same family as the proof ladder's Arcade rung), a
    ///      complete UTXO/spend index. Live-verified 2026-08-18 on the
    ///      fixture: `{"spent":true,"spentTxid":"3ddda993…"}`.
    ///   2. Bitails `GET {base}/tx/{txid}/output/{vout}/spent` — the house's
    ///      second provider. Its outpoint endpoint 500s today (probed
    ///      2026-08-18; matches low-app-layer's standing comment) — a 5xx is
    ///      a FAULT that falls through, so the rung self-heals into service
    ///      the day Bitails fixes it, and schema drift on a healthy answer
    ///      self-announces as a fault (never a silent "no hint").
    ///   3. WhatsOnChain `GET {base}/tx/{txid}/{vout}/spent` — break-glass,
    ///      the same route the app-layer's proven `/spent-any` uses.
    ///
    /// Precedence mirrors the #93/#94 reveal-scan doctrine: a FOUND spender
    /// short-circuits (it is proof-gated downstream, so one provider's word
    /// is enough to TRY); "no hint" requires walking every healthy rung; a
    /// rung's fault falls through, and if NO rung yields a hint while ANY
    /// rung faulted the whole resolve is a FAULT (the caller counts it —
    /// "couldn't ask" must never read as "nobody spent it"). SERVER-side per
    /// the no-client-indexer rule. Each rung draws one unit from the per-tick
    /// budget BEFORE its HTTP; an exhausted budget is a fault, and with the
    /// ladder running in phase 2 it can only spend what the primary chase
    /// left. Steady-state volume is ~zero (hints fire only for rows stuck
    /// unconfirmed, ≤ DISPLACE_CAP_PER_TICK per tick).
    async fn spender_hint_ladder(&self, txid: &str, vout: u32) -> Result<Option<String>, String> {
        let hdr_none: Option<(&str, &str)> = None;
        let woc_hdr = self.woc_api_key.as_deref().map(|k| ("woc-api-key", k));
        let mut faults: Vec<String> = Vec::new();
        for (name, url, hdr, parse) in [
            (
                "bananablocks",
                bananablocks_spend_url(&self.bananablocks_base, txid, vout),
                hdr_none,
                parse_bananablocks_spend_body
                    as fn(u16, &str, &str) -> Result<Option<String>, String>,
            ),
            (
                "bitails",
                bitails_spent_url(&self.bitails_base, txid, vout),
                hdr_none,
                parse_bitails_spend_body as fn(u16, &str, &str) -> Result<Option<String>, String>,
            ),
            (
                "woc",
                woc_spent_url(&self.woc_base, txid, vout),
                woc_hdr,
                parse_woc_spend_body as fn(u16, &str, &str) -> Result<Option<String>, String>,
            ),
        ] {
            let remaining = self.budget.get();
            if remaining == 0 {
                faults.push(format!("{name}: per-tick fetch budget exhausted"));
                break;
            }
            self.budget.set(remaining - 1);
            match http_get(&url, hdr).await {
                Err(e) => faults.push(format!("{name}: {e}")),
                Ok((status, body)) => match parse(status, &body, txid) {
                    Ok(Some(spender)) => return Ok(Some(spender)),
                    Ok(None) => {}
                    Err(e) => faults.push(format!("{name}: {e}")),
                },
            }
        }
        if faults.is_empty() {
            Ok(None) // every healthy rung answered: nobody reports a spender
        } else {
            Err(format!(
                "no rung yielded a hint and {} faulted ({})",
                faults.len(),
                faults.join("; ")
            ))
        }
    }
}

#[async_trait(?Send)]
impl AncestorFetcher for ChainProofFetcher {
    async fn resolve_spender(&self, txid: &str, vout: u32) -> Result<Option<String>, String> {
        self.spender_hint_ladder(txid, vout).await
    }

    async fn spender_binding_raw(
        &self,
        spender: &str,
        txid: &str,
        vout: u32,
    ) -> Result<Option<String>, String> {
        // BUDGETED (one unit covers the ladder's up-to-two provider hits, the
        // same coarse accounting as `fetch_ancestor`); exhausted ⇒ `Err`, a
        // counted local refusal — this preserves the pinned budget-0 ⇒
        // no-courier-traffic property for every caller of this fetcher.
        let remaining = self.budget.get();
        if remaining == 0 {
            return Err("per-tick fetch budget exhausted (binding raw skipped)".into());
        }
        self.budget.set(remaining - 1);
        // CONTENT-ADDRESSED raw (fetch_raw_hex accepts bytes only if they hash
        // to `spender`), then the pure input walk: the hint becomes a fact
        // only if one of the spender's inputs consumes exactly `txid:vout`.
        let raw = self
            .fetch_raw_hex(spender)
            .await
            .map_err(|e| format!("spender raw: {e}"))?;
        let raw = raw.trim().to_string();
        Ok(tx_consumes_outpoint(&raw, txid, vout)?.then_some(raw))
    }

    async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
        // Per-tick budget guard — bound subrequests per Worker invocation.
        let remaining = self.budget.get();
        if remaining == 0 {
            return Err(GASPError::RemoteError(format!(
                "proof-fetch per-tick budget exhausted (skipping {txid}; retried next tick)"
            )));
        }
        self.budget.set(remaining - 1);

        // Content-address: the returned raw MUST hash to the requested txid, so
        // a garbled/malicious courier response can never inject a forged
        // ancestor (trait mandate).
        let raw_tx = self.fetch_raw_hex(txid).await?;
        let recomputed = Transaction::from_hex(raw_tx.trim())
            .map_err(|e| GASPError::Other(format!("parse raw {txid}: {e}")))?
            .id();
        if !recomputed.eq_ignore_ascii_case(txid) {
            return Err(GASPError::Other(format!(
                "content-address mismatch: raw hashes to {recomputed}, requested {txid}"
            )));
        }

        // Proof: courier ladder + chaintracks verify. Unmined / unverifiable at
        // every tier → `None` (retry next tick), NEVER an error.
        // The ancestor path never needs the fault distinction — a proof-read
        // fault degrades to "no proof" (the raw alone is still the answer).
        let proof = self.fetch_verified_proof(txid).await.unwrap_or(None);
        Ok(FetchedAncestor { raw_tx, proof })
    }

    /// PROOF-ONLY completion path (#192/#193 FIX 2): run the courier ladder +
    /// chaintracks verify with NO raw-tx fetch — the completion passes already
    /// hold the raw in the stored BEEF, so a raw fetch there is a redundant
    /// round-trip (and a free-tier WoC raw fetch 429s). Budget-bounded exactly
    /// like [`Self::fetch_ancestor`]. Fail-closed: budget-exhausted / unmined /
    /// unverifiable → `None`.
    async fn verified_proof_for(&self, txid: &str) -> Option<String> {
        self.verified_proof_for_detailed(txid).await.unwrap_or(None)
    }

    async fn verified_proof_for_detailed(&self, txid: &str) -> Result<Option<String>, String> {
        let remaining = self.budget.get();
        if remaining == 0 {
            // push_log, not console_log (bsv-low#304 gate LOW-3 residual):
            // native tests exercise the zero-budget path (the admin bulk
            // re-verify's chaintracks-only property). A budget refusal is a
            // DELIBERATE local bound, not a read fault → Ok(None).
            push_log(&format!(
                "[proof] per-tick budget exhausted (skipping proof for {txid}; retried next tick)"
            ));
            return Ok(None);
        }
        self.budget.set(remaining - 1);
        self.fetch_verified_proof(txid).await
    }

    /// Re-verify a STORED bump against chaintracks (the header source is the
    /// only arbiter of a merkle root). Used by proof completion to refuse
    /// trusting an admit-time structural bump that was never SPV-verified or is
    /// forged. Fail-closed via [`verify_bump`].
    async fn verify_proof(&self, txid: &str, bump_hex: &str) -> bool {
        verify_bump(self.tracker.as_deref(), bump_hex, txid).await
    }
}

// ============================================================================
// Pure helpers (unit-tested)
// ============================================================================

/// Verify a BUMP hex against the chaintracks header source: compute the merkle
/// root from `txid`'s leaf and ask the tracker whether it is the root at the
/// bump's height. FAIL-CLOSED — any of {no tracker, malformed bump, compute
/// error, tracker error, tracker `false`} → `false`. Mirrors the proven
/// `overlay-discovery::pot::lookup_service::bump_verifies` pattern.
pub(crate) async fn verify_bump(
    tracker: Option<&dyn ChainTracker>,
    bump_hex: &str,
    txid: &str,
) -> bool {
    verify_bump_detailed(tracker, bump_hex, txid)
        .await
        .unwrap_or(false)
}

/// [`verify_bump`] with the chaintracks READ FAULT kept distinguishable
/// (bsv-low#304 gate M-5): `Err` = the header-source read itself failed
/// (transport / a starved subrequest at the tick's wall) — retryable and NOT
/// a chain verdict; `Ok(false)` = a definitive local/chain "no" (no tracker
/// configured, malformed bump, root mismatch). Collapsing the two made a
/// subrequest-wall starvation of the money-relevant spend-confirmation
/// chaser silent. Same fail-closed net: every non-`Ok(true)` refuses.
pub(crate) async fn verify_bump_detailed(
    tracker: Option<&dyn ChainTracker>,
    bump_hex: &str,
    txid: &str,
) -> Result<bool, String> {
    let Some(tracker) = tracker else {
        return Ok(false); // No header source → nothing is a proven fact.
    };
    let bump = match MerklePath::from_hex(bump_hex) {
        Ok(b) => b,
        Err(_) => return Ok(false),
    };
    let root = match bump.compute_root(Some(txid)) {
        Ok(r) => r,
        Err(_) => return Ok(false),
    };
    tracker
        .is_valid_root_for_height(&root, bump.block_height)
        .await
        .map_err(|e| {
            format!(
                "chaintracks read failed for {txid}@{}: {e}",
                bump.block_height
            )
        })
}

/// Extract a ready BUMP hex from an Arcade `GET /tx/{txid}` status body: present
/// only when `txStatus` is MINED/IMMUTABLE **and** a non-empty `merklePath`
/// (a BRC-74 BUMP hex) is carried. Anything else (SEEN, no merklePath, parse
/// failure) → `None` (treated as unmined by the ladder).
///
/// #214 — **Arcade REJECTED is never authoritative uncorroborated**: its stale
/// validator view has reported REJECTED for txs already MINED (sticky ≥28 min,
/// still REJECTED at 3 confs). Load-bearing consequences here:
/// 1. a REJECTED status maps to `None` = "no proof from THIS courier", never a
///    terminal verdict — the ladder falls through to Bitails/WoC, which is the
///    ONLY way a false-REJECTED tx's proof completes, because
/// 2. Arcade's MINED callback (/arc-ingest) will never fire for a txid its
///    view holds at REJECTED. Do not add any rule that skips/abandons proof
///    completion on an Arcade REJECTED status.
fn parse_arcade_merklepath(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let status = v.get("txStatus").and_then(|s| s.as_str()).unwrap_or("");
    if status != "MINED" && status != "IMMUTABLE" {
        return None;
    }
    let mp = v.get("merklePath").and_then(|m| m.as_str())?;
    let mp = mp.trim();
    if mp.is_empty() {
        return None;
    }
    Some(mp.to_string())
}

/// Parse a TSC-proof response body (WoC / Bitails share the shape) into a
/// BRC-74 BUMP hex at `block_height`. The body may be the bare TSC object or a
/// wrapper carrying it; we accept the object directly.
fn tsc_body_to_bump_hex(body: &str, block_height: u32) -> Option<String> {
    tsc_json_to_bump_hex(body, block_height)
}

/// Convert a TSC proof JSON string to a BRC-74 BUMP hex string.
///
/// Ported/adapted from `~/bsv/rust-wallet-toolbox/src/tsc_proof.rs`
/// (`tsc_json_to_bump_hex`) against this workspace's bsv-rs `MerklePath` API.
/// Returns `None` on any malformed input.
pub fn tsc_json_to_bump_hex(json_str: &str, block_height: u32) -> Option<String> {
    let json: serde_json::Value = serde_json::from_str(json_str).ok()?;

    let index = json.get("index")?.as_u64()?;
    let txid = json.get("txOrId").and_then(|v| v.as_str())?;
    let nodes: Vec<String> = json
        .get("nodes")?
        .as_array()?
        .iter()
        .filter_map(|n| n.as_str().map(|s| s.to_string()))
        .collect();

    let mp = tsc_proof_to_merkle_path(txid, index, &nodes, block_height).ok()?;
    Some(mp.to_hex())
}

/// Build a `MerklePath` from TSC components (same algorithm as the JS reference
/// `convertProofToMerklePath()`).
fn tsc_proof_to_merkle_path(
    txid: &str,
    index: u64,
    nodes: &[String],
    block_height: u32,
) -> Result<MerklePath, String> {
    if nodes.is_empty() {
        return Err("empty nodes list".to_string());
    }
    if txid.len() != 64 || hex::decode(txid).is_err() {
        return Err("invalid txid".to_string());
    }

    let mut path: Vec<Vec<MerklePathLeaf>> = Vec::new();
    let mut current_offset = index;

    for (level, node) in nodes.iter().enumerate() {
        let mut leaves = Vec::new();

        if level == 0 {
            leaves.push(MerklePathLeaf::new_txid(current_offset, txid.to_string()));
        }

        let sibling_offset = if current_offset.is_multiple_of(2) {
            current_offset + 1
        } else {
            current_offset - 1
        };

        if node == "*" {
            leaves.push(MerklePathLeaf::new_duplicate(sibling_offset));
        } else {
            if node.len() != 64 || hex::decode(node).is_err() {
                return Err(format!("invalid node hash at level {level}"));
            }
            leaves.push(MerklePathLeaf::new(sibling_offset, node.clone()));
        }

        leaves.sort_by_key(|l| l.offset);
        path.push(leaves);
        current_offset /= 2;
    }

    MerklePath::new(block_height, path).map_err(|e| format!("{e}"))
}

/// Fetch a URL via `worker::Fetch`, returning `(status, body)`. `header` is an
/// optional single `(name, value)` pair (e.g. the WoC api key). `pub(crate)`
/// since bsv-low#309: the advert-lifecycle pass probes the same courier hosts.
pub(crate) async fn http_get(
    url: &str,
    header: Option<(&str, &str)>,
) -> Result<(u16, String), String> {
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Get);
    init.with_redirect(worker::RequestRedirect::Manual);
    if let Some((name, value)) = header {
        let headers = worker::Headers::new();
        let _ = headers.set(name, value);
        init.with_headers(headers);
    }
    let request =
        worker::Request::new_with_init(url, &init).map_err(|e| format!("req {url}: {e}"))?;
    let mut response = worker::Fetch::Request(request)
        .send()
        .await
        .map_err(|e| format!("fetch {url}: {e}"))?;
    let status = response.status_code();
    let body = response.text().await.unwrap_or_default();
    Ok((status, body))
}

// ============================================================================
// pot_beefs proof-completion tick (P3)
// ============================================================================

/// Tally of one pot-store proof-completion pass (logged by the cron).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PotProofSummary {
    /// Not-yet-verified pot BEEFs scanned this tick.
    pub scanned: usize,
    /// BEEFs upgraded with a verified BUMP, trimmed, and compacted back.
    pub completed: usize,
    /// Candidates whose STORED structural bump chaintracks-re-verified —
    /// latched via `mark_pot_beef_proven`, no fetch, no byte rewrite
    /// (bsv-low#304: the honest-backlog fast path).
    pub already_proven: usize,
    /// Candidates still unmined (fetcher returned no verified proof) — retried.
    pub still_unconfirmed: usize,
    /// Candidates the fetcher errored on (budget / transport) — retried.
    pub fetch_failed: usize,
    /// Candidates whose stitch/trim/compact failed — retried.
    pub stitch_failed: usize,
}

/// Complete missing proofs in the LOW `pot_beefs` recovery store (#192/#193).
///
/// The engine's `complete_missing_proofs` only touches its OWN `transactions`
/// table; `pot_beefs` (the `/beef` / `/recovery-view` recovery surface) is
/// LOW-specific and needs this parallel pass. Per proofless candidate:
/// PROOF-ONLY fetch → chaintracks-verify (both inside
/// [`ChainProofFetcher::verified_proof_for`], reusing the raw already in the
/// stored BEEF — no redundant raw fetch, #192/#193 FIX 2) → stitch the BUMP →
/// `trim_known_proven` → [`PotStorage::compact_pot_beef`] (which BYPASSES the
/// longer-wins guard AND re-checks the proof, fail-closed).
///
/// FAIL-CLOSED throughout: a candidate the fetcher can't verify is skipped
/// (retried next tick), never written proofless. Bounded by `limit`.
pub async fn complete_pot_beef_proofs(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    fetcher: &dyn overlay_engine::gasp::AncestorFetcher,
    limit: u64,
    min_age_secs: u64,
) -> PotProofSummary {
    let mut summary = PotProofSummary::default();

    let candidates = match pot_storage
        .find_pot_beefs_for_proof_check(limit, min_age_secs)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            push_log(&format!("[pot-proof] candidate scan failed: {e}"));
            return summary;
        }
    };
    summary.scanned = candidates.len();

    // bsv-low#304 gate M-4: fast-path latches are COLLECTED and written in
    // one batched statement per chunk after the loop — one D1 write per row
    // was the wide page's dominant op cost (the subrequest math at
    // POT_PROOF_PASS_LIMIT counts on this).
    let mut latched: Vec<String> = Vec::new();

    for (txid, stored_beef) in candidates {
        // bsv-low#304 fast path (mirrors the engine's transactions pass): a
        // candidate whose STORED bytes already carry a bump for its own txid
        // gets that bump chaintracks-RE-VERIFIED first. Genuine → latch the
        // verified flag (no fetch, no byte rewrite — the honest backlog
        // clears without courier traffic). A bump that FAILS the re-verify
        // is a fake/stale claim: fall through to the fetch path, which
        // replaces it with a chaintracks-verified one (or honestly retries).
        let stored_bump = bsv_rs::transaction::Beef::from_binary(&stored_beef)
            .ok()
            .filter(|b| {
                b.find_txid(&txid)
                    .is_some_and(bsv_rs::transaction::BeefTx::has_proof)
            })
            .and_then(|b| {
                b.find_bump(&txid)
                    .map(bsv_rs::transaction::MerklePath::to_hex)
            });
        if let Some(bump_hex) = stored_bump {
            if fetcher.verify_proof(&txid, &bump_hex).await {
                latched.push(txid);
                continue;
            }
            push_log(&format!(
                "[pot-proof] {txid} stored structural bump FAILED chaintracks re-verify — not trusting it, refetching (bsv-low#304)"
            ));
        }

        // PROOF-ONLY fetch + chaintracks-verify (#192/#193 FIX 2): the raw is
        // ALREADY in `stored_beef` (which `stitch_and_trim_pot_beef` reuses), so
        // we never re-fetch it. The fetcher returns a bump ONLY once its root is
        // verified against our PoW-anchored header source; unmined/unverifiable
        // → `None` (retry next tick), fail-closed.
        let Some(bump_hex) = fetcher.verified_proof_for(&txid).await else {
            summary.still_unconfirmed += 1;
            continue;
        };

        match stitch_and_trim_pot_beef(&txid, &stored_beef, &bump_hex) {
            Some(compacted) => {
                // compact_pot_beef re-checks the proof (fail-closed) and
                // bypasses the longer-wins guard.
                if let Err(e) = pot_storage.compact_pot_beef(&txid, &compacted).await {
                    push_log(&format!("[pot-proof] {txid} compact write failed: {e}"));
                    summary.stitch_failed += 1;
                } else {
                    summary.completed += 1;
                }
            }
            None => {
                push_log(&format!("[pot-proof] {txid} stitch/trim failed (retry)"));
                summary.stitch_failed += 1;
            }
        }
    }

    // The batched verified-latch write (chunked at the backend). A failure
    // latches NOTHING from the failed chunk-set — those rows simply remain
    // candidates and re-verify next tick (fail-safe; counted so the tick is
    // not silently short).
    if !latched.is_empty() {
        match pot_storage.mark_pot_beefs_proven(&latched).await {
            Ok(()) => summary.already_proven = latched.len(),
            Err(e) => {
                push_log(&format!(
                    "[pot-proof] batched verified-latch write failed for {} row(s) (retry next tick): {e}",
                    latched.len()
                ));
                summary.stitch_failed += latched.len();
            }
        }
    }

    summary
}

/// Run the two LOW pot-store maintenance passes in their LOAD-BEARING ORDER
/// (bsv-low#304 gate M-5): the #186 spend-confirmation chaser FIRST, the
/// pot-beef proof/bulk-drain pass SECOND.
///
/// The chaser is the independent CREDIT ANCHOR — small (≤ ~20 budgeted
/// candidates) and money-relevant — while the pot-beef pass is a bulk drain
/// (≤ [`POT_PROOF_PASS_LIMIT`] chaintracks reads). If the drain ran first
/// and the invocation hit its subrequest wall, the chaser's chaintracks
/// reads would starve — and a starved read fail-closes shaped like "not
/// mined yet" (now surfaced via `tracker_faults`, but still a credit
/// delay). BOTH entry points (the scheduled tick and
/// `/admin/complete-proofs`) call THIS function, so the order cannot drift
/// apart. Each pass keeps its own fetcher (own budget cell).
pub async fn run_pot_maintenance(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    spend_fetcher: &dyn overlay_engine::gasp::AncestorFetcher,
    spend_limit: u64,
    pot_fetcher: &dyn overlay_engine::gasp::AncestorFetcher,
    pot_limit: u64,
    min_age_secs: u64,
) -> (SpendConfirmSummary, PotProofSummary) {
    let spend =
        complete_spend_confirmations(pot_storage, spend_fetcher, spend_limit, min_age_secs).await;
    let pot = complete_pot_beef_proofs(pot_storage, pot_fetcher, pot_limit, min_age_secs).await;
    (spend, pot)
}

/// Stitch a VERIFIED `bump_hex` into a stored pot BEEF for `txid`, trim the now
/// proven ancestry, and return the compacted BEEF bytes — or `None` on any
/// parse/serialize failure (fail-closed; the caller retries). The result is
/// re-checked at the storage layer before it overwrites anything.
fn stitch_and_trim_pot_beef(txid: &str, stored_beef: &[u8], bump_hex: &str) -> Option<Vec<u8>> {
    use bsv_rs::transaction::{Beef, MerklePath, Transaction};

    // Rebuild the subject tx (with its ancestry) from the stored BEEF and set
    // its own merkle path — mirrors the engine's `update_input_proofs` for the
    // subject-is-txid case.
    let mut tx = Transaction::from_beef(stored_beef, Some(txid)).ok()?;
    tx.merkle_path = Some(MerklePath::from_hex(bump_hex).ok()?);
    let proven_beef = tx.to_beef(true).ok()?;

    // Trim: BFS from tips, drop ancestry now reachable only through a proven tx.
    let mut beef = Beef::from_binary(&proven_beef).ok()?;
    beef.trim_known_proven();
    let compacted = beef.to_binary();

    // Guard: the compacted BEEF must still prove txid's own tx — otherwise the
    // trim went wrong; return None so nothing is written.
    let proves = Beef::from_binary(&compacted)
        .ok()
        .and_then(|b| {
            b.find_txid(txid)
                .map(bsv_rs::transaction::BeefTx::has_proof)
        })
        .unwrap_or(false);
    if proves {
        Some(compacted)
    } else {
        None
    }
}

// ============================================================================
// pot_records spend-confirmation chaser (#186)
// ============================================================================

/// The WoC spent-endpoint URL for an outpoint. PINNED BY TEST: the correct
/// route is `/tx/{txid}/{vout}/spent` — the same shape the app-layer's proven
/// reader uses (`low-app-layer routes.rs spent_any_resolve`). The plausible
/// `/tx/{txid}/out/{vout}/spent` variant DOES NOT EXIST on WoC (router 404,
/// live-probed 2026-08-18): shipping it made every hint read as the semantic
/// "not spent" and the reconcile leg died silently — the reason this is a
/// pure, unit-pinned function and not an inline format!.
fn woc_spent_url(woc_base: &str, txid: &str, vout: u32) -> String {
    format!("{woc_base}/tx/{txid}/{vout}/spent")
}

/// A well-formed 64-hex spender txid, lowercased — refusing the OUTPOINT's
/// own txid (an indexer echoing the queried txid back must never become a
/// self-hint; a tx cannot spend its own output in the same tx anyway, so a
/// same-txid answer is provider noise by construction).
fn well_formed_spender(candidate: Option<&str>, outpoint_txid: &str) -> Option<String> {
    candidate
        .map(str::to_lowercase)
        .filter(|t| t.len() == 64 && t.bytes().all(|b| b.is_ascii_hexdigit()))
        .filter(|t| !t.eq_ignore_ascii_case(outpoint_txid))
}

/// BananaBlocks `…/txo/{txid}/{vout}/spend` (live-verified 2026-08-18):
/// 200 `{"spent":true,"spentTxid":"…"}` = hint; 200 `{"spent":false,…}` =
/// no spender this indexer can see; 404 `Transaction not found` = it does not
/// know the tx at all ⇒ no hint. A 2xx claiming spent WITHOUT a well-formed
/// `spentTxid` is schema drift ⇒ FAULT (never "no hint"); any other status is
/// a fault.
fn parse_bananablocks_spend_body(
    status: u16,
    body: &str,
    outpoint_txid: &str,
) -> Result<Option<String>, String> {
    if status == 404 {
        return Ok(None);
    }
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}"));
    }
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("bad json: {e}"))?;
    match v.get("spent").and_then(serde_json::Value::as_bool) {
        Some(false) => Ok(None),
        Some(true) => {
            well_formed_spender(v.get("spentTxid").and_then(|t| t.as_str()), outpoint_txid)
                .map(Some)
                .ok_or_else(|| excerpt("2xx spent:true without a well-formed spentTxid", body))
        }
        None => Err(excerpt(
            "2xx without a boolean `spent` (schema drift?)",
            body,
        )),
    }
}

/// Bitails `…/tx/{txid}/output/{vout}/spent`. The endpoint 500s at the time
/// of writing (2026-08-18) — a 5xx is a plain FAULT so the rung self-heals
/// when Bitails fixes it. Healthy-shape expectations follow the app-layer's
/// `parse_bitails_unspent` (`{"spent": bool, …}`); the spender is accepted
/// from `spentTxid` or `spentIn.txid`, and a spent:true answer carrying
/// neither is drift ⇒ FAULT (self-announcing, per the finding-7 rule).
fn parse_bitails_spend_body(
    status: u16,
    body: &str,
    outpoint_txid: &str,
) -> Result<Option<String>, String> {
    if status == 404 {
        return Ok(None);
    }
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}"));
    }
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("bad json: {e}"))?;
    match v.get("spent").and_then(serde_json::Value::as_bool) {
        Some(false) => Ok(None),
        Some(true) => {
            let candidate = v.get("spentTxid").and_then(|t| t.as_str()).or_else(|| {
                v.get("spentIn")
                    .and_then(|s| s.get("txid"))
                    .and_then(|t| t.as_str())
            });
            well_formed_spender(candidate, outpoint_txid)
                .map(Some)
                .ok_or_else(|| excerpt("2xx spent:true without a well-formed spender", body))
        }
        None => Err(excerpt(
            "2xx without a boolean `spent` (schema drift?)",
            body,
        )),
    }
}

/// WhatsOnChain `…/tx/{txid}/{vout}/spent`: 404 is the endpoint's SEMANTIC
/// "no spend this indexer can see" ⇒ no hint. A 2xx MUST carry a well-formed
/// spender `txid` — a 2xx with a missing/garbled/non-hex value is a FAULT,
/// never "no hint" (the unknown-must-not-read-as-fine rule). Any other
/// status is a fault.
fn parse_woc_spend_body(
    status: u16,
    body: &str,
    outpoint_txid: &str,
) -> Result<Option<String>, String> {
    if status == 404 {
        return Ok(None);
    }
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}"));
    }
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("bad json: {e}"))?;
    well_formed_spender(v.get("txid").and_then(|t| t.as_str()), outpoint_txid)
        .map(Some)
        .ok_or_else(|| {
            excerpt(
                "2xx without a well-formed spender txid (schema drift?)",
                body,
            )
        })
}

/// Char-boundary-safe body excerpt for fault messages: a multibyte char
/// straddling the cut must shorten the excerpt, never panic the tick.
fn excerpt(prefix: &str, body: &str) -> String {
    let mut cut = body.len().min(120);
    while !body.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{prefix}: {}", &body[..cut])
}

/// BananaBlocks spend-lookup URL. PINNED BY TEST: `/txo/{txid}/{vout}/spend`
/// (live-probed 2026-08-18; the `_`/`:`-joined outpoint forms 400).
fn bananablocks_spend_url(base: &str, txid: &str, vout: u32) -> String {
    format!("{base}/txo/{txid}/{vout}/spend")
}

/// Bitails outpoint-spend URL, the shape the app-layer already queries.
fn bitails_spent_url(base: &str, txid: &str, vout: u32) -> String {
    format!("{base}/tx/{txid}/output/{vout}/spent")
}

/// TRUE iff `raw_hex` parses to a tx with an input consuming exactly
/// `txid:vout`. Pure so the conjunction (txid match AND vout match, txid
/// case-insensitive, coinbase `source_txid: None` never matches) is
/// unit-pinned against real bytes — the caller's content-addressing already
/// guarantees WHOSE bytes these are; this decides only what they spend.
fn tx_consumes_outpoint(raw_hex: &str, txid: &str, vout: u32) -> Result<bool, String> {
    let tx = Transaction::from_hex(raw_hex).map_err(|e| format!("spender parse: {e}"))?;
    let want = txid.to_lowercase();
    Ok(tx.inputs.iter().any(|i| {
        i.source_output_index == vout
            && i.source_txid
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case(&want))
    }))
}

/// Assemble the ATOMIC BEEF for a displaced-in spender from the bytes the
/// reconcile just proved (content-addressed raw + chaintracks-verified bump),
/// so the store holds the same durable copy `output_spent` would have written
/// had the true spender been submitted — without it the displaced spend is
/// visible but its WIN stays unattributable (`/results`/leaderboard need
/// `spender_beef_hex`). Pure; any failure is the caller's cue to log and
/// proceed (the POINTER write is the money fix — this is enrichment).
fn assemble_spender_beef(raw_hex: &str, bump_hex: &str, txid: &str) -> Result<Vec<u8>, String> {
    let bump = MerklePath::from_hex(bump_hex).map_err(|e| format!("bump parse: {e}"))?;
    let raw = hex::decode(raw_hex).map_err(|e| format!("raw decode: {e}"))?;
    let mut beef = Beef::new();
    let bump_index = beef.merge_bump(bump);
    beef.merge_raw_tx(raw, Some(bump_index));
    beef.to_binary_atomic(txid)
        .map_err(|e| format!("beef serialize: {e}"))
}

/// Tally of one pot-spend confirmation pass (logged by the cron / returned by
/// the admin route).
// NOTE: not `Copy` — `sample` is a Vec (observability only).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SpendConfirmSummary {
    /// Spent-but-unconfirmed pot rows scanned this tick.
    pub scanned: usize,
    /// Rows UPGRADED to `spentConfirmed = 1` this tick (the spending tx's bump
    /// verified against chaintracks).
    pub confirmed: usize,
    /// Rows whose spending tx is not yet verifiably mined — left unconfirmed,
    /// retried next tick (fail-closed).
    pub still_unconfirmed: usize,
    /// Rows skipped because the per-tick fetch budget was exhausted — retried
    /// next tick. NOTE: [`AncestorFetcher::verified_proof_for`] folds
    /// budget-exhausted and unmined into a single `None`, so this counter is
    /// structurally 0 here (matching [`PotProofSummary::fetch_failed`], which
    /// is likewise not separately observable); such candidates are counted
    /// under `still_unconfirmed`. Kept for shape parity + future use.
    pub fetch_failed: usize,
    /// bsv-low#304 gate M-5: proof/header READ FAULTS
    /// (`verified_proof_for_detailed` → `Err`, e.g. a chaintracks call
    /// starved at the invocation's subrequest wall) — retried next tick,
    /// counted SEPARATELY from `still_unconfirmed` so a starved tick is
    /// visible instead of masquerading as "not mined yet".
    pub tracker_faults: usize,
    /// bsv-low#301: rows whose confirmed CAS write MISSED — the spend
    /// pointer moved between the candidate read and the write (a racing
    /// displacement, reorg class included), so the guarded write was a
    /// NO-OP. Never confirmed on the stale read; the row is either still
    /// unconfirmed (→ re-surfaced by the next tick's candidate scan) or
    /// was confirmed by the competing writer (terminal, correct).
    pub cas_missed: usize,
    /// bsv-low#301 gate M2: CAS writes that ERRORED (storage/driver fault —
    /// distinct from a guard miss). Load-bearing observability:
    /// `confirm_spend_cas_sql` is the codebase's first `RETURNING` through
    /// the worker-rs D1 driver, so a driver that rejected the statement
    /// would fail EVERY row — without this counter that failure mode would
    /// be silent (confirmed=0, cas_missed=0, only per-row logs). A total-
    /// RETURNING failure now self-announces as `scanned > 0 && confirmed
    /// == 0 && cas_errors > 0`. Rows are retried next tick (still
    /// unconfirmed candidates), fail-safe.
    pub cas_errors: usize,
    /// Rows DISPLACED this tick (the 2026-08-18 index reconcile): the recorded
    /// spend pointer was a CLAIM that never verifiably mined (e.g. a parked
    /// non-final refund a client re-submitted), while the chain's ACTUAL
    /// spender — input-bound to the outpoint and chaintracks-verified mined —
    /// was a different tx. The pointer now names the true spender with
    /// `spentConfirmed = 1` (the existing last-confirmed-wins arm).
    pub displaced: usize,
    /// Reconcile pipelines ENTERED this tick — an `Ok(None)` row the chaser
    /// asked the chain about, whatever the outcome. Bounded by
    /// [`DISPLACE_CAP_PER_TICK`].
    pub displace_attempts: usize,
    /// Reconcile pipeline REFUSALS + READ FAULTS: hint/raw/proof transport
    /// faults, plus hints that FAILED the content-addressed input binding (a
    /// lying or garbled indexer answer — refused, logged, never written).
    pub displace_faults: usize,
    /// OBSERVABILITY ONLY (bounded to 5): the spending txids actually sampled
    /// this tick. Lets an operator check the candidates against a block explorer
    /// to tell "the chaser is broken" from "this backlog is genuinely
    /// unconfirmable" (e.g. a 0-conf spend that was later superseded and never
    /// mined, so no proof will ever exist). Never used for control flow.
    pub sample: Vec<String>,
}

/// Per-tick cap on reconcile pipelines entered from the chaser's `Ok(None)`
/// arm. Honest worst-case subrequest accounting PER ENTRY: 1 hint + up to 2
/// raw-provider hits + the proof leg's own budgeted ladder — every HTTP leg
/// draws from the fetcher's shared per-tick budget (hint and raw each charge
/// one unit; the proof leg charges as it always did), so a large
/// unconfirmable backlog cannot turn the reconcile into a courier hammer and
/// the pinned budget-0 ⇒ chaintracks-only property still holds. Candidates
/// are RANDOM-sampled upstream, so a capped tick still converges across
/// ticks.
const DISPLACE_CAP_PER_TICK: usize = 4;

/// Confirm 0-conf pot spends in the LOW `pot_records` landing-proof store
/// (#186).
///
/// LOW settles submit 0-conf (no merkle bump at submit time), so `mark_spent`
/// records `spent = 1, spentConfirmed = 0` and nothing ever upgrades it — the
/// overlay's SPV-confirmed wallet-credit tier goes unrealized. This pass, run
/// in the SAME completion tick as the BEEF proof passes, chases each such row:
/// fetch + chaintracks-verify the SPENDING tx's bump
/// ([`AncestorFetcher::verified_proof_for`] — the raw-free, budget-bounded
/// path), and on a verified `Some` latch `spentConfirmed = 1` via
/// [`PotStorage::mark_spent`] with `confirmed = true` (an UPGRADE that never
/// downgrades a confirmed row).
///
/// FAIL-CLOSED: a spend the fetcher can't verify against chaintracks is left
/// unconfirmed (retried next tick), NEVER latched on a courier's word. Bounded
/// by `limit`.
///
/// # #301: the confirmed write is a GUARDED CAS (the #284 MEDIUM-2 sibling)
///
/// The candidate read and the confirmed write are separated by awaits (the
/// proof fetch), so the write goes through
/// [`PotStorage::mark_confirmed_for_spender`] — conditional on the row's
/// spend pointer STILL being the spender the proof was verified FOR
/// (`WHERE … AND spendingTxid = ?`, the `verdict_cas_sql` idiom). Pre-#301
/// the unguarded `mark_spent(confirmed = true)` re-wrote the pointer from
/// the STALE read: a reorg-confirmed S2 landing in the window was RESET
/// back to S1, and nothing ever re-chased it (this pass's candidate query
/// only surfaces `spentConfirmed = 0` rows).
///
/// A CAS MISS (counted `cas_missed`) leaves the row untouched, and NO
/// explicit re-chase hook is needed — the normal candidate selection
/// covers both miss shapes:
/// - pointer moved to an UNCONFIRMED S2 (an `output_spent` last-writer
///   displacement): the row still matches `spent = 1 AND spentConfirmed =
///   0`, so [`PotStorage::find_spent_unconfirmed`] re-surfaces it and the
///   next tick chases the CURRENT pointer (the displacement restamped
///   `spentAt`, so the #228 push-first age gate re-applies to S2 —
///   correct: it is S2's push window now);
/// - pointer moved to a CONFIRMED S2 (the reorg class): the row left the
///   candidate set BECAUSE a competing chaintracks-verified confirm
///   landed — a terminal, correct state with nothing to re-chase.
///
/// Accepted residual (deliberate): the pre-#301 behaviour re-asserted a
/// fresh-SPV-verified S1 over a racing unconfirmed displacement in the
/// same tick; under the CAS that defers to the NEXT tick, which chases
/// the CURRENT pointer instead. If the displacing claim never proves, the
/// row is simply re-chased forever (bounded, RANDOM-sampled) — the
/// fail-safe direction, and the pointer is last-writer-wins among
/// unconfirmed claims by design (`mark_spent` trait doc). The reorg-reset
/// harm this closes (a silently reverted confirmed pointer that nothing
/// re-visits) outweighs the one-tick confirm delay.
pub async fn complete_spend_confirmations(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    fetcher: &dyn AncestorFetcher,
    limit: u64,
    min_age_secs: u64,
) -> SpendConfirmSummary {
    let mut summary = SpendConfirmSummary::default();

    let candidates = match pot_storage
        .find_spent_unconfirmed(limit, min_age_secs)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            push_log(&format!("[spend-confirm] candidate scan failed: {e}"));
            return summary;
        }
    };
    summary.scanned = candidates.len();

    // ── PHASE 1: the primary pointer chase (unchanged semantics). Rows whose
    // recorded pointer is not verifiably mined are ALSO collected (up to the
    // cap) for phase 2's reconcile — which runs strictly AFTER this loop so
    // the reconcile can only spend budget the primary credit-anchor chase
    // left over (the M-5 starvation ordering).
    let mut displace_candidates: Vec<overlay_discovery::pot::storage::PotRecord> = Vec::new();
    for rec in candidates {
        // A spent row always carries a spending txid; skip defensively if not.
        let Some(spending_txid) = rec.spending_txid.as_deref() else {
            continue;
        };
        if summary.sample.len() < 5 {
            summary.sample.push(spending_txid.to_string());
        }

        // PROOF-ONLY fetch + chaintracks-verify: the fetcher returns a bump
        // ONLY once its root is verified against our PoW-anchored header source;
        // unmined / unverifiable / budget-exhausted → `Ok(None)` (retry),
        // never a positive. A READ FAULT (`Err` — chaintracks/transport,
        // incl. a starved subrequest at the tick's wall) is surfaced +
        // counted separately (bsv-low#304 gate M-5): this pass is the
        // independent CREDIT ANCHOR, and a silent starvation here would be
        // indistinguishable from "chain says not yet".
        match fetcher.verified_proof_for_detailed(spending_txid).await {
            Err(e) => {
                summary.tracker_faults += 1;
                push_log(&format!(
                    "[spend-confirm] {spending_txid} proof/header READ FAULT (subrequest wall?) — retrying, NOT a chain verdict: {e}"
                ));
            }
            Ok(Some(bump_hex)) => {
                // UPGRADE: latch spentConfirmed = 1 — via the #301 GUARDED
                // CAS (`mark_confirmed_for_spender`), conditional on the
                // pointer still being the spender THIS proof was verified
                // for; a moved pointer ⇒ no-op, counted, left for the next
                // tick's candidate scan (see the fn doc's case analysis).
                //
                // #284: this caller only CONFIRMS an existing pointer and
                // has no spender raw in hand → the CAS never touches the
                // stored verdict/verdictTxid. The spentHeight DOES ride
                // along: the block height is a fact of the just-verified
                // BUMP (None keeps the stored value — same-pointer
                // COALESCE semantics).
                let spent_height = MerklePath::from_hex(&bump_hex)
                    .ok()
                    .map(|mp| u64::from(mp.block_height));
                match pot_storage
                    .mark_confirmed_for_spender(
                        &rec.txid,
                        rec.output_index,
                        spending_txid,
                        spent_height,
                    )
                    .await
                {
                    Ok(true) => summary.confirmed += 1,
                    Ok(false) => {
                        summary.cas_missed += 1;
                        push_log(&format!(
                            "[spend-confirm] {}:{} pointer moved off {spending_txid} between \
                             read and write (reorg/displacement race, bsv-low#301) — confirmed \
                             NOTHING; the next pass re-chases the current pointer",
                            rec.txid, rec.output_index
                        ));
                    }
                    Err(e) => {
                        // Gate M2: counted, not just logged — a driver that
                        // rejects the RETURNING statement fails EVERY row,
                        // and that must self-announce in the summary.
                        summary.cas_errors += 1;
                        push_log(&format!(
                            "[spend-confirm] {} confirm CAS failed: {e}",
                            rec.txid
                        ));
                    }
                }
            }
            Ok(None) => {
                summary.still_unconfirmed += 1;
                if displace_candidates.len() < DISPLACE_CAP_PER_TICK {
                    displace_candidates.push(rec);
                }
            }
        }
    }

    // ── PHASE 2: THE RECONCILE (2026-08-18 index regression). Each collected
    // row's recorded pointer is a CLAIM that is not verifiably mined (any
    // submitter's — including a parked NON-FINAL refund a client re-submitted
    // under Rule 4b), and chasing only the claim forever is exactly how a
    // MINED true spend stayed invisible behind it (pot e450f668…:0 — recorded
    // 41f70310… refund intent, actual mined settle 3ddda993…). Ask the chain
    // who ACTUALLY spent the outpoint; displace ONLY on the full ladder:
    //   1. `resolve_spender` — an indexer HINT, never a verdict;
    //   2. `spender_binding_raw` — the hinted tx's content-addressed raw
    //      bytes must consume exactly this outpoint (a lying indexer is
    //      refused HERE), and the proven bytes come back for fact
    //      derivation;
    //   3. `verified_proof_for_detailed` — the hinted spender must be MINED
    //      under a PoW-verified root. An unmined hint NEVER displaces: that
    //      would let one mempool claim overwrite another; only chain truth
    //      displaces.
    // The write is the GUARDED `displace_spend_for` CAS — conditional on the
    // pointer still being the claim this pipeline started from, so a
    // competing confirmed write inside the verify window makes this a no-op
    // re-evaluated next tick (the #301 discipline; never trade self-healing
    // for permanent). Verdict columns are NOT touched (the stale verdict
    // stays keyed to the OLD txid and every reader guards
    // `verdictTxid == spendingTxid`); `spenderFinal` is parsed from the
    // PROVEN bytes with the same rule as `output_spent` (the #371 witness
    // keeps one meaning); the proven raw + bump are persisted as the
    // spender's atomic BEEF so the displaced-in win stays classifiable
    // (enrichment — its failure never blocks the pointer fix).
    for rec in displace_candidates {
        let Some(recorded) = rec.spending_txid.as_deref() else {
            continue;
        };
        summary.displace_attempts += 1;
        let hinted = match fetcher.resolve_spender(&rec.txid, rec.output_index).await {
            Err(e) => {
                summary.displace_faults += 1;
                push_log(&format!(
                    "[spend-confirm] {}:{} spender-hint UNAVAILABLE (fault or budget) — \
                     reconcile skipped this tick, NOT a chain verdict: {e}",
                    rec.txid, rec.output_index
                ));
                continue;
            }
            Ok(h) => h,
        };
        let Some(actual) = hinted else {
            // No hint — nothing to reconcile against; the ordinary pointer
            // chase keeps retrying next tick.
            continue;
        };
        if actual.eq_ignore_ascii_case(recorded) {
            // The chain names the recorded pointer itself — the spend is
            // genuinely just not mined yet. Ordinary chase.
            continue;
        }
        let raw = match fetcher
            .spender_binding_raw(&actual, &rec.txid, rec.output_index)
            .await
        {
            Err(e) => {
                summary.displace_faults += 1;
                push_log(&format!(
                    "[spend-confirm] {}:{} binding read UNAVAILABLE for hint {actual} \
                     (fault or budget) — reconcile skipped this tick: {e}",
                    rec.txid, rec.output_index
                ));
                continue;
            }
            Ok(None) => {
                summary.displace_faults += 1;
                push_log(&format!(
                    "[spend-confirm] {}:{} hint {actual} does NOT consume the outpoint — \
                     indexer hint REFUSED (content-addressed input binding failed); \
                     nothing written",
                    rec.txid, rec.output_index
                ));
                continue;
            }
            Ok(Some(raw)) => raw,
        };
        let bump_hex = match fetcher.verified_proof_for_detailed(&actual).await {
            Err(e) => {
                summary.displace_faults += 1;
                push_log(&format!(
                    "[spend-confirm] {}:{} proof/header READ FAULT for true-spender \
                     candidate {actual} — reconcile skipped this tick: {e}",
                    rec.txid, rec.output_index
                ));
                continue;
            }
            Ok(None) => {
                push_log(&format!(
                    "[spend-confirm] {}:{} candidate {actual} binds the outpoint but has \
                     no verifiable proof THIS TICK (unmined, or the tick's proof budget \
                     ran dry — see any preceding budget log) — no displacement (only \
                     chain truth displaces a claim)",
                    rec.txid, rec.output_index
                ));
                continue;
            }
            Ok(Some(b)) => b,
        };
        // Facts derived from the PROVEN bytes:
        // — height from the verified bump;
        // — bytes-finality with the EXACT `output_spent` rule (#371: the
        //   column means one thing regardless of writer; parse failure ⇒
        //   None = not parsed, never a guess).
        let spent_height = MerklePath::from_hex(&bump_hex)
            .ok()
            .map(|mp| u64::from(mp.block_height));
        let spender_final = Transaction::from_hex(&raw)
            .ok()
            .map(|tx| !(tx.lock_time > 0 && tx.inputs.iter().any(|i| i.sequence < 0xffff_ffff)));
        // Durably persist the spender's atomic BEEF BEFORE the pointer flips,
        // so the classifier finds bytes the moment the row names this
        // spender. Failure logs and proceeds — the pointer is the money fix.
        match assemble_spender_beef(&raw, &bump_hex, &actual) {
            Ok(beef) => {
                if let Err(e) = pot_storage.store_beef(&actual, &beef).await {
                    push_log(&format!(
                        "[spend-confirm] {actual} displaced-spender beef store failed \
                         (win may stay unattributed until a re-submit): {e}"
                    ));
                }
            }
            Err(e) => {
                push_log(&format!(
                    "[spend-confirm] {actual} displaced-spender beef assembly failed \
                     (win may stay unattributed until a re-submit): {e}"
                ));
            }
        }
        match pot_storage
            .displace_spend_for(
                &rec.txid,
                rec.output_index,
                recorded,
                &actual,
                spent_height,
                spender_final,
            )
            .await
        {
            Ok(true) => {
                summary.displaced += 1;
                // The row ENDED this tick confirmed — it is not still
                // unconfirmed, and ops reads must not double-count it.
                summary.still_unconfirmed = summary.still_unconfirmed.saturating_sub(1);
                push_log(&format!(
                    "[spend-confirm] {}:{} DISPLACED {recorded} → {actual} (input-bound + \
                     chaintracks-verified mined; the recorded claim was never verifiably \
                     mined)",
                    rec.txid, rec.output_index
                ));
            }
            Ok(false) => {
                summary.cas_missed += 1;
                push_log(&format!(
                    "[spend-confirm] {}:{} displacement CAS missed — the pointer moved off \
                     {recorded} (or was competing-confirmed) inside the verify window; \
                     wrote NOTHING, next tick re-evaluates",
                    rec.txid, rec.output_index
                ));
            }
            Err(e) => {
                summary.cas_errors += 1;
                push_log(&format!(
                    "[spend-confirm] {}:{} displacement CAS failed: {e}",
                    rec.txid, rec.output_index
                ));
            }
        }
    }

    summary
}

// ============================================================================
// pot_records decoded-params lazy backfill (bsv-low #284)
// ============================================================================

/// Per-tick candidate bound for [`backfill_decoded_params`] (~16 rows; each
/// costs one `pot_beefs` read + a parse, plus a second pair for a spent
/// covenant row's verdict).
pub const PARAMS_BACKFILL_LIMIT: u64 = 16;

/// Tally of one decoded-params backfill pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ParamsBackfillSummary {
    /// Undecoded rows scanned this tick.
    pub scanned: usize,
    /// Rows whose decode was recorded (`paramsDecoded` latched) this tick —
    /// covenant, bare, p2pkh, or unrecognized.
    pub decoded: usize,
    /// Verdicts computed + stored for already-spent covenant rows.
    pub verdicts: usize,
    /// Rows skipped because their funding BEEF is missing/unparseable —
    /// they STAY candidates (retried forever, bounded per tick).
    pub missing_beef: usize,
}

/// Lazily decode the #284 columns for pre-migration `pot_records` rows
/// (`paramsDecoded = 0`), from the DURABLE `pot_beefs` store — modeled on
/// [`complete_spend_confirmations`] (bounded, RANDOM-sampled candidates,
/// fail-safe skips) but needing NO courier: everything decoded is already in
/// our own admitted bytes.
///
/// Per candidate row:
/// 1. read the funding BEEF (`get_beef`); missing → leave `paramsDecoded = 0`
///    (a permanent candidate, retried next tick — RANDOM order stops it
///    starving the tail);
/// 2. parse the tx out of the BEEF **hash-bound** (`Transaction::from_beef`
///    selects by the row's own txid over parse-computed ids — a garbled
///    stored row cannot masquerade as the funding tx) and take the lock at
///    the row's `outputIndex`;
/// 3. classify the lock shape: covenant (params extracted; an extraction
///    failure still records `lockKind='covenant'` with NULL params), bare
///    2-of-3, plain P2PKH, or unrecognized (`lockKind` NULL) — in EVERY
///    case `paramsDecoded` latches 1 (decode attempted + recorded), written
///    through [`PotStorage::store_record`]'s decoded-column upsert (which
///    never touches spend state);
/// 4. if the row is SPENT, its spender's BEEF is stored, and the lock
///    decoded to a covenant: compute the verdict for the CURRENT
///    `spendingTxid` (input match + stake conservation + template match)
///    and store it via `mark_spent` under the row's own confirmed flag —
///    which respects the confirmed-guard semantics (a racing confirmed
///    writer can never be displaced by this pass).
///
/// Bare rows NEVER get a verdict (their refund rule needs an unverified
/// marker hint — app-layer-only, money-neutral).
pub async fn backfill_decoded_params(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    limit: u64,
) -> ParamsBackfillSummary {
    use overlay_discovery::pot::storage::PotRecord;
    use overlay_discovery::pot::storage::VerdictWrite;
    use overlay_discovery::pot::{
        classify_covenant, extract_covenant_params, is_bare_2of3_lock, is_p2pkh_script,
        settle_signers_for_spend, RawTx, SettleSigners,
    };

    let mut summary = ParamsBackfillSummary::default();
    let candidates = match pot_storage.find_params_undecoded(limit).await {
        Ok(c) => c,
        Err(e) => {
            push_log(&format!("[params-backfill] candidate scan failed: {e}"));
            return summary;
        }
    };
    summary.scanned = candidates.len();

    for row in candidates {
        // 1. The funding BEEF — the only byte source this pass trusts.
        let beef = match pot_storage.get_beef(&row.txid).await {
            Ok(Some(b)) => b,
            Ok(None) => {
                summary.missing_beef += 1;
                continue; // stays a candidate — retried next tick
            }
            Err(e) => {
                push_log(&format!(
                    "[params-backfill] {} beef read failed: {e}",
                    row.txid
                ));
                summary.missing_beef += 1;
                continue;
            }
        };
        // 2. Hash-bound parse: select the subject by the row's own txid
        //    (BEEF txids are computed from the raw bytes at parse time).
        let Ok(tx) = Transaction::from_beef(&beef, Some(&row.txid)) else {
            push_log(&format!(
                "[params-backfill] {} stored beef unparseable — left a candidate",
                row.txid
            ));
            summary.missing_beef += 1;
            continue;
        };
        let output = tx.outputs.get(row.output_index as usize);

        // 3. Decode the lock shape into an upsert record.
        let mut decoded = PotRecord {
            txid: row.txid.clone(),
            output_index: row.output_index,
            pot_sats: output.and_then(|o| o.satoshis),
            params_decoded: true,
            ..Default::default()
        };
        let mut covenant_params = None;
        if let Some(o) = output {
            let lock = o.locking_script.to_binary();
            if let Some(p) = extract_covenant_params(&lock) {
                decoded.lock_kind = Some("covenant".into());
                decoded.pub_a = Some(hex::encode(p.pub_a));
                decoded.pub_b = Some(hex::encode(p.pub_b));
                decoded.pub_tower = Some(hex::encode(p.pub_tower));
                decoded.pay_pkh_a = Some(hex::encode(p.pay_pkh_a));
                decoded.pay_pkh_b = Some(hex::encode(p.pay_pkh_b));
                decoded.rake_pkh = Some(hex::encode(p.rake_pkh));
                decoded.stake_a = Some(p.stake_a);
                decoded.stake_b = Some(p.stake_b);
                decoded.fee_sats = Some(p.fee_sats);
                decoded.recovery_height = Some(p.recovery_height);
                covenant_params = Some(p);
            } else if overlay_discovery::pot::is_pot_covenant_script(&lock) {
                // Recognizer-matched but unextractable params ("impossible"
                // — mirrors the admission path): covenant kind, NULL params.
                decoded.lock_kind = Some("covenant".into());
            } else if is_bare_2of3_lock(&lock) {
                decoded.lock_kind = Some("bare".into());
            } else if is_p2pkh_script(&lock) {
                decoded.lock_kind = Some("p2pkh".into());
            }
            // else: unrecognized shape — lockKind stays NULL, decode
            // attempted (paramsDecoded = 1).
        }
        if let Err(e) = pot_storage.store_record(&decoded).await {
            push_log(&format!(
                "[params-backfill] {} decoded upsert failed: {e}",
                row.txid
            ));
            continue;
        }
        summary.decoded += 1;

        // 4. Verdict backfill for an already-spent covenant row whose
        //    spender BEEF we hold.
        let (Some(params), Some(pot_sats), true, Some(spending_txid)) = (
            covenant_params,
            decoded.pot_sats,
            row.spent,
            row.spending_txid.as_deref(),
        ) else {
            continue;
        };
        if params.stake_a.checked_add(params.stake_b) != Some(pot_sats) {
            continue; // conservation failed — never classify
        }
        let Ok(Some(spender_beef)) = pot_storage.get_beef(spending_txid).await else {
            continue; // no spender bytes — the read-path fallback covers it
        };
        let Ok(spending_tx) = Transaction::from_beef(&spender_beef, Some(spending_txid)) else {
            continue;
        };
        let Some((pot_input_index, pot_input_sequence)) =
            spending_tx.inputs.iter().enumerate().find_map(|(n, i)| {
                (i.source_txid
                    .as_deref()
                    .is_some_and(|t| t.eq_ignore_ascii_case(&row.txid))
                    && i.source_output_index == row.output_index)
                    .then_some((n, i.sequence))
            })
        else {
            continue; // the recorded spender does not spend this outpoint
        };
        let Some(spender) = RawTx::from_transaction(&spending_tx) else {
            continue;
        };
        let Some(verdict) = classify_covenant(&params, &spender, pot_input_sequence) else {
            continue; // non-template spend — honestly unresolved
        };
        // bsv-low #406: WHO SIGNED, from the durable spender bytes. This pass
        // holds the bytes by construction, so a no-pair answer latches
        // 'unresolved' (re-derived and concluded — never re-scanned), while a
        // verifying pair latches the wire value. Both ride the verdict CAS.
        let signers =
            settle_signers_for_spend(&params, pot_sats, &spending_tx.to_binary(), pot_input_index)
                .map(SettleSigners::as_str)
                .unwrap_or("unresolved");
        // GUARDED CAS write (gate MEDIUM-2, 2026-07-28): the candidate read
        // and this write are separated by several awaits, so the pointer may
        // have MOVED (e.g. a reorg-confirmed S2 landed). A plain mark_spent
        // echoing the stale read (`row.spent_confirmed` + the read-time
        // spender) would — on the confirmed always-write branch — reset the
        // row back to the stale S1, and nothing would re-chase it (the #186
        // chaser only surfaces spentConfirmed = 0 rows).
        // `mark_verdict_for_spender` writes verdict + verdictTxid ONLY while
        // the row's CURRENT pointer still equals the spender this verdict
        // was computed for, and touches nothing else (not the pointer, not
        // spentConfirmed, not the #228 spentAt anchor, not spentHeight) —
        // a moved pointer makes it a no-op and the read-path fallback (or a
        // later tick, if this row re-enters via its spender) covers it.
        if let Err(e) = pot_storage
            .mark_verdict_for_spender(
                &row.txid,
                row.output_index,
                spending_txid,
                VerdictWrite {
                    verdict: verdict.as_str(),
                    settle_signers: Some(signers),
                },
            )
            .await
        {
            push_log(&format!(
                "[params-backfill] {} verdict write failed: {e}",
                row.txid
            ));
        } else {
            summary.verdicts += 1;
        }
    }

    summary
}

// ============================================================================
// settleSigners historic backfill (bsv-low #406)
// ============================================================================

/// Per-tick candidate bound for [`backfill_settle_signers`] — same figure as
/// the #284 pass (each candidate costs one spender-BEEF read + at most a few
/// ECDSA verifies, all local).
pub const SETTLE_SIGNERS_BACKFILL_LIMIT: u64 = 16;

/// Tally of one settle-signers backfill pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SettleSignersBackfillSummary {
    /// Candidate rows scanned this tick.
    pub scanned: usize,
    /// Rows latched with a REAL signer value ('coop'/'tower-a'/'tower-b').
    pub latched: usize,
    /// Rows latched 'unresolved' — re-derived from the durable bytes and no
    /// pair verified (or a data anomaly made the row unclassifiable forever).
    /// Latching removes them from the candidate set; readers show "not
    /// established".
    pub unresolved: usize,
    /// Rows skipped because the spender BEEF is missing/unparseable — they
    /// STAY candidates (the bytes can still arrive via a later submit).
    pub missing_beef: usize,
}

/// Backfill `pot_records.settleSigners` for rows classified BEFORE #406
/// shipped (verdict present + current, signers NULL) from the durable
/// spender BEEF — the historic sibling of the live classify in
/// `pot::lookup_service::output_spent`.
///
/// Per candidate row:
/// 1. rebuild the committed params + pot value from the row's own decoded
///    columns (a verdict-holding covenant row has them by construction; a
///    row that doesn\'t is a data anomaly and latches 'unresolved' rather
///    than re-entering forever);
/// 2. read the SPENDER\'s durable BEEF (`get_beef(spendingTxid)`); missing →
///    stays a candidate (bounded per tick, RANDOM-sampled);
/// 3. hash-bound parse, locate the pot input, and classify WHO SIGNED
///    ([`overlay_discovery::pot::settle_signers_for_spend`] — signatures
///    verified against the committed triple over the network\'s own BIP-143
///    digest). No pair verifying latches 'unresolved';
/// 4. re-derive the verdict from the same bytes and CAS the whole group via
///    [`PotStorage::mark_verdict_for_spender`] (pointer-guarded — a moved
///    pointer makes it a no-op). The re-derived verdict equals the stored
///    one for any row the same classifier wrote (deterministic function of
///    bytes); if the re-derivation comes up empty the STORED verdict string
///    is echoed unchanged so the write stays a pure signers-attach.
/// (2026-08-26, OWNER RULING: NO WoC anywhere.) A briefly-shipped WoC
/// chain-fetch fallback here was reverted the same evening. A spend whose
/// bytes never reached our store stays a `missing_beef` candidate; the
/// DURABLE fix is FIRST-PARTY delivery — the tower already submits its pot
/// spends with ancestry (`broadcast_via_overlay` #193, `submit_tm_pot` #36
/// with per-tick retries), so this set converges without any indexer read.

pub async fn backfill_settle_signers(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    limit: u64,
) -> SettleSignersBackfillSummary {
    use overlay_discovery::pot::storage::VerdictWrite;
    use overlay_discovery::pot::{
        classify_covenant, settle_signers_for_spend, RawTx, SettleSigners,
    };

    let mut summary = SettleSignersBackfillSummary::default();
    let candidates = match pot_storage.find_settle_signers_unlatched(limit).await {
        Ok(c) => c,
        Err(e) => {
            push_log(&format!("[signers-backfill] candidate scan failed: {e}"));
            return summary;
        }
    };
    summary.scanned = candidates.len();

    for row in candidates {
        // The candidate query guarantees a current verdict group.
        let (Some(stored_verdict), Some(spending_txid)) =
            (row.verdict.as_deref(), row.spending_txid.as_deref())
        else {
            continue; // cannot happen per the query; leave it alone
        };
        // A latch that concludes "unclassifiable forever" — used for every
        // durable-bytes conclusion AND for data anomalies (a candidate that
        // cannot ever classify must not re-enter every tick).
        fn latch_unresolved(verdict: &str) -> VerdictWrite<'_> {
            VerdictWrite {
                verdict,
                settle_signers: Some("unresolved"),
            }
        }

        // 1. Params + value from the row\'s own decoded columns.
        let (Some(params), Some(pot_sats)) = (row.decoded_covenant_params(), row.pot_sats) else {
            if let Err(e) = pot_storage
                .mark_verdict_for_spender(
                    &row.txid,
                    row.output_index,
                    spending_txid,
                    latch_unresolved(stored_verdict),
                )
                .await
            {
                push_log(&format!(
                    "[signers-backfill] {} anomaly latch failed: {e}",
                    row.txid
                ));
            } else {
                summary.unresolved += 1;
            }
            continue;
        };
        if params.stake_a.checked_add(params.stake_b) != Some(pot_sats) {
            // Conservation broken on a verdict-holding row: an anomaly (the
            // classifier refuses this before writing a verdict) — latch out.
            if pot_storage
                .mark_verdict_for_spender(
                    &row.txid,
                    row.output_index,
                    spending_txid,
                    latch_unresolved(stored_verdict),
                )
                .await
                .is_ok()
            {
                summary.unresolved += 1;
            }
            continue;
        }

        // 2. The spender's durable bytes (first-party only — see the note
        // above `backfill_settle_signers`).
        let Ok(Some(spender_beef)) = pot_storage.get_beef(spending_txid).await else {
            summary.missing_beef += 1;
            continue; // stays a candidate — the bytes can still arrive
        };
        let Ok(spending_tx) = Transaction::from_beef(&spender_beef, Some(spending_txid)) else {
            summary.missing_beef += 1;
            continue; // unparseable stored bytes — longer-wins may repair
        };
        let Some((pot_input_index, pot_input_sequence)) =
            spending_tx.inputs.iter().enumerate().find_map(|(n, i)| {
                (i.source_txid
                    .as_deref()
                    .is_some_and(|t| t.eq_ignore_ascii_case(&row.txid))
                    && i.source_output_index == row.output_index)
                    .then_some((n, i.sequence))
            })
        else {
            // The recorded spender does not spend this outpoint: an anomaly
            // for a verdict-holding row — latch out.
            if pot_storage
                .mark_verdict_for_spender(
                    &row.txid,
                    row.output_index,
                    spending_txid,
                    latch_unresolved(stored_verdict),
                )
                .await
                .is_ok()
            {
                summary.unresolved += 1;
            }
            continue;
        };

        // 3+4. Classify signers; re-derive the verdict; CAS the group.
        let signers =
            settle_signers_for_spend(&params, pot_sats, &spending_tx.to_binary(), pot_input_index);
        let rederived = RawTx::from_transaction(&spending_tx)
            .and_then(|spender| classify_covenant(&params, &spender, pot_input_sequence));
        let verdict_str = rederived.map(|v| v.as_str()).unwrap_or(stored_verdict);
        let group = VerdictWrite {
            verdict: verdict_str,
            settle_signers: Some(signers.map(SettleSigners::as_str).unwrap_or("unresolved")),
        };
        match pot_storage
            .mark_verdict_for_spender(&row.txid, row.output_index, spending_txid, group)
            .await
        {
            Err(e) => push_log(&format!(
                "[signers-backfill] {} write failed: {e}",
                row.txid
            )),
            Ok(()) if signers.is_some() => summary.latched += 1,
            Ok(()) => summary.unresolved += 1,
        }
    }

    summary
}

// ============================================================================
// admitted-but-network-absent rebroadcast backstop (bsv-low #273, #267 item c)
// ============================================================================

/// Per-tick candidate bound for [`rebroadcast_absent_admitted`] — the same
/// figure as the #284 backfill (each candidate costs ≤2 presence GETs and,
/// only when provably absent, the rebroadcast POSTs).
pub const REBROADCAST_BACKSTOP_LIMIT: u64 = 16;

/// Minimum age before an admitted proofless row is presence-probed. The
/// */15 proof passes only ever HELP a tx that mined; an admitted tx the
/// network never held (the #267 incident class) is invisible to them — but a
/// healthy 0-conf tx is also proofless for ~1 block interval, so probing
/// younger rows is wasted budget. 30 min = the [`PUSH_BACKSTOP_MIN_AGE_SECS`]
/// rationale: ≥95% of healthy txs have mined (and left the proofless set)
/// by then, so what remains is either slow-mine (probe answers "present",
/// one GET wasted) or the incident class this backstop exists for.
pub const REBROADCAST_MIN_AGE_SECS: u64 = 5 * 60;
// ^ #413 (2026-08-26): was PUSH_BACKSTOP_MIN_AGE_SECS (30 min) — but the
// phantom class (admitted, never delivered) is rescuable the moment the
// indexers' mempool-ingestion lag has passed. 5 min beats both that lag and
// the reconcile's displacement clock; a false "absent" on a young healthy
// row costs one idempotent rebroadcast. Candidacy now orders newest-first
// (rescued rows leave the set next tick, so the aged tail still drains)
// instead of RANDOM — under a 14-day junk backlog the RANDOM 16 almost
// never drew the fresh rescuable rows (measured live: phantoms sat hours
// while the pass ran every 15 min).

/// Maximum age for backstop CANDIDACY (bsv-low#273, gate LOW-1): a proofless
/// admitted row older than this stops being presence-probed/rebroadcast —
/// permanently-dead rows (superseded/conflicting, can never land) would
/// otherwise churn in the 16-random sample forever and dilute the
/// genuinely-rescuable ones. 14 days ≫ any honest rescue window (the #267
/// incident class was rescued within a day; a healthy tx mines in ~10 min).
/// NEVER a deletion: aged-out rows stay stored, stay served, and stay
/// candidates of the ordinary proof-completion passes; the #247
/// `unconfirmable` verdict is the terminal client-facing signal for the
/// truly dead.
pub const REBROADCAST_MAX_CANDIDATE_AGE_SECS: u64 = 14 * 24 * 3600;

/// Per-candidate EF-leg cap for the ancestry-first rebroadcast — mirrors the
/// corroboration leg cap's rationale (bound serial POST work; real LOW
/// ancestry runs ~8 unproven legs). Over the cap the candidate is SKIPPED
/// (counted, retried on a later tick when its ancestry may have compacted),
/// never partially rebroadcast out of order.
pub const REBROADCAST_MAX_LEGS: usize = 32;

/// Total broadcast POSTs allowed per tick across ALL candidates — the belt
/// that keeps a pathological backlog (many absent candidates × deep
/// ancestry) from eating the invocation's subrequest budget and starving
/// the passes that ran before it (this pass runs LAST; the belt just keeps
/// its own worst case bounded too).
pub const REBROADCAST_POST_BUDGET: usize = 48;

/// The classified network presence of an admitted tx (PURE — unit-tested).
///
/// Fail direction: a REBROADCAST is idempotent (the network dedupes /
/// answers already-known), so the harmful error is a MISSED rescue, not a
/// redundant one — but doctrine still requires that a NEGATIVE never rest
/// on one provider's word (#212/#213/#214), so `Absent` (the only verdict
/// that triggers action) needs BOTH indexers' definitive 404. Anything
/// uncertain is `Inconclusive` → skip, retried next tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkPresence {
    /// At least one indexer holds the tx (mempool or mined) — healthy;
    /// proof completion will pick it up when it mines.
    Present,
    /// BOTH indexers answered a definitive 404 — the admitted tx is absent
    /// from the network (the #267 incident class): rebroadcast.
    Absent,
    /// Faults / mixed answers — no action this tick.
    Inconclusive,
}

/// PURE: fold the two indexer observations (`Some(true)` present,
/// `Some(false)` definitive 404, `None` fault) into a [`NetworkPresence`].
pub fn classify_presence(bitails: Option<bool>, woc: Option<bool>) -> NetworkPresence {
    match (bitails, woc) {
        (Some(true), _) | (_, Some(true)) => NetworkPresence::Present,
        (Some(false), Some(false)) => NetworkPresence::Absent,
        _ => NetworkPresence::Inconclusive,
    }
}

/// Tally of one rebroadcast-backstop pass (logged by the cron / returned by
/// the admin route).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RebroadcastSummary {
    /// Proofless admitted rows old enough to probe this tick.
    pub scanned: usize,
    /// Rows an indexer still holds (healthy — no action).
    pub present: usize,
    /// Rows probed inconclusively (provider fault / one-sided 404) — no
    /// action, retried next tick.
    pub inconclusive: usize,
    /// Provably-absent rows whose stored BEEF was rebroadcast ancestry-first
    /// and whose SUBJECT the network then accepted.
    pub rebroadcast: usize,
    /// Absent rows whose rebroadcast did not land (definitive rejection,
    /// transport on both hosts, unparseable stored BEEF, or the leg cap) —
    /// logged, retried next tick.
    pub rebroadcast_failed: usize,
    /// Candidates skipped because the per-tick POST budget ran out.
    pub budget_skipped: usize,
}

/// PURE (bsv-low#273, gate NEW-LOW): the broadcastable legs for one stored
/// BEEF — `(subject_txid, [(leg_txid, bytes)])`, dependency-ordered,
/// subject last.
///
/// Primary: the EF batch (ancestry-first — ARC validates each EF
/// standalone). Fallback to the SUBJECT RAW alone when NO EF is
/// constructible, in EITHER dress:
/// - EMPTY batch — every tx claims a bump (the #268 mined-claim class);
/// - CONVERSION ERROR — the subject's inputs cannot be sourced, e.g. a
///   #268-M1 STRIPPED mined-claim row: the strip drops bump-only parents
///   (no raws existed to keep), so the stored single-tx BEEF has
///   unsourceable inputs and `beef_to_ef_batch` errors. Pre-fix that row
///   hit the Err path and counted `rebroadcast_failed` forever instead of
///   rescuing.
///
/// For both populations the parents are on-chain by (corroborated) claim,
/// so a raw broadcast is the right rescue; a network that cannot source
/// the parents simply fails the subject verdict (retried) — never a false
/// rescue. The subject txid is CONTENT-ADDRESSED from the raw itself.
///
/// `None` = nothing broadcastable (unparseable BEEF / txid-only subject) —
/// the caller counts `rebroadcast_failed` and retries later.
///
/// One broadcastable leg: `(leg_txid, bytes)` — EF bytes on the primary
/// path, the subject raw on the fallback.
type RebroadcastLeg = (String, Vec<u8>);
fn rebroadcast_legs(beef: &[u8]) -> Option<(String, Vec<RebroadcastLeg>)> {
    match crate::ef::beef_to_ef_batch(beef) {
        Ok((efs, subject_txid)) if !efs.is_empty() => Some((
            subject_txid,
            efs.into_iter().map(|e| (e.txid, e.ef)).collect(),
        )),
        Ok(_) | Err(_) => {
            let raw = crate::ef::proven_subject_raw(beef)?;
            let subject_txid = bsv_rs::transaction::Transaction::from_binary(&raw)
                .ok()?
                .id();
            Some((subject_txid.clone(), vec![(subject_txid, raw)]))
        }
    }
}

/// Bitails `GET /tx/{txid}` presence: `Some(true)` on 2xx, `Some(false)` on
/// a definitive 404, `None` on anything else (fault — never a verdict).
async fn bitails_presence(base: &str, txid: &str) -> Option<bool> {
    let url = format!("{}/tx/{}", base.trim_end_matches('/'), txid);
    match http_get(&url, None).await {
        Ok((status, _)) if (200..300).contains(&status) => Some(true),
        Ok((404, _)) => Some(false),
        _ => None,
    }
}

/// WoC `GET /tx/hash/{txid}` presence — same three-way contract.
async fn woc_presence(base: &str, api_key: Option<&str>, txid: &str) -> Option<bool> {
    let url = format!("{}/tx/hash/{}", base.trim_end_matches('/'), txid);
    let hdr = api_key.map(|k| ("woc-api-key", k));
    match http_get(&url, hdr).await {
        Ok((status, _)) if (200..300).contains(&status) => Some(true),
        Ok((404, _)) => Some(false),
        _ => None,
    }
}

/// The #273 backstop: admitted (broadcast-gated) rows that are STILL
/// proofless after [`REBROADCAST_MIN_AGE_SECS`] get an external presence
/// probe (Bitails + WoC — the existing courier hosts); a row BOTH indexers
/// definitively 404 is the #267 incident class (admitted, network-absent,
/// invisible to every proof pass) and its stored BEEF is rebroadcast
/// ANCESTRY-FIRST (each unproven EF leg in dependency order, subject last,
/// via the TAAL→GorillaPool gated transport). This automates the manual
/// rescue of 2026-07-28 ~01:39Z.
///
/// Bounds: the caller supplies `candidates` from the backstop's OWN
/// age-bracketed window ([`REBROADCAST_MIN_AGE_SECS`] ..
/// [`REBROADCAST_MAX_CANDIDATE_AGE_SECS`], RANDOM-sampled —
/// `D1Storage::find_rebroadcast_candidates`; gate LOW-1: permanently-dead
/// rows age OUT of candidacy instead of diluting the sample forever, and
/// are never deleted), [`REBROADCAST_MAX_LEGS`] EF legs per candidate,
/// [`REBROADCAST_POST_BUDGET`] broadcast POSTs per tick. Runs LAST in the
/// completion tick with its OWN bounds — it can never starve the proof
/// passes that precede it.
///
/// Fail-safe: probe faults and one-sided 404s act on nothing; a rebroadcast
/// failure writes nothing and is retried on a later tick; a rebroadcast of
/// a present tx (should the probes both lie) is idempotent.
pub async fn rebroadcast_absent_admitted(
    candidates: Vec<overlay_engine::storage::TransactionBeef>,
    taal_api_key: Option<&str>,
    woc_api_key: Option<&str>,
) -> RebroadcastSummary {
    let mut summary = RebroadcastSummary {
        scanned: candidates.len(),
        ..Default::default()
    };

    let mut post_budget = REBROADCAST_POST_BUDGET;

    for candidate in candidates {
        let txid = &candidate.txid;

        // Presence probe: Bitails first (WoC is the free-tier-429 host — it
        // is only consulted when Bitails did not already prove presence).
        let bitails = bitails_presence(DEFAULT_BITAILS_BASE, txid).await;
        if bitails == Some(true) {
            summary.present += 1;
            continue;
        }
        let woc = woc_presence(DEFAULT_WOC_BASE, woc_api_key, txid).await;
        match classify_presence(bitails, woc) {
            NetworkPresence::Present => {
                summary.present += 1;
                continue;
            }
            NetworkPresence::Inconclusive => {
                summary.inconclusive += 1;
                continue;
            }
            NetworkPresence::Absent => {}
        }

        // Provably absent from both indexers: rebroadcast the stored BEEF
        // ancestry-first ([`rebroadcast_legs`] — falls back to the SUBJECT
        // RAW when no EF is constructible, e.g. a #268-M1 STRIPPED
        // mined-claim row).
        let Some((subject_txid, legs)) = rebroadcast_legs(&candidate.beef) else {
            push_log(&format!(
                "[rebroadcast-backstop] {txid} absent but no broadcastable bytes — retry later"
            ));
            summary.rebroadcast_failed += 1;
            continue;
        };
        if legs.len() > REBROADCAST_MAX_LEGS {
            push_log(&format!(
                "[rebroadcast-backstop] {txid} has {} EF legs > cap {REBROADCAST_MAX_LEGS} — skipped",
                legs.len()
            ));
            summary.rebroadcast_failed += 1;
            continue;
        }
        if post_budget < legs.len() {
            summary.budget_skipped += 1;
            continue;
        }
        post_budget -= legs.len();

        // Ancestor verdicts are logged but IGNORED (they prime/dedupe); the
        // SUBJECT's verdict decides the counter. No admission state changes
        // here — landing is verified by the ordinary proof passes later.
        let mut subject_outcome: Option<Result<crate::broadcaster::ArcOutcome, String>> = None;
        for (leg_txid, bytes) in &legs {
            let out =
                crate::broadcaster::broadcast_tx_hex_gated(taal_api_key, &hex::encode(bytes)).await;
            if leg_txid == &subject_txid {
                subject_outcome = Some(out);
            } else if let Err(e) = out {
                push_log(&format!(
                    "[rebroadcast-backstop] {txid} ancestor {leg_txid} transport: {e}"
                ));
            }
        }
        match subject_outcome {
            Some(Ok(crate::broadcaster::ArcOutcome::Accepted(_))) => {
                push_log(&format!(
                    "[rebroadcast-backstop] {txid} RESCUED — absent from both indexers, rebroadcast accepted ({} leg(s))",
                    legs.len()
                ));
                summary.rebroadcast += 1;
            }
            Some(Ok(crate::broadcaster::ArcOutcome::AcceptedPending(_))) => {
                // #397: queued but UNWITNESSED — not a rescue yet. The next
                // backstop pass either finds it on an indexer (done) or
                // retries; never count RESCUED without a witness.
                push_log(&format!(
                    "[rebroadcast-backstop] {txid} rebroadcast sync-accepted but unwitnessed (#397) — retry later"
                ));
                summary.rebroadcast_failed += 1;
            }
            Some(Ok(crate::broadcaster::ArcOutcome::Rejected(r))) => {
                push_log(&format!(
                    "[rebroadcast-backstop] {txid} rebroadcast REJECTED ({r}) — retry later"
                ));
                summary.rebroadcast_failed += 1;
            }
            Some(Err(e)) => {
                push_log(&format!(
                    "[rebroadcast-backstop] {txid} rebroadcast transport failed ({e}) — retry later"
                ));
                summary.rebroadcast_failed += 1;
            }
            None => {
                // Subject leg absent from the batch — beef_to_ef_batch
                // guarantees it, so this is defensive only.
                summary.rebroadcast_failed += 1;
            }
        }
    }

    summary
}

// ============================================================================
// /arc-ingest push consumer (bsv-low #228 — push is the PRIMARY proof source)
// ============================================================================

/// What one pushed proof landed in the LOW pot stores.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PushedPotSummary {
    /// The `pot_beefs` row for this txid was stitched + compacted (it drops
    /// out of the pot-beef poll pass's candidate set).
    pub pot_beef_compacted: bool,
    /// `pot_records` rows upgraded to `spentConfirmed = 1` because this txid
    /// is their recorded spender (they drop out of the #186 spend chaser).
    pub spends_confirmed: usize,
    /// bsv-low#301 gate M1: rows selected by `spendingTxid = <this txid>`
    /// whose guarded confirm then MISSED — the pointer moved off this txid
    /// in the row-loop window (the #301 race, narrower window). Nothing
    /// written; the row re-chases or was competing-confirmed (the
    /// `complete_spend_confirmations` case analysis applies verbatim).
    pub spends_cas_missed: usize,
    /// bsv-low#301 gate M2: CAS confirm writes that ERRORED (storage/driver
    /// fault) — counted so a total-RETURNING driver failure self-announces
    /// here too, not only in the poll chaser.
    pub spends_cas_errors: usize,
}

impl PushedPotSummary {
    /// Whether the push landed in ANY pot store.
    pub fn landed_anything(&self) -> bool {
        self.pot_beef_compacted || self.spends_confirmed > 0
    }
}

/// wasm-safe log: `worker::console_log!` panics off-wasm ("function not
/// implemented on non-wasm32 targets"). Used by every path native unit tests
/// exercise — the push consumer, the params backfill, and (since the
/// bsv-low#304 gate round) the pot-beef poll pass incl. its
/// stored-bump-failed-reverify branch. `pub(crate)` since bsv-low#309: the
/// natively-tested advert-lifecycle passes log through it too.
pub(crate) fn push_log(msg: &str) {
    #[cfg(target_arch = "wasm32")]
    worker::console_log!("{}", msg);
    #[cfg(not(target_arch = "wasm32"))]
    eprintln!("{msg}");
}

/// Fold an `/arc-ingest`-pushed, ALREADY-chaintracks-VERIFIED bump for `txid`
/// into the LOW pot stores, so the poll passes skip the tx entirely:
///
/// 1. `pot_beefs`: if a stored BEEF for `txid` exists and is not yet
///    VERIFIED-proven (the `proof_verified` latch, bsv-low#304 — NOT byte
///    structure: a structurally-bumped admit row carries an untrusted bump
///    this free verified push should REPLACE and latch), stitch the bump,
///    trim, and [`PotStorage::compact_pot_beef`] (which re-checks the
///    proof, fail-closed) — same shape as one [`complete_pot_beef_proofs`]
///    candidate, minus the courier fetch.
/// 2. `pot_records`: every outpoint whose recorded spender is `txid` and is
///    still unconfirmed is upgraded via the #301 GUARDED CAS
///    ([`PotStorage::mark_confirmed_for_spender`]) — the spending tx
///    verifiably mined, which is exactly the #186 chaser's latch condition.
///    The rows were SELECTED by `spendingTxid = txid`, but the selection and
///    the per-row writes straddle awaits (gate M1 — the same race class the
///    chaser closed, narrower window): an unguarded confirmed `mark_spent`
///    would re-write the pointer from the stale selection, silently
///    resetting a reorg-confirmed S2 that landed mid-loop — invisible to
///    every re-chase. A CAS miss writes nothing and is counted
///    (`spends_cas_missed`); the chaser's re-visit case analysis applies
///    verbatim (still-unconfirmed rows re-surface, competing-confirmed rows
///    are terminal).
///
/// SECURITY PRECONDITION / LOAD-BEARING GUARD: the caller MUST have verified
/// `bump_hex` against chaintracks for `txid` first. This function has NO
/// chaintracks bar of its own — its ONLY chaintracks guard is the
/// `verify_bump` → 422 refusal in the `/arc-ingest` route
/// (`routes.rs::arc_ingest`, "Callback merklePath failed chaintracks
/// verification") sitting in front of its single production caller. Because
/// what it writes latches the bsv-low#304 `proof_verified` trust flag
/// (via `compact_pot_beef`), ADDING A NEW CALLER WITHOUT AN EQUIVALENT
/// CHAINTRACKS VERIFY WOULD REOPEN THE FAKE-BUMP HOLE #304 CLOSED. The
/// function still fails closed on its own STRUCTURAL account: a bump that
/// doesn't parse/stitch/prove writes nothing, and `compact_pot_beef`
/// re-checks the (structural) proof at the storage layer — but structure is
/// not chain truth; the route's 422 is the chain bar.
///
/// Best-effort per store: a failure in one store is logged and does not block
/// the other (the poll backstop still covers whatever didn't land).
pub async fn apply_pushed_proof_to_pot_stores(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    txid: &str,
    bump_hex: &str,
) -> PushedPotSummary {
    let mut summary = PushedPotSummary::default();

    // Defense-in-depth: the route has already chaintracks-verified this bump,
    // but a structurally malformed one (unparseable, or not containing this
    // txid's leaf) must latch NOTHING here either — fail-closed, the poll
    // backstop keeps covering the rows.
    let structurally_ok = bsv_rs::transaction::MerklePath::from_hex(bump_hex)
        .ok()
        .and_then(|mp| mp.compute_root(Some(txid)).ok())
        .is_some();
    if !structurally_ok {
        push_log(&format!(
            "[arc-ingest] {txid} pushed bump is malformed — nothing latched"
        ));
        return summary;
    }

    // 1. pot_beefs stitch + compact. Gate on the VERIFIED latch, not byte
    // structure (bsv-low#304 gate LOW-2): a structurally-bumped row whose
    // latch is still 0 carries an UNTRUSTED admit-path bump — this push's
    // route-verified bump replaces it and latches for free (no courier, no
    // poll-pass wait). A latch-read fault degrades to "unverified" — worst
    // case a redundant VERIFYING write, never a trust strengthening.
    let already_verified = pot_storage
        .pot_beef_proof_verified(txid)
        .await
        .unwrap_or(false);
    match pot_storage.get_beef(txid).await {
        Ok(Some(stored_beef)) if !already_verified => {
            match stitch_and_trim_pot_beef(txid, &stored_beef, bump_hex) {
                Some(compacted) => match pot_storage.compact_pot_beef(txid, &compacted).await {
                    Ok(()) => summary.pot_beef_compacted = true,
                    Err(e) => {
                        push_log(&format!("[arc-ingest] {txid} pot-beef compact failed: {e}"));
                    }
                },
                None => {
                    // Fail-closed: an unstitchable pushed bump writes nothing;
                    // the poll backstop retries this row later.
                    push_log(&format!(
                        "[arc-ingest] {txid} pot-beef stitch failed (backstop will retry)"
                    ));
                }
            }
        }
        Ok(_) => {} // no pot beef, or already VERIFIED-proven — nothing to do
        Err(e) => push_log(&format!("[arc-ingest] {txid} pot-beef read failed: {e}")),
    }

    // 2. pot_records spend-confirmation latch (this txid as the spender).
    // #284: a confirm-only latch — no spender raw in hand → verdict = None
    // (the stored verdict/verdictTxid are left UNCHANGED); the spentHeight
    // rides along from the (route-verified, structurally re-checked) bump.
    let spent_height = bsv_rs::transaction::MerklePath::from_hex(bump_hex)
        .ok()
        .map(|mp| u64::from(mp.block_height));
    match pot_storage.find_unconfirmed_by_spending_txid(txid).await {
        Ok(records) => {
            for rec in records {
                // #301 gate M1: the guarded CAS — the row was selected BY
                // spendingTxid = txid, so a hit changes no pointer (and the
                // old unguarded write's pointer re-write is gone); a miss
                // means the pointer moved mid-loop → write NOTHING, count.
                match pot_storage
                    .mark_confirmed_for_spender(&rec.txid, rec.output_index, txid, spent_height)
                    .await
                {
                    Ok(true) => summary.spends_confirmed += 1,
                    Ok(false) => {
                        summary.spends_cas_missed += 1;
                        push_log(&format!(
                            "[arc-ingest] {}:{} pointer moved off {txid} in the row-loop window \
                             (bsv-low#301) — confirmed NOTHING; the poll chaser covers the row",
                            rec.txid, rec.output_index
                        ));
                    }
                    Err(e) => {
                        summary.spends_cas_errors += 1;
                        push_log(&format!(
                            "[arc-ingest] {}:{} spend-confirm CAS failed: {e}",
                            rec.txid, rec.output_index
                        ));
                    }
                }
            }
        }
        Err(e) => push_log(&format!("[arc-ingest] {txid} spender lookup failed: {e}")),
    }

    summary
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use bsv_rs::transaction::MockChainTracker;

    const TXID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HEIGHT: u32 = 830_000;

    /// A minimal valid single-tx-block BUMP proving `txid` as the sole tx —
    /// whose merkle root IS `txid`. Mirrors the proven lookup_service fixture.
    fn single_tx_bump(txid: &str, height: u32) -> MerklePath {
        MerklePath::new(height, vec![vec![MerklePathLeaf::new_txid(0, txid.into())]])
            .expect("valid single-leaf merkle path")
    }

    // ── 0. #273 rebroadcast-backstop presence classification ─────────────────

    #[test]
    fn presence_positive_from_either_indexer_is_present() {
        // A single indexer holding the tx is enough to STAND DOWN (positive
        // presence is cheap to trust — the harmful error is a missed rescue,
        // and proof completion covers a present tx).
        assert_eq!(
            classify_presence(Some(true), None),
            NetworkPresence::Present
        );
        assert_eq!(
            classify_presence(None, Some(true)),
            NetworkPresence::Present
        );
        assert_eq!(
            classify_presence(Some(true), Some(false)),
            NetworkPresence::Present
        );
        assert_eq!(
            classify_presence(Some(false), Some(true)),
            NetworkPresence::Present
        );
    }

    #[test]
    fn presence_absent_requires_both_definitive_404s() {
        // The ACTION verdict needs BOTH indexers' definitive 404 — a negative
        // never rests on one provider's word (#212/#213/#214 doctrine).
        assert_eq!(
            classify_presence(Some(false), Some(false)),
            NetworkPresence::Absent
        );
        // One-sided 404s and faults are inconclusive → no action, retried.
        assert_eq!(
            classify_presence(Some(false), None),
            NetworkPresence::Inconclusive
        );
        assert_eq!(
            classify_presence(None, Some(false)),
            NetworkPresence::Inconclusive
        );
        assert_eq!(classify_presence(None, None), NetworkPresence::Inconclusive);
    }

    // ── 0b. #273 rebroadcast legs (gate NEW-LOW: the stripped-row rescue) ────

    /// The all-proven fixture BEEF (subject a7d76588… + its bump — the
    /// mined-claim shape; shared with ef.rs's suite).
    const PARENT_BEEF_HEX: &str = include_str!("../tests/fixtures/ef/parent_a7d76588_beef.hex");

    #[test]
    fn stripped_beef_row_rescues_via_the_subject_raw() {
        use bsv_rs::transaction::{Beef, Transaction};
        // A #268-M1 STRIPPED mined-claim row, built by the REAL strip
        // producer: the bump-only parent does not survive, so the stored
        // BEEF is a single proofless tx with unsourceable inputs.
        let beef = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap().to_binary();
        let subject_txid = {
            let b = Beef::from_binary(&beef).unwrap();
            b.txs.last().unwrap().txid()
        };
        let stripped =
            crate::ef::strip_subject_bump(&beef, &subject_txid).expect("fixture must sanitize");
        // Premise pinned: NO EF is constructible from the stripped row —
        // exactly the shape that previously dead-ended in rebroadcast_failed.
        assert!(
            crate::ef::beef_to_ef_batch(&stripped).is_err(),
            "a stripped bump-only-parent BEEF must not EF-convert"
        );

        // The backstop's real leg producer falls back to the SUBJECT RAW:
        // one leg, content-addressed to the subject, bytes = the raw tx.
        let (subj, legs) =
            rebroadcast_legs(&stripped).expect("a stripped row must still be broadcastable");
        assert_eq!(subj, subject_txid);
        assert_eq!(legs.len(), 1, "subject raw alone");
        assert_eq!(legs[0].0, subject_txid);
        let parsed = Transaction::from_binary(&legs[0].1).unwrap();
        assert_eq!(
            parsed.id(),
            subject_txid,
            "the broadcast bytes ARE the subject raw"
        );

        // The ordinary shapes are untouched: a normal unproven-subject BEEF
        // still yields its EF legs (subject last) …
        let unproven = {
            let b = Beef::from_hex(PARENT_BEEF_HEX.trim()).unwrap();
            let raw = b.txs.last().unwrap().tx().unwrap().to_hex();
            let mut nb = Beef::new();
            nb.merge_transaction(Transaction::from_hex(&raw).unwrap());
            nb.to_binary()
        };
        let (subj2, legs2) = rebroadcast_legs(&unproven).unwrap();
        assert_eq!(subj2, subject_txid);
        assert_eq!(legs2.last().unwrap().0, subject_txid);
        // … an all-proven (unstripped mined-claim) BEEF falls back to raw …
        let (subj3, legs3) = rebroadcast_legs(&beef).unwrap();
        assert_eq!((subj3.as_str(), legs3.len()), (subject_txid.as_str(), 1));
        // … and garbage is honestly unbroadcastable.
        assert!(rebroadcast_legs(&[0xde, 0xad]).is_none());
    }

    // ── 1. Arcade merklePath extraction ──────────────────────────────────────

    #[test]
    fn arcade_mined_with_merklepath_extracts_bump() {
        let bump_hex = single_tx_bump(TXID, HEIGHT).to_hex();
        let body = format!(
            r#"{{"txid":"{TXID}","txStatus":"MINED","blockHeight":{HEIGHT},"merklePath":"{bump_hex}"}}"#
        );
        assert_eq!(
            parse_arcade_merklepath(&body).as_deref(),
            Some(bump_hex.as_str())
        );
    }

    #[test]
    fn arcade_unmined_yields_none() {
        // SEEN_ON_NETWORK (not mined) → no proof; the ladder retries next tick.
        let body = format!(r#"{{"txid":"{TXID}","txStatus":"SEEN_ON_NETWORK"}}"#);
        assert!(parse_arcade_merklepath(&body).is_none());
    }

    #[test]
    fn arcade_mined_without_merklepath_yields_none() {
        let body = format!(r#"{{"txid":"{TXID}","txStatus":"MINED"}}"#);
        assert!(parse_arcade_merklepath(&body).is_none());
        // Empty merklePath is also nothing.
        let empty = format!(r#"{{"txid":"{TXID}","txStatus":"MINED","merklePath":""}}"#);
        assert!(parse_arcade_merklepath(&empty).is_none());
    }

    // ── 2. TSC → BUMP conversion ─────────────────────────────────────────────

    #[test]
    fn tsc_json_converts_to_parseable_bump() {
        let json = r#"{
            "index": 0,
            "txOrId": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "target": "0000000000000000000000000000000000000000000000000000000000000000",
            "nodes": [
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            ]
        }"#;
        let bump_hex = tsc_json_to_bump_hex(json, HEIGHT).expect("TSC converts");
        let mp = MerklePath::from_hex(&bump_hex).expect("BUMP parses back");
        assert_eq!(mp.block_height, HEIGHT);
        assert_eq!(mp.path.len(), 3);
    }

    #[test]
    fn tsc_json_rejects_malformed() {
        assert!(tsc_json_to_bump_hex("not json", HEIGHT).is_none());
        assert!(tsc_json_to_bump_hex("{}", HEIGHT).is_none());
        // A bad-length node hash is rejected.
        let bad = r#"{"index":0,"txOrId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","nodes":["zz"]}"#;
        assert!(tsc_json_to_bump_hex(bad, HEIGHT).is_none());
    }

    // ── 3. verify_bump against chaintracks (rejects a forged root) ───────────

    #[tokio::test]
    async fn verify_bump_accepts_a_root_the_tracker_confirms() {
        // Single-leaf bump: its computed root IS the txid. A tracker that knows
        // that root at that height confirms it.
        let bump_hex = single_tx_bump(TXID, HEIGHT).to_hex();
        let mut tracker = MockChainTracker::new(HEIGHT + 6);
        tracker.add_root(HEIGHT, TXID.to_string());
        assert!(verify_bump(Some(&tracker), &bump_hex, TXID).await);
    }

    #[tokio::test]
    async fn verify_bump_rejects_a_forged_root() {
        // The tracker only vouches for a DIFFERENT root at this height → the
        // bump's real root fails verification (fail-closed, no positive).
        let bump_hex = single_tx_bump(TXID, HEIGHT).to_hex();
        let mut tracker = MockChainTracker::new(HEIGHT + 6);
        tracker.add_root(
            HEIGHT,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        );
        assert!(!verify_bump(Some(&tracker), &bump_hex, TXID).await);
    }

    #[tokio::test]
    async fn verify_bump_fails_closed_without_a_tracker() {
        let bump_hex = single_tx_bump(TXID, HEIGHT).to_hex();
        assert!(!verify_bump(None, &bump_hex, TXID).await);
    }

    #[tokio::test]
    async fn verify_bump_rejects_garbage_bump_hex() {
        let mut tracker = MockChainTracker::new(HEIGHT + 6);
        tracker.add_root(HEIGHT, TXID.to_string());
        assert!(!verify_bump(Some(&tracker), "deadbeef", TXID).await);
    }

    // ── 4. unmined at every tier → the ladder yields None ────────────────────

    #[tokio::test]
    async fn ladder_yields_none_when_verify_never_passes() {
        // A tracker that vouches for NOTHING → even a well-formed bump can't
        // pass verify, so the whole ladder degrades to None (retry), never a
        // spurious proof. (Network tiers are exercised on mainnet in P5.)
        let bump_hex = single_tx_bump(TXID, HEIGHT).to_hex();
        let tracker = MockChainTracker::new(HEIGHT + 6); // no roots added
        assert!(!verify_bump(Some(&tracker), &bump_hex, TXID).await);
    }

    // ── 5. spend-confirmation chaser pass (#186) ─────────────────────────────

    use overlay_discovery::pot::storage::{
        MemoryPotStorage, PotRecord, PotStorage, PotStorageError, VerdictWrite,
    };

    /// A fetcher whose `verified_proof_for` returns a (dummy) verified bump ONLY
    /// for the txids in `minable` — models the chaintracks-verified vs unmined
    /// outcome without hitting the network (the concrete ChainProofFetcher is
    /// network-only). `fetch_ancestor` is never called by the pass.
    #[derive(Default)]
    struct MockProofFetcher {
        minable: std::collections::HashSet<String>,
        /// `(txid, vout)` → the spender the chain HINT names (`resolve_spender`).
        spender_hints: std::collections::HashMap<(String, u32), String>,
        /// spender → the raw hex `spender_binding_raw` hands back for it
        /// (present = binds; absent = fetched-but-does-not-bind).
        binding_raw: std::collections::HashMap<String, String>,
        /// When set, `resolve_spender` errors (the transport-fault path).
        hint_fault: bool,
        /// txid → a REAL bump hex to serve instead of the opaque "beefbump"
        /// (cells that exercise BEEF assembly / height parsing).
        real_bumps: std::collections::HashMap<String, String>,
    }

    #[async_trait(?Send)]
    impl AncestorFetcher for MockProofFetcher {
        async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
            Err(GASPError::NodeNotFound(format!(
                "mock: no ancestor for {txid}"
            )))
        }
        async fn verified_proof_for(&self, txid: &str) -> Option<String> {
            self.minable.contains(txid).then(|| {
                self.real_bumps
                    .get(txid)
                    .cloned()
                    .unwrap_or_else(|| "beefbump".to_string())
            })
        }
        async fn resolve_spender(&self, txid: &str, vout: u32) -> Result<Option<String>, String> {
            if self.hint_fault {
                return Err("mock: hint transport down".into());
            }
            Ok(self.spender_hints.get(&(txid.to_string(), vout)).cloned())
        }
        async fn spender_binding_raw(
            &self,
            spender: &str,
            _txid: &str,
            _vout: u32,
        ) -> Result<Option<String>, String> {
            Ok(self.binding_raw.get(spender).cloned())
        }
    }

    fn spent_unconfirmed(txid: &str, spender: &str) -> PotRecord {
        PotRecord {
            txid: txid.into(),
            output_index: 0,
            spent: true,
            spending_txid: Some(spender.into()),
            spent_confirmed: false,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn spend_confirmation_upgrades_when_spend_is_mined() {
        let store = MemoryPotStorage::new();
        // Admit then record a 0-conf spend (spent, unconfirmed).
        store
            .store_record(&PotRecord {
                txid: "potA".into(),
                output_index: 0,
                spent: false,
                spending_txid: None,
                spent_confirmed: false,
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .mark_spent("potA", 0, "settleA", false, None, None, None)
            .await
            .unwrap();

        let fetcher = MockProofFetcher {
            minable: ["settleA".to_string()].into_iter().collect(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.scanned, 1);
        assert_eq!(s.confirmed, 1);
        assert_eq!(s.still_unconfirmed, 0);

        // The row is now SPV-confirmed and drops out of the candidate set.
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed, "a verified spend latches spentConfirmed");
        assert_eq!(r.spending_txid.as_deref(), Some("settleA"));
        assert!(store
            .find_spent_unconfirmed(10, 0)
            .await
            .unwrap()
            .is_empty());
    }

    /// A pot row spent-marked by an UNCONFIRMED claim `claimed` (the parked
    /// non-final refund shape), stored and ready for the chaser.
    async fn pot_with_parked_claim(store: &MemoryPotStorage, pot: &str, claimed: &str) {
        store
            .store_record(&PotRecord {
                txid: pot.into(),
                output_index: 0,
                spent: false,
                spending_txid: None,
                spent_confirmed: false,
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .mark_spent(pot, 0, claimed, false, None, None, Some(false))
            .await
            .unwrap();
    }

    /// THE LIVE REGRESSION (2026-08-18, pot e450f668…:0): the recorded pointer
    /// is a parked non-final refund that will never mine; the actual settle
    /// mined an hour earlier. Full ladder green (hint + binding + proof) ⇒
    /// the pointer is DISPLACED to the true spender, spentConfirmed latched.
    #[tokio::test]
    async fn a_mined_true_spender_displaces_a_parked_claim() {
        let store = MemoryPotStorage::new();
        pot_with_parked_claim(&store, "potA", "refundA").await;
        let fetcher = MockProofFetcher {
            minable: ["settleA".to_string()].into_iter().collect(),
            spender_hints: [(("potA".to_string(), 0u32), "settleA".to_string())]
                .into_iter()
                .collect(),
            binding_raw: [("settleA".to_string(), "not-parseable-raw".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.displaced, 1);
        assert_eq!(s.displace_attempts, 1);
        assert_eq!(s.displace_faults, 0);
        assert_eq!(
            s.still_unconfirmed, 0,
            "a displaced row ENDED the tick confirmed — it must not read as still-unconfirmed"
        );
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("settleA"),
            "the pointer must name the TRUE spender after displacement"
        );
        assert!(r.spent_confirmed, "displacement latches spentConfirmed");
        // A displaced row drops out of the candidate set — the loop ends.
        assert!(store
            .find_spent_unconfirmed(10, 0)
            .await
            .unwrap()
            .is_empty());
    }

    /// An unmined hint must NEVER displace: that would let one mempool claim
    /// overwrite another. Binding green, proof absent ⇒ nothing written.
    #[tokio::test]
    async fn an_unmined_hint_never_displaces() {
        let store = MemoryPotStorage::new();
        pot_with_parked_claim(&store, "potA", "refundA").await;
        let fetcher = MockProofFetcher {
            spender_hints: [(("potA".to_string(), 0u32), "settleA".to_string())]
                .into_iter()
                .collect(),
            binding_raw: [("settleA".to_string(), "not-parseable-raw".to_string())]
                .into_iter()
                .collect(),
            ..Default::default() // minable EMPTY: the hint is not mined
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.displaced, 0);
        assert_eq!(s.displace_attempts, 1);
        assert_eq!(s.displace_faults, 0, "unmined is a wait, not a fault");
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("refundA"),
            "pointer untouched"
        );
        assert!(!r.spent_confirmed);
    }

    /// A hint whose raw bytes do NOT consume the outpoint is a lying (or
    /// garbled) indexer answer — REFUSED at the binding, counted as a fault,
    /// never written, even though the liar's tx is "mined".
    #[tokio::test]
    async fn a_hint_that_fails_the_input_binding_is_refused() {
        let store = MemoryPotStorage::new();
        pot_with_parked_claim(&store, "potA", "refundA").await;
        let fetcher = MockProofFetcher {
            minable: ["liarTx".to_string()].into_iter().collect(),
            spender_hints: [(("potA".to_string(), 0u32), "liarTx".to_string())]
                .into_iter()
                .collect(),
            ..Default::default() // binding_raw EMPTY: the bytes do not bind
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.displaced, 0);
        assert_eq!(
            s.displace_faults, 1,
            "a non-binding hint is a counted refusal"
        );
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("refundA"),
            "pointer untouched"
        );
        assert!(!r.spent_confirmed);
    }

    /// The hint naming the RECORDED pointer is the ordinary "not mined yet"
    /// case — no displacement machinery beyond the equality check runs.
    #[tokio::test]
    async fn a_hint_agreeing_with_the_recorded_pointer_is_left_to_the_proof_chase() {
        let store = MemoryPotStorage::new();
        pot_with_parked_claim(&store, "potA", "refundA").await;
        let fetcher = MockProofFetcher {
            spender_hints: [(("potA".to_string(), 0u32), "refundA".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.displaced, 0);
        assert_eq!(s.displace_attempts, 1);
        assert_eq!(s.displace_faults, 0);
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("refundA"));
        assert!(!r.spent_confirmed);
    }

    /// No hint at all: nothing to reconcile against; the ordinary chase keeps
    /// the row for next tick. Entered (counted) but neither fault nor write.
    #[tokio::test]
    async fn no_hint_leaves_the_row_to_the_ordinary_chase() {
        let store = MemoryPotStorage::new();
        pot_with_parked_claim(&store, "potA", "refundA").await;
        let fetcher = MockProofFetcher::default();
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.displaced, 0);
        assert_eq!(s.displace_attempts, 1);
        assert_eq!(s.displace_faults, 0);
        assert!(
            !store
                .get_spent_status("potA", 0)
                .await
                .unwrap()
                .unwrap()
                .spent_confirmed
        );
    }

    /// A hint transport fault is COUNTED and skipped — never read as "no
    /// spender" (unknown must not read as fine), never a write.
    #[tokio::test]
    async fn a_hint_transport_fault_is_counted_never_a_verdict() {
        let store = MemoryPotStorage::new();
        pot_with_parked_claim(&store, "potA", "refundA").await;
        let fetcher = MockProofFetcher {
            hint_fault: true,
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.displaced, 0);
        assert_eq!(s.displace_faults, 1);
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("refundA"),
            "pointer untouched"
        );
        assert!(!r.spent_confirmed);
    }

    /// The reconcile pipeline is BOUNDED per tick: with more displaceable rows
    /// than the cap, exactly `DISPLACE_CAP_PER_TICK` are attempted; the rest
    /// stay ordinary still-unconfirmed candidates for the next (random-sampled)
    /// tick.
    #[tokio::test]
    async fn the_reconcile_pipeline_is_bounded_per_tick() {
        let store = MemoryPotStorage::new();
        let n = DISPLACE_CAP_PER_TICK + 2;
        let mut hints = std::collections::HashMap::new();
        let mut minable = std::collections::HashSet::new();
        let mut binding_raw = std::collections::HashMap::new();
        for i in 0..n {
            let pot = format!("pot{i}");
            let claim = format!("refund{i}");
            let truth = format!("settle{i}");
            pot_with_parked_claim(&store, &pot, &claim).await;
            hints.insert((pot, 0u32), truth.clone());
            minable.insert(truth.clone());
            binding_raw.insert(truth, "not-parseable-raw".to_string());
        }
        let fetcher = MockProofFetcher {
            minable,
            spender_hints: hints,
            binding_raw,
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.displace_attempts, DISPLACE_CAP_PER_TICK);
        assert_eq!(s.displaced, DISPLACE_CAP_PER_TICK);
        assert_eq!(
            s.still_unconfirmed,
            n - DISPLACE_CAP_PER_TICK,
            "displaced rows ended confirmed; only the uncapped remainder is still unconfirmed"
        );
        let remaining = store.find_spent_unconfirmed(20, 0).await.unwrap();
        assert_eq!(remaining.len(), n - DISPLACE_CAP_PER_TICK);
    }

    // ══ The reconcile's CONCRETE layer (gate findings 1/4/7: the ladder tests
    // above stub these; the URL/parse/walk are where the HIGH shipped) ══

    /// FINDING-1 PIN + ladder-order URL pins: every rung's route is the one
    /// that EXISTS, live-probed 2026-08-18. WoC's plausible `/out/` variant is
    /// a router miss; BananaBlocks' `_`/`:`-joined outpoint forms 400.
    #[test]
    fn every_hint_rung_url_is_the_route_that_exists() {
        let woc = woc_spent_url("https://api.whatsonchain.com/v1/bsv/main", "aabb", 3);
        assert_eq!(
            woc,
            "https://api.whatsonchain.com/v1/bsv/main/tx/aabb/3/spent"
        );
        assert!(
            !woc.contains("/out/"),
            "the /out/ shape is a router miss, not an API answer"
        );
        assert_eq!(
            bananablocks_spend_url("https://bananablocks.com/api/v1", "aabb", 3),
            "https://bananablocks.com/api/v1/txo/aabb/3/spend"
        );
        assert_eq!(
            bitails_spent_url("https://api.bitails.io", "aabb", 3),
            "https://api.bitails.io/tx/aabb/output/3/spent"
        );
    }

    const OUTPOINT_TX: &str = "e450f6686efb27662a387fd7af0fb7d992648186d3e2e219ef9cd1af72c51d58";

    #[test]
    fn bananablocks_parse_matches_the_live_shapes() {
        let spender = "3d".repeat(32);
        let spent = format!(
            "{{\"spent\":true,\"spentTxid\":\"{}\",\"txid\":\"{OUTPOINT_TX}\",\"vout\":0}}",
            spender.to_uppercase()
        );
        assert_eq!(
            parse_bananablocks_spend_body(200, &spent, OUTPOINT_TX),
            Ok(Some(spender))
        );
        let unspent = format!("{{\"spent\":false,\"txid\":\"{OUTPOINT_TX}\",\"vout\":0}}");
        assert_eq!(
            parse_bananablocks_spend_body(200, &unspent, OUTPOINT_TX),
            Ok(None)
        );
        assert_eq!(
            parse_bananablocks_spend_body(
                404,
                "{\"error\":\"Transaction not found\"}",
                OUTPOINT_TX
            ),
            Ok(None)
        );
        // Drift: spent:true without a usable spender = FAULT, never "no hint".
        assert!(parse_bananablocks_spend_body(200, "{\"spent\":true}", OUTPOINT_TX).is_err());
        assert!(parse_bananablocks_spend_body(200, "{}", OUTPOINT_TX).is_err());
        assert!(parse_bananablocks_spend_body(500, "boom", OUTPOINT_TX).is_err());
        // An indexer echoing the OUTPOINT's own txid is noise, and with no
        // other candidate that makes the answer drift ⇒ fault.
        let echo = format!("{{\"spent\":true,\"spentTxid\":\"{OUTPOINT_TX}\"}}");
        assert!(parse_bananablocks_spend_body(200, &echo, OUTPOINT_TX).is_err());
    }

    #[test]
    fn bitails_parse_faults_on_todays_500_and_reads_both_spender_spellings() {
        // Today's live behavior: 500 Unhandled Error ⇒ FAULT (falls through).
        assert!(parse_bitails_spend_body(500, "{\"statusCode\":500}", OUTPOINT_TX).is_err());
        let spender = "3d".repeat(32);
        for body in [
            format!("{{\"spent\":true,\"spentTxid\":\"{spender}\"}}"),
            format!("{{\"spent\":true,\"spentIn\":{{\"txid\":\"{spender}\"}}}}"),
        ] {
            assert_eq!(
                parse_bitails_spend_body(200, &body, OUTPOINT_TX),
                Ok(Some(spender.clone()))
            );
        }
        assert_eq!(
            parse_bitails_spend_body(200, "{\"spent\":false}", OUTPOINT_TX),
            Ok(None)
        );
        assert_eq!(parse_bitails_spend_body(404, "nope", OUTPOINT_TX), Ok(None));
        assert!(parse_bitails_spend_body(200, "{\"spent\":true}", OUTPOINT_TX).is_err());
    }

    /// FINDING-7 PIN (WoC rung): a 2xx without a well-formed txid is a FAULT,
    /// never "no hint" — and the excerpt is char-boundary-safe.
    #[test]
    fn woc_parse_garbled_200_is_a_fault_not_a_no() {
        let spender = "f3".repeat(32);
        let ok = format!("{{\"txid\":\"{}\",\"vin\":12}}", spender.to_uppercase());
        assert_eq!(
            parse_woc_spend_body(200, &ok, OUTPOINT_TX),
            Ok(Some(spender))
        );
        assert_eq!(
            parse_woc_spend_body(404, "404 page not found", OUTPOINT_TX),
            Ok(None)
        );
        for body in [
            "{}",
            "{\"txid\":null}",
            "{\"txid\":\"short\"}",
            "not json",
            "{\"txid\":12}",
        ] {
            assert!(
                parse_woc_spend_body(200, body, OUTPOINT_TX).is_err(),
                "{body:?}"
            );
        }
        assert!(parse_woc_spend_body(500, "boom", OUTPOINT_TX).is_err());
        assert!(parse_woc_spend_body(429, "rate", OUTPOINT_TX).is_err());
        // A multibyte char straddling the 120-byte excerpt cut must yield an
        // Err, never a char-boundary panic that kills the whole tick.
        let straddle = format!("{{\"note\":\"{}\"}}", "é".repeat(80));
        assert!(parse_woc_spend_body(200, &straddle, OUTPOINT_TX).is_err());
    }

    /// A REAL spender tx for the input-walk cells: one input consuming
    /// `pot_txid:vout`, one dust output (bytes serialize + reparse cleanly).
    fn real_spender_raw(pot_txid: &str, vout: u32) -> String {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{TransactionInput, TransactionOutput};
        let mut tx = Transaction::new();
        tx.inputs
            .push(TransactionInput::new(pot_txid.to_string(), vout));
        tx.outputs.push(TransactionOutput {
            satoshis: Some(1),
            locking_script: LockingScript::from_hex("51").unwrap(),
            change: false,
        });
        tx.to_hex()
    }

    /// FINDING-4 PIN: the REAL input walk against real bytes — each conjunct
    /// (txid equality, vout equality) falls to its own cell, so deleting
    /// either breaks a test (the mutation the mock layer could never see).
    #[test]
    fn the_input_walk_binds_only_the_exact_outpoint() {
        let pot = "ab".repeat(32);
        let raw = real_spender_raw(&pot, 2);
        assert_eq!(
            tx_consumes_outpoint(&raw, &pot, 2),
            Ok(true),
            "binds its outpoint"
        );
        assert_eq!(
            tx_consumes_outpoint(&raw, &pot, 3),
            Ok(false),
            "wrong vout must not bind"
        );
        let other = "cd".repeat(32);
        assert_eq!(
            tx_consumes_outpoint(&raw, &other, 2),
            Ok(false),
            "wrong txid must not bind"
        );
        assert_eq!(
            tx_consumes_outpoint(&raw, &pot.to_uppercase(), 2),
            Ok(true),
            "txid compare is case-insensitive"
        );
        assert!(
            tx_consumes_outpoint("zz-not-hex", &pot, 2).is_err(),
            "garbage is a fault"
        );
    }

    /// The assembled displaced-spender BEEF is a REAL atomic beef: it parses,
    /// carries the spender at its bump height, and garbage in either half is
    /// an Err (the leg logs + proceeds — enrichment never blocks the pointer).
    #[test]
    fn assemble_spender_beef_round_trips_and_fails_closed() {
        let pot = "ab".repeat(32);
        let raw = real_spender_raw(&pot, 0);
        let spender = Transaction::from_hex(&raw).unwrap().id();
        let bump_hex = single_tx_bump(&spender, 901_000).to_hex();
        let beef = assemble_spender_beef(&raw, &bump_hex, &spender).unwrap();
        assert_eq!(stored_bump_height(&beef, &spender), Some(901_000));
        assert!(assemble_spender_beef("zz", &bump_hex, &spender).is_err());
        assert!(assemble_spender_beef(&raw, "zz", &spender).is_err());
    }

    /// End-to-end enrichment: a displacement whose binding raw + bump are REAL
    /// leaves the spender's atomic BEEF in the store (classifiable win) and
    /// stamps bytes-finality with the `output_spent` rule (final bytes ⇒ true).
    #[tokio::test]
    async fn displacement_persists_the_spender_beef_and_bytes_finality() {
        let store = MemoryPotStorage::new();
        let pot = "ab".repeat(32);
        pot_with_parked_claim(&store, &pot, "refundA").await;
        let raw = real_spender_raw(&pot, 0);
        let spender = Transaction::from_hex(&raw).unwrap().id();
        let fetcher = MockProofFetcher {
            minable: [spender.clone()].into_iter().collect(),
            spender_hints: [((pot.clone(), 0u32), spender.clone())]
                .into_iter()
                .collect(),
            binding_raw: [(spender.clone(), raw)].into_iter().collect(),
            real_bumps: [(spender.clone(), single_tx_bump(&spender, 901_000).to_hex())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.displaced, 1);
        let r = store.get_spent_status(&pot, 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some(spender.as_str()));
        assert_eq!(
            r.spender_final,
            Some(true),
            "lock_time 0 bytes are final by the output_spent rule"
        );
        assert!(
            store.get_beef(&spender).await.unwrap().is_some(),
            "the proven bytes must be durably stored so the win stays classifiable"
        );
    }

    /// The leg maps a displacement-CAS MISS (pointer moved/competing-confirmed
    /// inside the verify window) to cas_missed — wrote nothing, retried next
    /// tick. Pinned through a store whose displace CAS always misses.
    struct DisplaceMissStore {
        inner: MemoryPotStorage,
    }

    #[async_trait(?Send)]
    impl PotStorage for DisplaceMissStore {
        async fn store_record(&self, r: &PotRecord) -> Result<(), PotStorageError> {
            self.inner.store_record(r).await
        }
        async fn get_spent_status(
            &self,
            txid: &str,
            output_index: u32,
        ) -> Result<Option<PotRecord>, PotStorageError> {
            self.inner.get_spent_status(txid, output_index).await
        }
        async fn find_spent_unconfirmed(
            &self,
            limit: u64,
            min_age_secs: u64,
        ) -> Result<Vec<PotRecord>, PotStorageError> {
            self.inner.find_spent_unconfirmed(limit, min_age_secs).await
        }
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
            self.inner
                .mark_spent(
                    txid,
                    output_index,
                    spending_txid,
                    confirmed,
                    verdict,
                    spent_height,
                    spender_final,
                )
                .await
        }
        async fn mark_confirmed_for_spender(
            &self,
            txid: &str,
            output_index: u32,
            spending_txid: &str,
            spent_height: Option<u64>,
        ) -> Result<bool, PotStorageError> {
            self.inner
                .mark_confirmed_for_spender(txid, output_index, spending_txid, spent_height)
                .await
        }
        async fn displace_spend_for(
            &self,
            _txid: &str,
            _output_index: u32,
            _from_spender: &str,
            _to_spender: &str,
            _spent_height: Option<u64>,
            _spender_final: Option<bool>,
        ) -> Result<bool, PotStorageError> {
            Ok(false) // the guard missed: pointer moved inside the window
        }
        async fn store_beef(&self, txid: &str, beef: &[u8]) -> Result<(), PotStorageError> {
            self.inner.store_beef(txid, beef).await
        }
        async fn get_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, PotStorageError> {
            self.inner.get_beef(txid).await
        }
    }

    #[tokio::test]
    async fn a_displacement_cas_miss_is_counted_and_writes_nothing() {
        let store = DisplaceMissStore {
            inner: MemoryPotStorage::new(),
        };
        pot_with_parked_claim(&store.inner, "potA", "refundA").await;
        let fetcher = MockProofFetcher {
            minable: ["settleA".to_string()].into_iter().collect(),
            spender_hints: [(("potA".to_string(), 0u32), "settleA".to_string())]
                .into_iter()
                .collect(),
            binding_raw: [("settleA".to_string(), "not-parseable-raw".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.displaced, 0);
        assert_eq!(
            s.cas_missed, 1,
            "a guard miss is counted, never silently dropped"
        );
        assert_eq!(
            s.still_unconfirmed, 1,
            "the row stays a candidate for next tick"
        );
        let r = store
            .inner
            .get_spent_status("potA", 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("refundA"),
            "nothing was written"
        );
        assert!(!r.spent_confirmed);
    }

    /// The Memory displacement CAS itself: both guard arms + the happy write
    /// (pointer + confirm latch + incoming height/finality; verdict UNTOUCHED).
    #[tokio::test]
    async fn memory_displace_cas_guards_and_writes() {
        let store = MemoryPotStorage::new();
        pot_with_parked_claim(&store, "potA", "refundA").await;
        // Stale verdict keyed to the OLD spender must survive displacement.
        store
            .mark_spent(
                "potA",
                0,
                "refundA",
                false,
                Some(VerdictWrite::bare("refund")),
                None,
                Some(false),
            )
            .await
            .unwrap();
        // Guard 1: from-pointer mismatch ⇒ no-op.
        assert!(!store
            .displace_spend_for("potA", 0, "someoneElse", "settleA", Some(9), Some(true))
            .await
            .unwrap());
        // Happy: guarded displacement writes pointer/confirm/height/finality.
        assert!(store
            .displace_spend_for("potA", 0, "refundA", "settleA", Some(9), Some(true))
            .await
            .unwrap());
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("settleA"));
        assert!(r.spent_confirmed);
        assert_eq!(r.spent_height, Some(9));
        assert_eq!(r.spender_final, Some(true));
        assert_eq!(
            r.verdict.as_deref(),
            Some("refund"),
            "verdict columns untouched"
        );
        assert_eq!(
            r.verdict_txid.as_deref(),
            Some("refundA"),
            "stale verdict stays keyed to the OLD txid — the reader guard hides it"
        );
        // Guard 2: already-confirmed ⇒ no-op (never displace chain truth).
        assert!(!store
            .displace_spend_for("potA", 0, "settleA", "later", Some(10), None)
            .await
            .unwrap());
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("settleA"));
    }

    /// FINDING-5 PIN: the hint and binding legs draw from the SAME per-tick
    /// budget as every other courier read — at zero budget both REFUSE with a
    /// counted fault BEFORE any HTTP, preserving the pinned budget-0 ⇒
    /// no-courier-traffic property for the whole fetcher. (If the guard were
    /// deleted, the native run would surface a transport error instead — the
    /// "budget" text assert distinguishes the two.)
    #[tokio::test]
    async fn hint_and_binding_refuse_at_zero_budget_before_any_courier_traffic() {
        let fetcher = ChainProofFetcher::new(None).with_budget(0);
        let e = fetcher
            .resolve_spender(&"aa".repeat(32), 0)
            .await
            .unwrap_err();
        assert!(e.contains("budget"), "hint must refuse on budget, got: {e}");
        let e = fetcher
            .spender_binding_raw(&"bb".repeat(32), &"aa".repeat(32), 0)
            .await
            .unwrap_err();
        assert!(
            e.contains("budget"),
            "binding must refuse on budget, got: {e}"
        );
    }

    #[tokio::test]
    async fn spend_confirmation_leaves_unmined_untouched() {
        let store = MemoryPotStorage::new();
        store
            .store_record(&PotRecord {
                txid: "potA".into(),
                output_index: 0,
                spent: false,
                spending_txid: None,
                spent_confirmed: false,
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .mark_spent("potA", 0, "settleA", false, None, None, None)
            .await
            .unwrap();

        // The spending tx is NOT verifiably mined → fail-closed, no upgrade.
        let fetcher = MockProofFetcher {
            minable: std::collections::HashSet::new(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.scanned, 1);
        assert_eq!(s.confirmed, 0);
        assert_eq!(s.still_unconfirmed, 1);

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(!r.spent_confirmed, "an unverified spend is never latched");
        assert_eq!(
            store.find_spent_unconfirmed(10, 0).await.unwrap().len(),
            1,
            "the row stays a candidate for the next tick"
        );
    }

    #[tokio::test]
    async fn spend_confirmation_no_candidates_is_a_noop() {
        let store = MemoryPotStorage::new();
        let fetcher = MockProofFetcher {
            minable: std::collections::HashSet::new(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s, SpendConfirmSummary::default());
    }

    #[tokio::test]
    async fn spend_confirmation_only_upgrades_the_mined_row() {
        let store = MemoryPotStorage::new();
        for (txid, spender) in [("potA", "settleA"), ("potB", "settleB")] {
            store
                .store_record(&spent_unconfirmed(txid, spender))
                .await
                .unwrap();
        }
        // Only settleA is mined.
        let fetcher = MockProofFetcher {
            minable: ["settleA".to_string()].into_iter().collect(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.scanned, 2);
        assert_eq!(s.confirmed, 1);
        assert_eq!(s.still_unconfirmed, 1);

        assert!(
            store
                .get_spent_status("potA", 0)
                .await
                .unwrap()
                .unwrap()
                .spent_confirmed
        );
        assert!(
            !store
                .get_spent_status("potB", 0)
                .await
                .unwrap()
                .unwrap()
                .spent_confirmed
        );
    }

    // ── 5b. #301: the confirmed write is a guarded CAS ───────────────────────

    /// A fetcher that DISPLACES the row's spend pointer inside the proof-
    /// fetch await window — the exact #301 race shape (the chaser's
    /// candidate read and its confirmed write straddle this call) — then
    /// returns a verified proof for the ORIGINALLY-read spender.
    struct RacingFetcher {
        store: std::rc::Rc<MemoryPotStorage>,
        /// (txid, vout, new_spender, confirmed, height) applied on first call.
        displacement: (String, u32, String, bool, Option<u64>),
        fired: std::cell::Cell<bool>,
    }

    #[async_trait(?Send)]
    impl AncestorFetcher for RacingFetcher {
        async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
            Err(GASPError::NodeNotFound(format!(
                "mock: no ancestor for {txid}"
            )))
        }
        async fn verified_proof_for(&self, _txid: &str) -> Option<String> {
            if !self.fired.replace(true) {
                let (t, v, s, confirmed, h) = &self.displacement;
                self.store
                    .mark_spent(t, *v, s, *confirmed, None, *h, None)
                    .await
                    .unwrap();
            }
            Some("beefbump".to_string())
        }
    }

    /// #301 producer path, unconfirmed-displacement shape: the pointer moves
    /// to an UNCONFIRMED S2 while the chaser verifies S1's proof. The CAS
    /// misses (counted), the row keeps S2 unconfirmed — and the pass's
    /// NORMAL candidate selection re-surfaces it (no explicit re-chase hook
    /// needed: `find_spent_unconfirmed` matches it again).
    #[tokio::test]
    async fn spend_confirmation_cas_miss_leaves_the_displaced_row_a_candidate() {
        let store = std::rc::Rc::new(MemoryPotStorage::new());
        store
            .store_record(&spent_unconfirmed("potA", "settleS1"))
            .await
            .unwrap();

        let fetcher = RacingFetcher {
            store: store.clone(),
            displacement: ("potA".into(), 0, "settleS2".into(), false, None),
            fired: std::cell::Cell::new(false),
        };
        let s = complete_spend_confirmations(store.as_ref(), &fetcher, 20, 0).await;
        assert_eq!((s.scanned, s.confirmed, s.cas_missed), (1, 0, 1));

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(
            r.spending_txid.as_deref(),
            Some("settleS2"),
            "the stale S1 write never resets the displaced pointer"
        );
        assert!(!r.spent_confirmed, "nothing confirmed on a stale read");
        assert_eq!(
            store.find_spent_unconfirmed(10, 0).await.unwrap().len(),
            1,
            "the CAS-missed row is RE-VISITED by the normal candidate scan \
             (spent=1, spentConfirmed=0 still matches) — not stranded"
        );
    }

    /// #301 producer path, the REORG shape the issue was filed for: a
    /// CONFIRMED S2 lands while the chaser verifies S1's proof. Pre-#301
    /// the unguarded write reset the pointer to S1 and the row — now
    /// confirmed-with-a-stale-pointer — was invisible to every re-chase.
    /// Under the CAS: miss (counted), S2's confirmed pointer + height
    /// survive, and the row is terminal (confirmed by the competing
    /// verified writer — nothing left to re-chase).
    #[tokio::test]
    async fn spend_confirmation_cas_miss_never_resets_a_reorg_confirmed_pointer() {
        let store = std::rc::Rc::new(MemoryPotStorage::new());
        store
            .store_record(&spent_unconfirmed("potA", "settleS1"))
            .await
            .unwrap();

        let fetcher = RacingFetcher {
            store: store.clone(),
            displacement: ("potA".into(), 0, "settleS2".into(), true, Some(802_000)),
            fired: std::cell::Cell::new(false),
        };
        let s = complete_spend_confirmations(store.as_ref(), &fetcher, 20, 0).await;
        assert_eq!((s.scanned, s.confirmed, s.cas_missed), (1, 0, 1));

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some("settleS2"));
        assert!(r.spent_confirmed, "the reorg-confirmed pointer SURVIVES");
        assert_eq!(r.spent_height, Some(802_000), "S2 keeps its own height");
        assert!(
            store
                .find_spent_unconfirmed(10, 0)
                .await
                .unwrap()
                .is_empty(),
            "terminal: confirmed by the competing writer, nothing to re-chase"
        );
    }

    // ── 6. push-primary /arc-ingest consumer + poll backstop (#228) ──────────

    /// Two distinct valid mainnet raw txs (same fixtures as the pot storage
    /// tests) — used to build REAL BEEFs for the stitch/compact path.
    const RAW_A: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

    /// A proofless single-tx BEEF for RAW_A + its txid.
    fn proofless_pot_beef() -> (Vec<u8>, String) {
        use bsv_rs::transaction::{Beef, Transaction};
        let tx = Transaction::from_hex(RAW_A).unwrap();
        let txid = tx.id();
        let mut beef = Beef::new();
        beef.merge_transaction(tx);
        (beef.to_binary(), txid)
    }

    /// A STRUCTURALLY-bumped pot BEEF exactly as an ungated admit leaves it
    /// (bytes carry a bump nobody verified). `lock_time_salt` varies the
    /// txid so tests can seed many distinct rows from one raw fixture.
    fn structurally_bumped_pot_beef(bump_height: u32, lock_time_salt: u32) -> (Vec<u8>, String) {
        use bsv_rs::transaction::Transaction;
        // Salt the lock_time in the RAW BYTES (its trailing 4 LE bytes) and
        // parse fresh: `Transaction` caches raw bytes + hash at parse time,
        // so a post-parse field mutation would keep the stale txid.
        let mut raw = hex::decode(RAW_A).unwrap();
        let n = raw.len();
        raw[n - 4..].copy_from_slice(&lock_time_salt.to_le_bytes());
        let mut tx = Transaction::from_binary(&raw).unwrap();
        let txid = tx.id();
        tx.merkle_path = Some(single_tx_bump(&txid, bump_height));
        (tx.to_beef(true).unwrap(), txid)
    }

    /// The subject's own bump height inside a stored BEEF (None = proofless).
    fn stored_bump_height(beef: &[u8], txid: &str) -> Option<u32> {
        let b = bsv_rs::transaction::Beef::from_binary(beef).ok()?;
        let idx = b.find_txid(txid)?.bump_index()?;
        Some(b.bumps.get(idx)?.block_height)
    }

    /// bsv-low#304 fast-path test fetcher: configurable stored-bump verdict
    /// + refetch table, with call counters so tests can assert "no courier
    ///   fetch happened" through the REAL pass (never hand-fed candidates).
    struct ReverifyFetcher {
        verify_ok: bool,
        refetch: std::collections::HashMap<String, String>,
        verify_calls: std::cell::Cell<usize>,
        fetch_calls: std::cell::Cell<usize>,
    }

    impl ReverifyFetcher {
        fn new(verify_ok: bool) -> Self {
            Self {
                verify_ok,
                refetch: std::collections::HashMap::new(),
                verify_calls: std::cell::Cell::new(0),
                fetch_calls: std::cell::Cell::new(0),
            }
        }
    }

    #[async_trait(?Send)]
    impl AncestorFetcher for ReverifyFetcher {
        async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
            Err(GASPError::NodeNotFound(format!(
                "mock: no ancestor for {txid}"
            )))
        }
        async fn verified_proof_for(&self, txid: &str) -> Option<String> {
            self.fetch_calls.set(self.fetch_calls.get() + 1);
            self.refetch.get(txid).cloned()
        }
        async fn verify_proof(&self, _txid: &str, _bump_hex: &str) -> bool {
            self.verify_calls.set(self.verify_calls.get() + 1);
            self.verify_ok
        }
    }

    // ── bsv-low#304: stored-bump re-verify fast path (poll pass) ─────────

    #[tokio::test]
    async fn stored_bump_reverify_fast_path_latches_without_fetch_or_rewrite() {
        let store = MemoryPotStorage::new();
        let (beef, txid) = structurally_bumped_pot_beef(HEIGHT, 0);
        store.store_beef(&txid, &beef).await.unwrap();

        let fetcher = ReverifyFetcher::new(true);
        let pass = complete_pot_beef_proofs(&store, &fetcher, 20, 0).await;
        assert_eq!(pass.already_proven, 1, "verified stored bump latches");
        assert_eq!(pass.completed, 0);
        assert_eq!(pass.still_unconfirmed, 0);
        assert_eq!(fetcher.verify_calls.get(), 1);
        assert_eq!(
            fetcher.fetch_calls.get(),
            0,
            "the fast path never touches the courier"
        );

        // Latched (out of the candidate set), bytes untouched.
        assert!(store
            .find_pot_beefs_for_proof_check(10, 0)
            .await
            .unwrap()
            .is_empty());
        assert!(store.pot_beef_proof_verified(&txid).await.unwrap());
        assert_eq!(
            store.get_beef(&txid).await.unwrap().unwrap(),
            beef,
            "no byte rewrite"
        );
    }

    #[tokio::test]
    async fn stored_bump_failing_reverify_is_not_latched_and_falls_to_refetch() {
        // bsv-low#304 gate LOW-3: the fail branch runs NATIVELY (its log is
        // push_log — a console_log here aborts off-wasm) and behaves
        // fail-closed: chaintracks says no → NOT latched, the courier
        // refetch path is taken, and with nothing refetchable the row stays
        // an honest candidate.
        let store = MemoryPotStorage::new();
        let (beef, txid) = structurally_bumped_pot_beef(HEIGHT, 0);
        store.store_beef(&txid, &beef).await.unwrap();

        let fetcher = ReverifyFetcher::new(false);
        let pass = complete_pot_beef_proofs(&store, &fetcher, 20, 0).await;
        assert_eq!(pass.already_proven, 0, "a FAILED re-verify must not latch");
        assert_eq!(pass.still_unconfirmed, 1);
        assert_eq!(fetcher.verify_calls.get(), 1);
        assert_eq!(
            fetcher.fetch_calls.get(),
            1,
            "falls through to the refetch path"
        );
        assert!(
            !store.pot_beef_proof_verified(&txid).await.unwrap(),
            "fake bump stays unverified"
        );
        assert_eq!(
            store
                .find_pot_beefs_for_proof_check(10, 0)
                .await
                .unwrap()
                .len(),
            1,
            "still a candidate — retried next tick"
        );
    }

    #[tokio::test]
    async fn failed_reverify_with_a_refetched_proof_replaces_the_fake_bump() {
        let store = MemoryPotStorage::new();
        let (beef, txid) = structurally_bumped_pot_beef(HEIGHT, 0);
        store.store_beef(&txid, &beef).await.unwrap();

        let mut fetcher = ReverifyFetcher::new(false);
        fetcher
            .refetch
            .insert(txid.clone(), single_tx_bump(&txid, HEIGHT + 1).to_hex());
        let pass = complete_pot_beef_proofs(&store, &fetcher, 20, 0).await;
        assert_eq!(
            pass.completed, 1,
            "the verified refetched proof stitches in"
        );
        assert_eq!(pass.already_proven, 0);

        let stored = store.get_beef(&txid).await.unwrap().unwrap();
        assert_eq!(
            stored_bump_height(&stored, &txid),
            Some(HEIGHT + 1),
            "the fake bump was REPLACED by the chaintracks-verified one"
        );
        assert!(store.pot_beef_proof_verified(&txid).await.unwrap());
        assert!(store
            .find_pot_beefs_for_proof_check(10, 0)
            .await
            .unwrap()
            .is_empty());
    }

    // ── bsv-low#304 gate M-5: order + starvation visibility ──────────────

    /// A fetcher pair model of the invocation's SUBREQUEST WALL: a shared
    /// allowance every chaintracks-ish op consumes; at 0 further reads
    /// FAULT (detailed) / fail closed (bool), exactly like a starved tick.
    struct StarvingFetcher {
        allowance: std::rc::Rc<std::cell::Cell<u32>>,
        bump_for: std::collections::HashMap<String, String>,
    }

    #[async_trait(?Send)]
    impl AncestorFetcher for StarvingFetcher {
        async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
            Err(GASPError::NodeNotFound(format!(
                "mock: no ancestor for {txid}"
            )))
        }
        async fn verified_proof_for(&self, txid: &str) -> Option<String> {
            self.verified_proof_for_detailed(txid).await.unwrap_or(None)
        }
        async fn verified_proof_for_detailed(&self, txid: &str) -> Result<Option<String>, String> {
            if self.allowance.get() == 0 {
                return Err("chaintracks read starved at the subrequest wall".into());
            }
            self.allowance.set(self.allowance.get() - 1);
            Ok(self.bump_for.get(txid).cloned())
        }
        async fn verify_proof(&self, _txid: &str, _bump_hex: &str) -> bool {
            if self.allowance.get() == 0 {
                return false; // starved read fail-closes (the silent shape M-5 surfaces)
            }
            self.allowance.set(self.allowance.get() - 1);
            true
        }
    }

    #[tokio::test]
    async fn credit_anchor_chaser_runs_before_the_bulk_drain() {
        // bsv-low#304 gate M-5(a): with an op allowance of EXACTLY ONE, the
        // shipped order must spend it on the #186 spend-confirmation chaser
        // (the credit anchor) — the pot-beef bulk drain starves instead.
        // RED-form: swap the order inside run_pot_maintenance and the drain
        // eats the allowance, the anchor faults, and `confirmed` stays 0.
        let store = MemoryPotStorage::new();
        store
            .store_record(&spent_unconfirmed("potA", SETTLE_A))
            .await
            .unwrap();
        for salt in 0..5u32 {
            let (beef, txid) = structurally_bumped_pot_beef(HEIGHT, salt);
            store.store_beef(&txid, &beef).await.unwrap();
        }

        let allowance = std::rc::Rc::new(std::cell::Cell::new(1u32));
        let spend_fetcher = StarvingFetcher {
            allowance: allowance.clone(),
            bump_for: std::collections::HashMap::from([(
                SETTLE_A.to_string(),
                single_tx_bump(SETTLE_A, HEIGHT).to_hex(),
            )]),
        };
        let pot_fetcher = StarvingFetcher {
            allowance: allowance.clone(),
            bump_for: std::collections::HashMap::new(),
        };

        let (spend, pot) = run_pot_maintenance(
            &store,
            &spend_fetcher,
            20,
            &pot_fetcher,
            POT_PROOF_PASS_LIMIT,
            0,
        )
        .await;

        assert_eq!(spend.confirmed, 1, "the credit anchor ran FIRST and landed");
        assert_eq!(spend.tracker_faults, 0);
        assert!(
            store
                .get_spent_status("potA", 0)
                .await
                .unwrap()
                .unwrap()
                .spent_confirmed,
            "spentConfirmed latched by the anchor"
        );
        // The bulk drain hit the wall AFTER the anchor: nothing latched,
        // every row honestly still a candidate.
        assert_eq!(pot.already_proven, 0);
        assert_eq!(pot.still_unconfirmed, 5);
        assert_eq!(
            store
                .find_pot_beefs_for_proof_check(100, 0)
                .await
                .unwrap()
                .len(),
            5,
            "starved drain latches nothing (fail-safe, retried next tick)"
        );
    }

    #[tokio::test]
    async fn a_starved_chaser_read_is_a_counted_tracker_fault_not_a_chain_verdict() {
        // bsv-low#304 gate M-5(b): a chaintracks/proof READ FAULT is counted
        // under tracker_faults — DISTINGUISHABLE from still_unconfirmed
        // ("chain says not yet"), so a subrequest-wall starvation is visible.
        let store = MemoryPotStorage::new();
        store
            .store_record(&spent_unconfirmed("potA", SETTLE_A))
            .await
            .unwrap();
        let starved = StarvingFetcher {
            allowance: std::rc::Rc::new(std::cell::Cell::new(0)),
            bump_for: std::collections::HashMap::new(),
        };
        let s = complete_spend_confirmations(&store, &starved, 20, 0).await;
        assert_eq!(s.tracker_faults, 1, "the fault is counted separately");
        assert_eq!(
            s.still_unconfirmed, 0,
            "a read fault is NOT 'not mined yet'"
        );
        assert_eq!(s.confirmed, 0);
        assert!(
            !store
                .get_spent_status("potA", 0)
                .await
                .unwrap()
                .unwrap()
                .spent_confirmed,
            "nothing latched on a fault (fail-closed)"
        );
    }

    // ── bsv-low#304 gate LOW-3 residual: the REAL fetcher at budget 0 ─────

    #[tokio::test]
    async fn bulk_reverify_is_chaintracks_only_with_the_real_fetcher() {
        // The admin drain's exact configuration: the PRODUCTION
        // ChainProofFetcher with budget 0 — the fast path chaintracks-verifies
        // stored bumps (latching genuine ones), and the courier path REFUSES
        // at zero budget (push_log, natively safe — the residual swap this
        // test exists to pin). A MockChainTracker vouches for the bumped
        // row's root; the proofless row can only go the (refused) courier way.
        let store = MemoryPotStorage::new();
        let (bumped, txid_bumped) = structurally_bumped_pot_beef(HEIGHT, 1);
        store.store_beef(&txid_bumped, &bumped).await.unwrap();
        let (proofless, txid_proofless) = proofless_pot_beef();
        assert_ne!(txid_bumped, txid_proofless);
        store.store_beef(&txid_proofless, &proofless).await.unwrap();

        let mut tracker = MockChainTracker::new(HEIGHT + 6);
        tracker.add_root(HEIGHT, txid_bumped.clone());
        let fetcher = ChainProofFetcher::new(Some(std::rc::Rc::new(tracker))).with_budget(0);

        let pass = complete_pot_beef_proofs(&store, &fetcher, ADMIN_REVERIFY_MAX_LIMIT, 0).await;
        assert_eq!(pass.scanned, 2);
        assert_eq!(
            pass.already_proven, 1,
            "real chaintracks re-verify latches the bumped row"
        );
        assert_eq!(
            pass.still_unconfirmed, 1,
            "the proofless row is REFUSED at zero courier budget — chaintracks-only"
        );
        assert!(store.pot_beef_proof_verified(&txid_bumped).await.unwrap());
        assert!(!store
            .pot_beef_proof_verified(&txid_proofless)
            .await
            .unwrap());
    }

    // ── bsv-low#304 gate M-2: the backlog drains in one wide pass ────────

    #[tokio::test]
    #[allow(
        clippy::assertions_on_constants,
        reason = "deliberate drift pin: the drain-math floor on the shipped page const"
    )]
    async fn one_pass_drains_a_backlog_wider_than_the_old_page() {
        // The shipped cron page must clear a whole seeded backlog in ONE
        // pass — the pre-M-2 page of 20 strands rows 21+ for later ticks,
        // keeping /tx-any's WoC-first external leg warm (the 429 doctrine).
        assert!(
            POT_PROOF_PASS_LIMIT >= 96,
            "drain math: at {POT_PROOF_PASS_LIMIT}/tick x 96 ticks/day a ~3,000-row \
             backlog must clear in hours, not weeks — do not shrink this below ~96 \
             without redoing the bsv-low#304 M-2 math"
        );

        let store = MemoryPotStorage::new();
        let mut txids = Vec::new();
        for salt in 0..25u32 {
            let (beef, txid) = structurally_bumped_pot_beef(HEIGHT, salt);
            store.store_beef(&txid, &beef).await.unwrap();
            txids.push(txid);
        }
        assert_eq!(
            store
                .find_pot_beefs_for_proof_check(1000, 0)
                .await
                .unwrap()
                .len(),
            25
        );

        let fetcher = ReverifyFetcher::new(true);
        let pass = complete_pot_beef_proofs(&store, &fetcher, POT_PROOF_PASS_LIMIT, 0).await;
        assert_eq!(
            pass.already_proven, 25,
            "one shipped-page pass drains the whole seeded backlog"
        );
        assert_eq!(
            fetcher.fetch_calls.get(),
            0,
            "chaintracks-only — zero courier fetches"
        );
        assert!(store
            .find_pot_beefs_for_proof_check(1000, 0)
            .await
            .unwrap()
            .is_empty());
    }

    /// 64-hex settle txids (a bump subject must be a real txid shape).
    const SETTLE_A: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SETTLE_B: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    // ── #284 decoded-params lazy backfill ────────────────────────────────

    use overlay_discovery::pot::{encode_covenant_param_pushes, CovenantParams, POC5_TEMPLATE_HEX};

    fn backfill_params() -> CovenantParams {
        CovenantParams {
            pub_a: [0x02; 33],
            pub_b: [0x03; 33],
            pub_tower: [0x04; 33],
            pay_pkh_a: [0xAA; 20],
            pay_pkh_b: [0xBB; 20],
            rake_pkh: [0xCC; 20],
            stake_a: 1250,
            stake_b: 1250,
            fee_sats: 100,
            recovery_height: 900_000,
        }
    }

    /// A byte-faithful covenant lock: frozen HEAD ‖ encoded params ‖ TAIL.
    fn covenant_lock(p: &CovenantParams) -> Vec<u8> {
        let t = POC5_TEMPLATE_HEX;
        let mut s = hex::decode(&t[..t.find('<').unwrap()]).unwrap();
        s.extend(encode_covenant_param_pushes(p));
        s.extend(hex::decode(&t[t.rfind('>').unwrap() + 1..]).unwrap());
        s
    }

    fn p2pkh_script(pkh: &[u8; 20]) -> Vec<u8> {
        let mut s = vec![0x76, 0xa9, 0x14];
        s.extend_from_slice(pkh);
        s.extend_from_slice(&[0x88, 0xac]);
        s
    }

    /// A tx paying the given `(sats, lock)` outputs from a salt-derived
    /// dummy input; returns `(beef_bytes, txid)`.
    fn tx_beef(salt: u8, outs: &[(u64, Vec<u8>)]) -> (Vec<u8>, String) {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Beef, TransactionInput, TransactionOutput};
        let mut tx = Transaction::new();
        tx.add_input(TransactionInput::new(hex::encode([salt; 32]), 0))
            .unwrap();
        for (sats, lock) in outs {
            tx.add_output(TransactionOutput {
                satoshis: Some(*sats),
                locking_script: LockingScript::from_binary(lock).unwrap(),
                change: false,
            })
            .unwrap();
        }
        let txid = tx.id();
        let mut beef = Beef::new();
        beef.merge_transaction(tx);
        (beef.to_binary(), txid)
    }

    /// A spender of `pot_txid:0` paying `outs`; returns `(beef, txid)`.
    fn spender_beef(pot_txid: &str, outs: &[(u64, Vec<u8>)]) -> (Vec<u8>, String) {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Beef, TransactionInput, TransactionOutput};
        let mut tx = Transaction::new();
        tx.add_input(TransactionInput::new(pot_txid.to_string(), 0))
            .unwrap();
        for (sats, lock) in outs {
            tx.add_output(TransactionOutput {
                satoshis: Some(*sats),
                locking_script: LockingScript::from_binary(lock).unwrap(),
                change: false,
            })
            .unwrap();
        }
        let txid = tx.id();
        let mut beef = Beef::new();
        beef.merge_transaction(tx);
        (beef.to_binary(), txid)
    }

    /// An undecoded (pre-#284) pot_records row for `(txid, 0)`.
    fn undecoded_row(txid: &str) -> PotRecord {
        PotRecord {
            txid: txid.into(),
            output_index: 0,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn backfill_decodes_covenant_bare_p2pkh_and_unknown_shapes() {
        let store = MemoryPotStorage::new();
        let p = backfill_params();
        // Covenant pot (funded value == stakeA+stakeB).
        let (cov_beef, cov_txid) = tx_beef(1, &[(2500, covenant_lock(&p))]);
        // Bare 2-of-3 pot.
        let mut bare = vec![0x52];
        for seed in [0x02u8, 0x03, 0x04] {
            bare.push(33);
            bare.extend_from_slice(&[seed; 33]);
        }
        bare.push(0x53);
        bare.push(0xae);
        let (bare_beef, bare_txid) = tx_beef(2, &[(4000, bare)]);
        // A plain hop P2PKH.
        let (hop_beef, hop_txid) = tx_beef(3, &[(546, p2pkh_script(&[0xDD; 20]))]);
        // An unrecognized lock shape.
        let (odd_beef, odd_txid) = tx_beef(4, &[(1000, vec![0x6a, 0x01, 0xff])]);

        for (txid, beef) in [
            (&cov_txid, &cov_beef),
            (&bare_txid, &bare_beef),
            (&hop_txid, &hop_beef),
            (&odd_txid, &odd_beef),
        ] {
            store.store_record(&undecoded_row(txid)).await.unwrap();
            store.store_beef(txid, beef).await.unwrap();
        }

        let s = backfill_decoded_params(&store, 20).await;
        assert_eq!(s.scanned, 4);
        assert_eq!(s.decoded, 4);
        assert_eq!(s.missing_beef, 0);

        let cov = store.get_spent_status(&cov_txid, 0).await.unwrap().unwrap();
        assert_eq!(cov.lock_kind.as_deref(), Some("covenant"));
        assert_eq!(cov.decoded_covenant_params(), Some(p));
        assert_eq!(cov.pot_sats, Some(2500));
        let bare = store
            .get_spent_status(&bare_txid, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bare.lock_kind.as_deref(), Some("bare"));
        assert!(bare.params_decoded);
        let hop = store.get_spent_status(&hop_txid, 0).await.unwrap().unwrap();
        assert_eq!(hop.lock_kind.as_deref(), Some("p2pkh"));
        let odd = store.get_spent_status(&odd_txid, 0).await.unwrap().unwrap();
        assert_eq!(odd.lock_kind, None, "unrecognized shape stays kind-less…");
        assert!(odd.params_decoded, "…but the decode attempt is recorded");

        // TERMINATION: every row decoded — the next tick scans nothing.
        let s2 = backfill_decoded_params(&store, 20).await;
        assert_eq!(s2.scanned, 0, "decoded rows are never rescanned");
    }

    #[tokio::test]
    async fn backfill_missing_beef_stays_a_candidate() {
        let store = MemoryPotStorage::new();
        store
            .store_record(&undecoded_row("potNoBeef"))
            .await
            .unwrap();
        let s = backfill_decoded_params(&store, 20).await;
        assert_eq!((s.scanned, s.decoded, s.missing_beef), (1, 0, 1));
        // Still a candidate next tick (retry forever, bounded per tick).
        let s2 = backfill_decoded_params(&store, 20).await;
        assert_eq!(s2.scanned, 1, "a missing-BEEF row is retried");
        let r = store
            .get_spent_status("potNoBeef", 0)
            .await
            .unwrap()
            .unwrap();
        assert!(!r.params_decoded);
    }

    #[tokio::test]
    async fn backfill_computes_the_verdict_for_a_spent_covenant_row() {
        let store = MemoryPotStorage::new();
        let p = backfill_params();
        let (cov_beef, cov_txid) = tx_beef(1, &[(2500, covenant_lock(&p))]);
        // The exact winner-B template: pot 2500, fee 100 → net 2400, rake 25.
        let (settle_bytes, settle_txid) = spender_beef(
            &cov_txid,
            &[
                (25, p2pkh_script(&p.rake_pkh)),
                (2375, p2pkh_script(&p.pay_pkh_b)),
            ],
        );

        // A pre-#284 row that is already SPENT (unconfirmed pointer), with
        // both BEEFs durably stored.
        store.store_record(&undecoded_row(&cov_txid)).await.unwrap();
        store
            .mark_spent(&cov_txid, 0, &settle_txid, false, None, None, None)
            .await
            .unwrap();
        store.store_beef(&cov_txid, &cov_beef).await.unwrap();
        store.store_beef(&settle_txid, &settle_bytes).await.unwrap();

        let s = backfill_decoded_params(&store, 20).await;
        assert_eq!((s.scanned, s.decoded, s.verdicts), (1, 1, 1));
        let r = store.get_spent_status(&cov_txid, 0).await.unwrap().unwrap();
        assert_eq!(r.verdict.as_deref(), Some("winner-b"));
        assert_eq!(r.verdict_txid.as_deref(), Some(settle_txid.as_str()));
        assert_eq!(r.spent_height, None, "this pass verifies no height");
        // #406: this synthetic spender carries no signatures, and the pass
        // held its durable bytes — so the signer question is CONCLUDED, not
        // deferred: 'unresolved' rides the same verdict write.
        assert_eq!(r.settle_signers.as_deref(), Some("unresolved"));

        // A spent BARE row never gets a verdict from the backfill.
        let mut bare = vec![0x52];
        for seed in [0x02u8, 0x03, 0x04] {
            bare.push(33);
            bare.extend_from_slice(&[seed; 33]);
        }
        bare.push(0x53);
        bare.push(0xae);
        let (bare_beef, bare_txid) = tx_beef(2, &[(4000, bare)]);
        let (bspend, bspend_txid) = spender_beef(
            &bare_txid,
            &[
                (1800, p2pkh_script(&[0xAA; 20])),
                (1800, p2pkh_script(&[0xBB; 20])),
            ],
        );
        store
            .store_record(&undecoded_row(&bare_txid))
            .await
            .unwrap();
        store
            .mark_spent(&bare_txid, 0, &bspend_txid, false, None, None, None)
            .await
            .unwrap();
        store.store_beef(&bare_txid, &bare_beef).await.unwrap();
        store.store_beef(&bspend_txid, &bspend).await.unwrap();
        let s = backfill_decoded_params(&store, 20).await;
        assert_eq!(s.verdicts, 0, "bare pots NEVER get a stored verdict");
        let r = store
            .get_spent_status(&bare_txid, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.lock_kind.as_deref(), Some("bare"));
        assert_eq!(r.verdict, None);
    }

    // ── bsv-low #406: the settleSigners HISTORIC backfill ────────────────────

    /// A decoded covenant row carrying a pre-#406 verdict (no signers) —
    /// exactly what the old writer left behind.
    fn pre406_row(txid: &str, p: &CovenantParams, pot_sats: u64) -> PotRecord {
        PotRecord {
            txid: txid.into(),
            output_index: 0,
            params_decoded: true,
            lock_kind: Some("covenant".into()),
            pub_a: Some(hex::encode(p.pub_a)),
            pub_b: Some(hex::encode(p.pub_b)),
            pub_tower: Some(hex::encode(p.pub_tower)),
            pay_pkh_a: Some(hex::encode(p.pay_pkh_a)),
            pay_pkh_b: Some(hex::encode(p.pay_pkh_b)),
            rake_pkh: Some(hex::encode(p.rake_pkh)),
            stake_a: Some(p.stake_a),
            stake_b: Some(p.stake_b),
            fee_sats: Some(p.fee_sats),
            recovery_height: Some(p.recovery_height),
            pot_sats: Some(pot_sats),
            ..Default::default()
        }
    }

    /// Params whose key triple is REAL (deterministic scalars 1/2/3), so a
    /// signed spend can actually verify.
    fn real_key_params() -> ([bsv_rs::primitives::ec::PrivateKey; 3], CovenantParams) {
        let key = |scalar: u8| {
            let mut b = [0u8; 32];
            b[31] = scalar;
            bsv_rs::primitives::ec::PrivateKey::from_bytes(&b).unwrap()
        };
        let keys = [key(1), key(2), key(3)];
        let p = CovenantParams {
            pub_a: keys[0].public_key().to_compressed(),
            pub_b: keys[1].public_key().to_compressed(),
            pub_tower: keys[2].public_key().to_compressed(),
            pay_pkh_a: [0xAA; 20],
            pay_pkh_b: [0xBB; 20],
            rake_pkh: [0xCC; 20],
            stake_a: 1250,
            stake_b: 1250,
            fee_sats: 100,
            recovery_height: 900_000,
        };
        (keys, p)
    }

    /// A spender of `pot_txid:0` SIGNED by `signers` against the pot's real
    /// covenant lock digest (SIGHASH_ALL|FORKID) — the shape the network
    /// actually validated. Returns `(beef, txid)`.
    fn signed_spender_beef(
        pot_txid: &str,
        p: &CovenantParams,
        pot_sats: u64,
        signers: &[&bsv_rs::primitives::ec::PrivateKey],
        outs: &[(u64, Vec<u8>)],
    ) -> (Vec<u8>, String) {
        use bsv_rs::primitives::bsv::sighash::{
            compute_sighash_for_signing, parse_transaction, SighashParams, SIGHASH_ALL,
            SIGHASH_FORKID,
        };
        use bsv_rs::script::{LockingScript, UnlockingScript};
        use bsv_rs::transaction::{Beef, TransactionInput, TransactionOutput};
        let lock = covenant_lock(p);
        // Build twice rather than mutate: `Transaction::to_binary` CACHES its
        // serialization, so an unlock spliced in after a digest read would be
        // silently dropped from every later serialization (found the hard
        // way — the beef carried the unsigned skeleton).
        let build = |unlock: Option<&[u8]>| {
            let mut tx = Transaction::new();
            let mut input = TransactionInput::new(pot_txid.to_string(), 0);
            if let Some(u) = unlock {
                input.unlocking_script = Some(UnlockingScript::from_binary(u).unwrap());
            }
            tx.add_input(input).unwrap();
            for (sats, out_lock) in outs {
                tx.add_output(TransactionOutput {
                    satoshis: Some(*sats),
                    locking_script: LockingScript::from_binary(out_lock).unwrap(),
                    change: false,
                })
                .unwrap();
            }
            tx
        };
        // The digest does not commit the input script (BIP-143), so sign over
        // the skeleton's bytes.
        let raw = build(None).to_binary();
        let parsed = parse_transaction(&raw).unwrap();
        let digest = compute_sighash_for_signing(&SighashParams {
            version: parsed.version,
            inputs: &parsed.inputs,
            outputs: &parsed.outputs,
            locktime: parsed.locktime,
            input_index: 0,
            subscript: &lock,
            satoshis: pot_sats,
            scope: SIGHASH_ALL | SIGHASH_FORKID,
        });
        let mut unlock = vec![0x00]; // CHECKMULTISIG null dummy
        for sk in signers {
            let mut der = sk.sign(&digest).unwrap().to_der();
            der.push((SIGHASH_ALL | SIGHASH_FORKID) as u8);
            unlock.push(der.len() as u8);
            unlock.extend_from_slice(&der);
        }
        let tx = build(Some(&unlock));
        let txid = tx.id();
        let mut beef = Beef::new();
        beef.merge_transaction(tx);
        (beef.to_binary(), txid)
    }

    #[tokio::test]
    async fn signers_backfill_latches_who_signed_from_the_durable_bytes() {
        let store = MemoryPotStorage::new();
        let (keys, p) = real_key_params();
        let pot_txid = hex::encode([0x11u8; 32]);
        // Signed by seat B + the TOWER — the enforced family.
        let (settle_bytes, settle_txid) = signed_spender_beef(
            &pot_txid,
            &p,
            2500,
            &[&keys[1], &keys[2]],
            &[(2375, p2pkh_script(&p.pay_pkh_b))],
        );
        store
            .store_record(&pre406_row(&pot_txid, &p, 2500))
            .await
            .unwrap();
        store
            .mark_spent(
                &pot_txid,
                0,
                &settle_txid,
                false,
                Some(VerdictWrite::bare("winner-b")), // the pre-#406 writer's shape
                None,
                None,
            )
            .await
            .unwrap();
        store.store_beef(&settle_txid, &settle_bytes).await.unwrap();

        let s = backfill_settle_signers(&store, 20).await;
        assert_eq!(
            (s.scanned, s.latched, s.unresolved, s.missing_beef),
            (1, 1, 0, 0)
        );
        let r = store.get_spent_status(&pot_txid, 0).await.unwrap().unwrap();
        assert_eq!(r.settle_signers.as_deref(), Some("tower-b"));
        // The stored verdict is preserved (the outputs match no template, so
        // the re-derivation came up empty and the pass ECHOED the stored
        // string — a pure signers-attach).
        assert_eq!(r.verdict.as_deref(), Some("winner-b"));
        assert_eq!(r.verdict_txid.as_deref(), Some(settle_txid.as_str()));

        // TERMINATION: latched rows leave the candidate set.
        let s2 = backfill_settle_signers(&store, 20).await;
        assert_eq!(s2.scanned, 0);
    }

    #[tokio::test]
    async fn signers_backfill_missing_spender_beef_stays_a_candidate() {
        let store = MemoryPotStorage::new();
        let (_, p) = real_key_params();
        let pot_txid = hex::encode([0x22u8; 32]);
        store
            .store_record(&pre406_row(&pot_txid, &p, 2500))
            .await
            .unwrap();
        store
            .mark_spent(
                &pot_txid,
                0,
                "deadbeef",
                false,
                Some(VerdictWrite::bare("winner-a")),
                None,
                None,
            )
            .await
            .unwrap();
        let s = backfill_settle_signers(&store, 20).await;
        assert_eq!((s.scanned, s.latched, s.missing_beef), (1, 0, 1));
        // Retried next tick — the spender bytes can still arrive.
        let s2 = backfill_settle_signers(&store, 20).await;
        assert_eq!(s2.scanned, 1);
        let r = store.get_spent_status(&pot_txid, 0).await.unwrap().unwrap();
        assert_eq!(r.settle_signers, None, "nothing concluded without bytes");
    }

    #[tokio::test]
    async fn signers_backfill_concludes_unresolved_for_an_unsigned_spender() {
        let store = MemoryPotStorage::new();
        let (_, p) = real_key_params();
        let pot_txid = hex::encode([0x33u8; 32]);
        // Durable spender bytes with NO signatures at all.
        let (settle_bytes, settle_txid) =
            spender_beef(&pot_txid, &[(2375, p2pkh_script(&p.pay_pkh_a))]);
        store
            .store_record(&pre406_row(&pot_txid, &p, 2500))
            .await
            .unwrap();
        store
            .mark_spent(
                &pot_txid,
                0,
                &settle_txid,
                false,
                Some(VerdictWrite::bare("winner-a")),
                None,
                None,
            )
            .await
            .unwrap();
        store.store_beef(&settle_txid, &settle_bytes).await.unwrap();
        let s = backfill_settle_signers(&store, 20).await;
        assert_eq!((s.scanned, s.latched, s.unresolved), (1, 0, 1));
        let r = store.get_spent_status(&pot_txid, 0).await.unwrap().unwrap();
        assert_eq!(r.settle_signers.as_deref(), Some("unresolved"));
        // TERMINATION: 'unresolved' leaves the candidate set — no tick-loop.
        let s2 = backfill_settle_signers(&store, 20).await;
        assert_eq!(s2.scanned, 0);
    }

    #[tokio::test]
    async fn pushed_proof_confirms_spends_and_the_chaser_skips_them() {
        // pushed-proof-then-chaser-skips: /arc-ingest receives (and verifies)
        // the settle's bump → apply_pushed_proof_to_pot_stores latches every
        // pot outpoint that settle spent → the #186 poll chaser finds ZERO
        // candidates and never asks a courier — through the real producers
        // (mark_spent → find_unconfirmed_by_spending_txid → mark_spent
        // confirmed → find_spent_unconfirmed → complete_spend_confirmations).
        let store = MemoryPotStorage::new();
        for pot in ["potA", "potB"] {
            store
                .store_record(&spent_unconfirmed(pot, SETTLE_A))
                .await
                .unwrap();
        }
        // A third pot spent by a DIFFERENT settle stays untouched.
        store
            .store_record(&spent_unconfirmed("potC", SETTLE_B))
            .await
            .unwrap();

        let bump_hex = single_tx_bump(SETTLE_A, HEIGHT).to_hex();
        let s = apply_pushed_proof_to_pot_stores(&store, SETTLE_A, &bump_hex).await;
        assert_eq!(
            s.spends_confirmed, 2,
            "both outpoints the settle spent are latched"
        );

        for pot in ["potA", "potB"] {
            assert!(
                store
                    .get_spent_status(pot, 0)
                    .await
                    .unwrap()
                    .unwrap()
                    .spent_confirmed
            );
        }
        assert!(
            !store
                .get_spent_status("potC", 0)
                .await
                .unwrap()
                .unwrap()
                .spent_confirmed
        );

        // The chaser (min_age 0 = widest possible candidate set) now sees only
        // potC — and with an unminable fetcher it upgrades nothing.
        let fetcher = MockProofFetcher {
            minable: std::collections::HashSet::new(),
            ..Default::default()
        };
        let chase = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(chase.scanned, 1, "pushed-latched rows are skipped entirely");
        assert_eq!(chase.sample, vec![SETTLE_B.to_string()]);
        assert_eq!(chase.confirmed, 0);
    }

    // ── bsv-low handoff #2b: proof-poll retirement through the REAL passes ──

    /// The #186 chaser's confirm is a retirement moment: once the settle's
    /// proof chaintracks-verifies, the superseded pre-signed refund —
    /// stored as poll-pool bytes when it was submitted as a spend — is
    /// latched structurally unprovable and leaves the pot-beef poll pool,
    /// through the REAL producers (mark_spent → find_spent_unconfirmed →
    /// complete_spend_confirmations → mark_confirmed_for_spender →
    /// retirement → find_pot_beefs_for_proof_check). Before the confirm,
    /// NOTHING is latched (the no-unconfirmed-latch pin, pass-level).
    #[tokio::test]
    async fn chaser_confirm_retires_superseded_refund_from_the_poll_pool() {
        let store = MemoryPotStorage::new();
        let pot = "ee".repeat(32);
        store.store_record(&undecoded_row(&pot)).await.unwrap();
        let (settle_beef, settle_txid) = spender_beef(&pot, &[(1200, p2pkh_script(&[0xAA; 20]))]);
        let (refund_beef, refund_txid) = spender_beef(&pot, &[(2400, p2pkh_script(&[0xBB; 20]))]);
        store.store_beef(&settle_txid, &settle_beef).await.unwrap();
        store.store_beef(&refund_txid, &refund_beef).await.unwrap();
        store
            .mark_spent(&pot, 0, &settle_txid, false, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            store
                .find_pot_beefs_for_proof_check(10, 0)
                .await
                .unwrap()
                .len(),
            2,
            "no confirm yet ⇒ nothing retired — both spenders still poll"
        );

        let fetcher = MockProofFetcher {
            minable: [settle_txid.clone()].into_iter().collect(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.confirmed, 1);

        let pool: Vec<String> = store
            .find_pot_beefs_for_proof_check(10, 0)
            .await
            .unwrap()
            .into_iter()
            .map(|(t, _)| t)
            .collect();
        assert!(
            !pool.contains(&refund_txid),
            "the superseded refund is retired from the poll pool"
        );
        assert!(
            pool.contains(&settle_txid),
            "the confirmed spender still completes its own proof normally"
        );
    }

    /// Confirm beats the latch through the PRODUCTION push path: a reorg
    /// makes the latched refund the real spend, its chaintracks-verified
    /// bump arrives via /arc-ingest, and `apply_pushed_proof_to_pot_stores`
    /// (which deliberately reads the row DIRECTLY, not through the pool)
    /// stitches, compacts, and clears the latch — the latch can never
    /// suppress a real proof.
    #[tokio::test]
    async fn pushed_proof_for_a_latched_row_clears_the_latch() {
        let store = MemoryPotStorage::new();
        let pot = "ff".repeat(32);
        store.store_record(&undecoded_row(&pot)).await.unwrap();
        let (settle_beef, settle_txid) = spender_beef(&pot, &[(1200, p2pkh_script(&[0xAA; 20]))]);
        let (refund_beef, refund_txid) = spender_beef(&pot, &[(2400, p2pkh_script(&[0xBB; 20]))]);
        store.store_beef(&settle_txid, &settle_beef).await.unwrap();
        store.store_beef(&refund_txid, &refund_beef).await.unwrap();
        // The settle confirms ⇒ the refund is latched and out of the pool.
        store
            .mark_spent(
                &pot,
                0,
                &settle_txid,
                true,
                None,
                Some(u64::from(HEIGHT)),
                None,
            )
            .await
            .unwrap();
        assert!(store.is_structurally_unprovable(&refund_txid));

        // The reorg: the REFUND's verified bump is pushed.
        let bump_hex = single_tx_bump(&refund_txid, HEIGHT).to_hex();
        let s = apply_pushed_proof_to_pot_stores(&store, &refund_txid, &bump_hex).await;
        assert!(
            s.pot_beef_compacted,
            "the latch must not block the push path"
        );
        assert!(
            !store.is_structurally_unprovable(&refund_txid),
            "confirm beats the latch — the verified writer cleared it"
        );
        assert!(store.pot_beef_proof_verified(&refund_txid).await.unwrap());
    }

    /// #301 gate M1 producer path (the RacingFetcher pattern for the PUSH
    /// consumer): a storage whose `find_unconfirmed_by_spending_txid`
    /// answers the stale selection and THEN displaces one selected row's
    /// pointer — modelling a reorg-confirmed S2 landing between the push
    /// consumer's selection and its per-row write.
    struct DisplacingStore {
        inner: std::rc::Rc<MemoryPotStorage>,
        /// (txid, vout, new_spender, confirmed, height) applied AFTER the
        /// selection is taken (once).
        displacement: (String, u32, String, bool, Option<u64>),
        fired: std::cell::Cell<bool>,
    }

    #[async_trait(?Send)]
    impl PotStorage for DisplacingStore {
        async fn store_record(&self, r: &PotRecord) -> Result<(), PotStorageError> {
            self.inner.store_record(r).await
        }
        #[allow(clippy::too_many_arguments)]
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
            self.inner
                .mark_spent(
                    txid,
                    output_index,
                    spending_txid,
                    confirmed,
                    verdict,
                    spent_height,
                    spender_final,
                )
                .await
        }
        async fn get_spent_status(
            &self,
            txid: &str,
            output_index: u32,
        ) -> Result<Option<PotRecord>, PotStorageError> {
            self.inner.get_spent_status(txid, output_index).await
        }
        async fn store_beef(&self, txid: &str, beef: &[u8]) -> Result<(), PotStorageError> {
            self.inner.store_beef(txid, beef).await
        }
        async fn get_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, PotStorageError> {
            self.inner.get_beef(txid).await
        }
        async fn mark_confirmed_for_spender(
            &self,
            txid: &str,
            output_index: u32,
            spending_txid: &str,
            spent_height: Option<u64>,
        ) -> Result<bool, PotStorageError> {
            self.inner
                .mark_confirmed_for_spender(txid, output_index, spending_txid, spent_height)
                .await
        }
        async fn find_unconfirmed_by_spending_txid(
            &self,
            spending_txid: &str,
        ) -> Result<Vec<PotRecord>, PotStorageError> {
            // Take the (soon to be stale) selection FIRST, then displace —
            // the exact row-loop race window shape.
            let stale = self
                .inner
                .find_unconfirmed_by_spending_txid(spending_txid)
                .await?;
            if !self.fired.replace(true) {
                let (t, v, s, confirmed, h) = &self.displacement;
                self.inner
                    .mark_spent(t, *v, s, *confirmed, None, *h, None)
                    .await?;
            }
            Ok(stale)
        }
    }

    /// #301 gate M1: the push consumer's per-row confirm is the guarded
    /// CAS. A reorg-confirmed S2 landing after the `spendingTxid = T`
    /// selection but before the row's write is NEVER reset to T (pre-fix
    /// the unguarded `mark_spent(confirmed)` re-wrote the pointer from the
    /// stale selection); the miss is counted, the untouched sibling row
    /// still latches.
    #[tokio::test]
    async fn pushed_proof_cas_miss_never_resets_a_mid_loop_reorg_pointer() {
        let inner = std::rc::Rc::new(MemoryPotStorage::new());
        // Both pots selected by SETTLE_A; potA is displaced mid-loop.
        inner
            .store_record(&spent_unconfirmed("potA", SETTLE_A))
            .await
            .unwrap();
        inner
            .store_record(&spent_unconfirmed("potB", SETTLE_A))
            .await
            .unwrap();
        let store = DisplacingStore {
            inner: inner.clone(),
            displacement: ("potA".into(), 0, SETTLE_B.into(), true, Some(802_000)),
            fired: std::cell::Cell::new(false),
        };

        let bump_hex = single_tx_bump(SETTLE_A, HEIGHT).to_hex();
        let s = apply_pushed_proof_to_pot_stores(&store, SETTLE_A, &bump_hex).await;
        assert_eq!(
            (s.spends_confirmed, s.spends_cas_missed, s.spends_cas_errors),
            (1, 1, 0),
            "the displaced row misses (counted); the intact sibling latches"
        );

        // The reorg-confirmed S2 pointer + height SURVIVE (pre-#301-M1 the
        // stale write reset the pointer to SETTLE_A).
        let a = inner.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert_eq!(a.spending_txid.as_deref(), Some(SETTLE_B));
        assert!(a.spent_confirmed);
        assert_eq!(a.spent_height, Some(802_000), "S2 keeps its own height");
        // The sibling latched under the pushed spender.
        let b = inner.get_spent_status("potB", 0).await.unwrap().unwrap();
        assert_eq!(b.spending_txid.as_deref(), Some(SETTLE_A));
        assert!(b.spent_confirmed);
        // Terminal for potA (competing-confirmed), nothing left to chase.
        assert!(inner
            .find_spent_unconfirmed(10, 0)
            .await
            .unwrap()
            .is_empty());
    }

    /// #301 gate M2: a CAS write that ERRORS (the driver-rejects-RETURNING
    /// failure mode) is COUNTED in both consumers — a total failure
    /// self-announces as scanned>0 & confirmed=0 & cas_errors>0 instead of
    /// silently stalling confirmations with clean-looking counters.
    struct FailingCasStore(std::rc::Rc<MemoryPotStorage>);

    #[async_trait(?Send)]
    impl PotStorage for FailingCasStore {
        async fn store_record(&self, r: &PotRecord) -> Result<(), PotStorageError> {
            self.0.store_record(r).await
        }
        #[allow(clippy::too_many_arguments)]
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
            self.0
                .mark_spent(
                    txid,
                    output_index,
                    spending_txid,
                    confirmed,
                    verdict,
                    spent_height,
                    spender_final,
                )
                .await
        }
        async fn get_spent_status(
            &self,
            txid: &str,
            output_index: u32,
        ) -> Result<Option<PotRecord>, PotStorageError> {
            self.0.get_spent_status(txid, output_index).await
        }
        async fn store_beef(&self, txid: &str, beef: &[u8]) -> Result<(), PotStorageError> {
            self.0.store_beef(txid, beef).await
        }
        async fn get_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, PotStorageError> {
            self.0.get_beef(txid).await
        }
        async fn mark_confirmed_for_spender(
            &self,
            _txid: &str,
            _output_index: u32,
            _spending_txid: &str,
            _spent_height: Option<u64>,
        ) -> Result<bool, PotStorageError> {
            Err(PotStorageError::Database(
                "RETURNING rejected (test)".into(),
            ))
        }
        async fn find_spent_unconfirmed(
            &self,
            limit: u64,
            min_age_secs: u64,
        ) -> Result<Vec<PotRecord>, PotStorageError> {
            self.0.find_spent_unconfirmed(limit, min_age_secs).await
        }
        async fn find_unconfirmed_by_spending_txid(
            &self,
            spending_txid: &str,
        ) -> Result<Vec<PotRecord>, PotStorageError> {
            self.0
                .find_unconfirmed_by_spending_txid(spending_txid)
                .await
        }
    }

    #[tokio::test]
    async fn cas_errors_are_counted_in_both_consumers_never_silent() {
        let inner = std::rc::Rc::new(MemoryPotStorage::new());
        inner
            .store_record(&spent_unconfirmed("potA", SETTLE_A))
            .await
            .unwrap();
        let store = FailingCasStore(inner.clone());

        // Poll chaser: the proof verifies, the CAS errors → counted.
        let fetcher = MockProofFetcher {
            minable: [SETTLE_A.to_string()].into_iter().collect(),
            ..Default::default()
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(
            (s.scanned, s.confirmed, s.cas_missed, s.cas_errors),
            (1, 0, 0, 1),
            "the M2 signature: scanned>0, confirmed=0, cas_errors>0"
        );

        // Push consumer: same signature in its own summary shape.
        let bump_hex = single_tx_bump(SETTLE_A, HEIGHT).to_hex();
        let p = apply_pushed_proof_to_pot_stores(&store, SETTLE_A, &bump_hex).await;
        assert_eq!(
            (p.spends_confirmed, p.spends_cas_missed, p.spends_cas_errors),
            (0, 0, 1)
        );

        // Fail-safe: the row is untouched and still a candidate for retry.
        let r = inner.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(!r.spent_confirmed);
        assert_eq!(inner.find_spent_unconfirmed(10, 0).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn pushed_proof_compacts_pot_beef_and_the_poll_pass_skips_it() {
        // Same skip property for the pot_beefs pass: a pushed proof stitches +
        // compacts the stored BEEF, so find_pot_beefs_for_proof_check returns
        // nothing and the poll pass never runs the courier ladder for it.
        let store = MemoryPotStorage::new();
        let (beef, txid) = proofless_pot_beef();
        store.store_beef(&txid, &beef).await.unwrap();
        assert_eq!(
            store
                .find_pot_beefs_for_proof_check(10, 0)
                .await
                .unwrap()
                .len(),
            1,
            "proofless row is a candidate before the push"
        );

        let bump_hex = single_tx_bump(&txid, HEIGHT).to_hex();
        let s = apply_pushed_proof_to_pot_stores(&store, &txid, &bump_hex).await;
        assert!(
            s.pot_beef_compacted,
            "the pushed bump compacts the stored BEEF"
        );

        // The stored BEEF now proves its own tx…
        let stored = store.get_beef(&txid).await.unwrap().unwrap();
        assert!(overlay_discovery::pot::storage::pot_beef_has_proof(
            &txid, &stored
        ));
        // …and the poll pass has nothing left to do.
        assert!(store
            .find_pot_beefs_for_proof_check(10, 0)
            .await
            .unwrap()
            .is_empty());
        let pass_fetcher = ChainProofFetcher::new(None).with_budget(0);
        let pass = complete_pot_beef_proofs(&store, &pass_fetcher, 20, 0).await;
        assert_eq!(
            pass.scanned, 0,
            "a pushed-compacted BEEF is never re-polled"
        );
    }

    #[tokio::test]
    async fn pushed_proof_latches_a_structurally_bumped_unverified_row() {
        // bsv-low#304 gate LOW-2: the push consumer gates on the VERIFIED
        // latch, not byte structure. A fake-bumped row admitted via the
        // REAL admit path (store_beef) used to be skipped as "already
        // proven" — the free route-verified push now REPLACES the untrusted
        // bump and latches, with zero courier/poll-pass involvement.
        let store = MemoryPotStorage::new();
        let (beef, txid) = structurally_bumped_pot_beef(HEIGHT, 0);
        store.store_beef(&txid, &beef).await.unwrap();
        assert!(!store.pot_beef_proof_verified(&txid).await.unwrap());

        let pushed = single_tx_bump(&txid, HEIGHT + 2).to_hex();
        let s = apply_pushed_proof_to_pot_stores(&store, &txid, &pushed).await;
        assert!(
            s.pot_beef_compacted,
            "a structurally-bumped UNVERIFIED row must accept the verified push"
        );
        assert!(store.pot_beef_proof_verified(&txid).await.unwrap());
        assert!(store
            .find_pot_beefs_for_proof_check(10, 0)
            .await
            .unwrap()
            .is_empty());
        let stored = store.get_beef(&txid).await.unwrap().unwrap();
        assert_eq!(
            stored_bump_height(&stored, &txid),
            Some(HEIGHT + 2),
            "the untrusted admit bump was replaced by the pushed verified one"
        );

        // Idempotence: a second push against the now-VERIFIED row is a
        // no-op (verified rows are authoritative — nothing to strengthen).
        let again = apply_pushed_proof_to_pot_stores(&store, &txid, &pushed).await;
        assert!(
            !again.pot_beef_compacted,
            "an already-verified row is left alone"
        );
    }

    #[tokio::test]
    async fn pushed_malformed_bump_writes_nothing_fail_closed() {
        // Malformed-merklePath fail-closed at the apply layer: an unstitchable
        // bump must leave the stored BEEF byte-identical and the spend rows
        // unlatched — the poll backstop retains the row. (At the route, a
        // malformed/forged merklePath is already refused 422 by verify_bump
        // before apply is ever reached; this pins the second, independent
        // layer.)
        let store = MemoryPotStorage::new();
        let (beef, txid) = proofless_pot_beef();
        store.store_beef(&txid, &beef).await.unwrap();
        store
            .store_record(&spent_unconfirmed("potA", &txid))
            .await
            .unwrap();

        let s = apply_pushed_proof_to_pot_stores(&store, &txid, "deadbeef").await;
        assert_eq!(
            s,
            PushedPotSummary::default(),
            "a malformed bump latches NOTHING"
        );
        // The stored BEEF is byte-identical, the spend row unlatched, and both
        // remain poll-backstop candidates.
        assert_eq!(store.get_beef(&txid).await.unwrap().unwrap(), beef);
        assert!(
            !store
                .get_spent_status("potA", 0)
                .await
                .unwrap()
                .unwrap()
                .spent_confirmed
        );
        assert_eq!(
            store
                .find_pot_beefs_for_proof_check(10, 0)
                .await
                .unwrap()
                .len(),
            1,
            "the proofless row remains a backstop candidate"
        );
        assert_eq!(store.find_spent_unconfirmed(10, 0).await.unwrap().len(), 1);

        // A well-formed bump for a DIFFERENT txid is equally refused (its
        // root cannot be computed for OUR txid's leaf).
        let foreign = single_tx_bump(TXID, HEIGHT).to_hex();
        let s = apply_pushed_proof_to_pot_stores(&store, &txid, &foreign).await;
        assert_eq!(
            s,
            PushedPotSummary::default(),
            "a foreign bump latches NOTHING"
        );
    }

    #[tokio::test]
    async fn spend_chaser_backstop_age_gate_young_waits_old_polls() {
        // no-push-then-backstop-polls + webhook-outage degradation at the pot
        // level: a fresh 0-conf spend is NOT polled while inside the backstop
        // window (its push is still expected); once the window passes with no
        // push, the SAME pass polls and confirms it exactly as pre-#228.
        let store = MemoryPotStorage::new();
        store
            .store_record(&spent_unconfirmed("potA", "settleA"))
            .await
            .unwrap();
        // Re-record the spend at clock time so spentAt is stamped by the real
        // producer (mark_spent).
        store
            .mark_spent("potA", 0, "settleA", false, None, None, None)
            .await
            .unwrap();

        let fetcher = MockProofFetcher {
            minable: ["settleA".to_string()].into_iter().collect(),
            ..Default::default()
        };
        let min_age = PUSH_BACKSTOP_MIN_AGE_SECS;

        // Young: skipped entirely (not even scanned).
        let s = complete_spend_confirmations(&store, &fetcher, 20, min_age).await;
        assert_eq!(s.scanned, 0, "a young spend waits for its push");
        assert!(
            !store
                .get_spent_status("potA", 0)
                .await
                .unwrap()
                .unwrap()
                .spent_confirmed
        );

        // The webhook never delivers; the row ages past the gate → the
        // backstop polls and confirms (degradation to polling, not nothing).
        store.advance_clock(min_age);
        let s = complete_spend_confirmations(&store, &fetcher, 20, min_age).await;
        assert_eq!(s.scanned, 1);
        assert_eq!(
            s.confirmed, 1,
            "the backstop completes what the push missed"
        );
        assert!(
            store
                .get_spent_status("potA", 0)
                .await
                .unwrap()
                .unwrap()
                .spent_confirmed
        );
    }

    #[tokio::test]
    async fn pot_beef_pass_backstop_age_gate_young_waits_old_polls() {
        // The same young-waits/old-polls property for the pot_beefs pass,
        // through its real candidate producer (store_beef stamps createdAt).
        let store = MemoryPotStorage::new();
        let (beef, txid) = proofless_pot_beef();
        store.store_beef(&txid, &beef).await.unwrap();

        let min_age = PUSH_BACKSTOP_MIN_AGE_SECS;
        assert!(
            store
                .find_pot_beefs_for_proof_check(10, min_age)
                .await
                .unwrap()
                .is_empty(),
            "a young pot BEEF waits for its push"
        );
        store.advance_clock(min_age);
        let cands = store
            .find_pot_beefs_for_proof_check(10, min_age)
            .await
            .unwrap();
        assert_eq!(cands.len(), 1, "past the window the backstop takes over");
        assert_eq!(cands[0].0, txid);
    }
}
