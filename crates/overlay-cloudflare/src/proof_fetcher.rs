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
use bsv_rs::transaction::{ChainTracker, MerklePath, MerklePathLeaf, Transaction};
use overlay_engine::gasp::{AncestorFetcher, FetchedAncestor, GASPError};

/// WoC mainnet base URL (mainnet only).
pub const DEFAULT_WOC_BASE: &str = "https://api.whatsonchain.com/v1/bsv/main";

/// Bitails mainnet base URL.
pub const DEFAULT_BITAILS_BASE: &str = "https://api.bitails.io";

/// Default live Arcade V2 mainnet endpoint.
pub const DEFAULT_ARCADE_URL: &str = "https://arcade-v2-us-1.bsvblockchain.tech";

/// Per-tick fetch budget — bounds a single Worker invocation under the CF
/// subrequest cap. Each proofless candidate costs a handful of subrequests
/// (raw + ≤3 courier probes + a height lookup), so ~40 keeps a tick well under
/// the cap. The candidate query is `RANDOM()`-ordered upstream so a stuck head
/// never starves the queue.
pub const DEFAULT_FETCH_BUDGET: u32 = 40;

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
    async fn fetch_verified_proof(&self, txid: &str) -> Option<String> {
        let tracker = self.tracker.as_deref();

        // 1. Arcade — our own broadcaster's free BUMP (MINED status merklePath).
        if let Some(bump_hex) = self.arcade_merklepath(txid).await {
            if verify_bump(tracker, &bump_hex, txid).await {
                return Some(bump_hex);
            }
            worker::console_log!("[proof] arcade bump for {txid} FAILED chaintracks verify");
        }

        // 2. Bitails TSC (secondary — tx mined outside Arcade).
        match self.bitails_tsc_bump(txid).await {
            Some(bump_hex) => {
                if verify_bump(tracker, &bump_hex, txid).await {
                    return Some(bump_hex);
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
            if verify_bump(tracker, &bump_hex, txid).await {
                return Some(bump_hex);
            }
            worker::console_log!("[proof] woc bump for {txid} FAILED chaintracks verify");
        }

        None
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

#[async_trait(?Send)]
impl AncestorFetcher for ChainProofFetcher {
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
        let proof = self.fetch_verified_proof(txid).await;
        Ok(FetchedAncestor { raw_tx, proof })
    }

    /// PROOF-ONLY completion path (#192/#193 FIX 2): run the courier ladder +
    /// chaintracks verify with NO raw-tx fetch — the completion passes already
    /// hold the raw in the stored BEEF, so a raw fetch there is a redundant
    /// round-trip (and a free-tier WoC raw fetch 429s). Budget-bounded exactly
    /// like [`Self::fetch_ancestor`]. Fail-closed: budget-exhausted / unmined /
    /// unverifiable → `None`.
    async fn verified_proof_for(&self, txid: &str) -> Option<String> {
        let remaining = self.budget.get();
        if remaining == 0 {
            worker::console_log!(
                "[proof] per-tick budget exhausted (skipping proof for {txid}; retried next tick)"
            );
            return None;
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
    let Some(tracker) = tracker else {
        return false; // No header source → nothing is a proven fact.
    };
    let bump = match MerklePath::from_hex(bump_hex) {
        Ok(b) => b,
        Err(_) => return false,
    };
    let root = match bump.compute_root(Some(txid)) {
        Ok(r) => r,
        Err(_) => return false,
    };
    matches!(
        tracker.is_valid_root_for_height(&root, bump.block_height).await,
        Ok(true)
    )
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
/// optional single `(name, value)` pair (e.g. the WoC api key).
async fn http_get(url: &str, header: Option<(&str, &str)>) -> Result<(u16, String), String> {
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
    /// Proofless pot BEEFs scanned this tick.
    pub scanned: usize,
    /// BEEFs upgraded with a verified BUMP, trimmed, and compacted back.
    pub completed: usize,
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
    fetcher: &ChainProofFetcher,
    limit: u64,
    min_age_secs: u64,
) -> PotProofSummary {
    use overlay_engine::gasp::AncestorFetcher;

    let mut summary = PotProofSummary::default();

    let candidates = match pot_storage
        .find_pot_beefs_for_proof_check(limit, min_age_secs)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            worker::console_log!("[pot-proof] candidate scan failed: {e}");
            return summary;
        }
    };
    summary.scanned = candidates.len();

    for (txid, stored_beef) in candidates {
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
                    worker::console_log!("[pot-proof] {txid} compact write failed: {e}");
                    summary.stitch_failed += 1;
                } else {
                    summary.completed += 1;
                }
            }
            None => {
                worker::console_log!("[pot-proof] {txid} stitch/trim failed (retry)");
                summary.stitch_failed += 1;
            }
        }
    }

    summary
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
        .and_then(|b| b.find_txid(txid).map(bsv_rs::transaction::BeefTx::has_proof))
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
    /// OBSERVABILITY ONLY (bounded to 5): the spending txids actually sampled
    /// this tick. Lets an operator check the candidates against a block explorer
    /// to tell "the chaser is broken" from "this backlog is genuinely
    /// unconfirmable" (e.g. a 0-conf spend that was later superseded and never
    /// mined, so no proof will ever exist). Never used for control flow.
    pub sample: Vec<String>,
}

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
/// NOTE (2026-07-28 gate, MEDIUM-2 sibling — deliberately NOT CAS-guarded
/// here): unlike the #284 backfill's verdict write, this pass's confirmed
/// write is justified by a FRESH SPV verification of exactly the spender it
/// writes — `verified_proof_for(rec.spending_txid)` proved THAT txid mined,
/// which is chain truth, so last-confirmed-wins re-asserting it over a
/// racing UNCONFIRMED displacement is correct, not stale. The residual is
/// the narrow window where a DIFFERENT spender was CONFIRMED in-flight (two
/// verified-mined spenders of one outpoint can only coexist across a reorg);
/// that pre-existing base-branch class is left for a follow-up issue rather
/// than folded into #284.
pub async fn complete_spend_confirmations(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    fetcher: &dyn AncestorFetcher,
    limit: u64,
    min_age_secs: u64,
) -> SpendConfirmSummary {
    let mut summary = SpendConfirmSummary::default();

    let candidates = match pot_storage.find_spent_unconfirmed(limit, min_age_secs).await {
        Ok(c) => c,
        Err(e) => {
            worker::console_log!("[spend-confirm] candidate scan failed: {e}");
            return summary;
        }
    };
    summary.scanned = candidates.len();

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
        // unmined / unverifiable / budget-exhausted → `None` (retry), never a
        // positive.
        match fetcher.verified_proof_for(spending_txid).await {
            Some(bump_hex) => {
                // UPGRADE: latch spentConfirmed = 1. mark_spent(confirmed=true)
                // always writes and never downgrades a confirmed row.
                //
                // #284: this caller only CONFIRMS an existing pointer and has
                // no spender raw in hand → verdict = None (the SQL leaves the
                // stored verdict/verdictTxid UNCHANGED — never nulled). The
                // spentHeight DOES ride along: the block height is a fact of
                // the just-verified BUMP.
                let spent_height = MerklePath::from_hex(&bump_hex)
                    .ok()
                    .map(|mp| u64::from(mp.block_height));
                if let Err(e) = pot_storage
                    .mark_spent(
                        &rec.txid,
                        rec.output_index,
                        spending_txid,
                        true,
                        None,
                        spent_height,
                    )
                    .await
                {
                    worker::console_log!("[spend-confirm] {} mark_spent failed: {e}", rec.txid);
                } else {
                    summary.confirmed += 1;
                }
            }
            None => {
                summary.still_unconfirmed += 1;
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
    use overlay_discovery::pot::{
        classify_covenant, extract_covenant_params, is_bare_2of3_lock, is_p2pkh_script, RawTx,
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
                push_log(&format!("[params-backfill] {} beef read failed: {e}", row.txid));
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
            push_log(&format!("[params-backfill] {} decoded upsert failed: {e}", row.txid));
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
        let Some(pot_input_sequence) = spending_tx.inputs.iter().find_map(|i| {
            (i.source_txid
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case(&row.txid))
                && i.source_output_index == row.output_index)
                .then_some(i.sequence)
        }) else {
            continue; // the recorded spender does not spend this outpoint
        };
        let Some(spender) = RawTx::from_transaction(&spending_tx) else {
            continue;
        };
        let Some(verdict) = classify_covenant(&params, &spender, pot_input_sequence) else {
            continue; // non-template spend — honestly unresolved
        };
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
                verdict.as_str(),
            )
            .await
        {
            push_log(&format!("[params-backfill] {} verdict write failed: {e}", row.txid));
        } else {
            summary.verdicts += 1;
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
pub const REBROADCAST_MIN_AGE_SECS: u64 = PUSH_BACKSTOP_MIN_AGE_SECS;

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
        // ancestry-first. The EF batch is already dependency-ordered,
        // subject last.
        let (efs, subject_txid) = match crate::ef::beef_to_ef_batch(&candidate.beef) {
            Ok(v) => v,
            Err(e) => {
                push_log(&format!(
                    "[rebroadcast-backstop] {txid} stored BEEF unusable ({e}) — retry later"
                ));
                summary.rebroadcast_failed += 1;
                continue;
            }
        };
        // An absent-yet-all-proven BEEF (fake or stale bump — the #268
        // class): fall back to the subject raw so the rescue still fires.
        let legs: Vec<(String, Vec<u8>)> = if efs.is_empty() {
            match crate::ef::proven_subject_raw(&candidate.beef) {
                Some(raw) => vec![(subject_txid.clone(), raw)],
                None => {
                    push_log(&format!(
                        "[rebroadcast-backstop] {txid} absent but no broadcastable bytes — retry later"
                    ));
                    summary.rebroadcast_failed += 1;
                    continue;
                }
            }
        } else {
            efs.into_iter().map(|e| (e.txid, e.ef)).collect()
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
                crate::broadcaster::broadcast_tx_hex_gated(taal_api_key, &hex::encode(bytes))
                    .await;
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
}

impl PushedPotSummary {
    /// Whether the push landed in ANY pot store.
    pub fn landed_anything(&self) -> bool {
        self.pot_beef_compacted || self.spends_confirmed > 0
    }
}

/// wasm-safe log for the push consumer: `worker::console_log!` panics off-wasm
/// ("function not implemented on non-wasm32 targets"), and unlike the poll
/// passes this path IS exercised by native unit tests.
fn push_log(msg: &str) {
    #[cfg(target_arch = "wasm32")]
    worker::console_log!("{}", msg);
    #[cfg(not(target_arch = "wasm32"))]
    eprintln!("{msg}");
}

/// Fold an `/arc-ingest`-pushed, ALREADY-chaintracks-VERIFIED bump for `txid`
/// into the LOW pot stores, so the poll passes skip the tx entirely:
///
/// 1. `pot_beefs`: if a stored BEEF for `txid` exists and is still proofless,
///    stitch the bump, trim, and [`PotStorage::compact_pot_beef`] (which
///    re-checks the proof, fail-closed) — same shape as one
///    [`complete_pot_beef_proofs`] candidate, minus the courier fetch.
/// 2. `pot_records`: every outpoint whose recorded spender is `txid` and is
///    still unconfirmed is upgraded via `mark_spent(confirmed = true)` — the
///    spending tx verifiably mined, which is exactly the #186 chaser's latch
///    condition.
///
/// SECURITY PRECONDITION: the caller MUST have verified `bump_hex` against
/// chaintracks for `txid` first (`/arc-ingest` refuses unverifiable proofs
/// with 422 before ever reaching here). This function still fails closed on
/// its own account: a bump that doesn't stitch/prove writes nothing, and
/// `compact_pot_beef` re-checks the proof at the storage layer.
///
/// Best-effort per store: a failure in one store is logged and does not block
/// the other (the poll backstop still covers whatever didn't land).
pub async fn apply_pushed_proof_to_pot_stores(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    txid: &str,
    bump_hex: &str,
) -> PushedPotSummary {
    use overlay_discovery::pot::storage::pot_beef_has_proof;

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
        push_log(&format!("[arc-ingest] {txid} pushed bump is malformed — nothing latched"));
        return summary;
    }

    // 1. pot_beefs stitch + compact.
    match pot_storage.get_beef(txid).await {
        Ok(Some(stored_beef)) if !pot_beef_has_proof(txid, &stored_beef) => {
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
                    push_log(&format!("[arc-ingest] {txid} pot-beef stitch failed (backstop will retry)"));
                }
            }
        }
        Ok(_) => {} // no pot beef, or already proven — nothing to do
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
                match pot_storage
                    .mark_spent(&rec.txid, rec.output_index, txid, true, None, spent_height)
                    .await
                {
                    Ok(()) => summary.spends_confirmed += 1,
                    Err(e) => push_log(&format!(
                        "[arc-ingest] {}:{} spend-confirm latch failed: {e}",
                        rec.txid, rec.output_index
                    )),
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
        MerklePath::new(
            height,
            vec![vec![MerklePathLeaf::new_txid(0, txid.into())]],
        )
        .expect("valid single-leaf merkle path")
    }

    // ── 0. #273 rebroadcast-backstop presence classification ─────────────────

    #[test]
    fn presence_positive_from_either_indexer_is_present() {
        // A single indexer holding the tx is enough to STAND DOWN (positive
        // presence is cheap to trust — the harmful error is a missed rescue,
        // and proof completion covers a present tx).
        assert_eq!(classify_presence(Some(true), None), NetworkPresence::Present);
        assert_eq!(classify_presence(None, Some(true)), NetworkPresence::Present);
        assert_eq!(classify_presence(Some(true), Some(false)), NetworkPresence::Present);
        assert_eq!(classify_presence(Some(false), Some(true)), NetworkPresence::Present);
    }

    #[test]
    fn presence_absent_requires_both_definitive_404s() {
        // The ACTION verdict needs BOTH indexers' definitive 404 — a negative
        // never rests on one provider's word (#212/#213/#214 doctrine).
        assert_eq!(classify_presence(Some(false), Some(false)), NetworkPresence::Absent);
        // One-sided 404s and faults are inconclusive → no action, retried.
        assert_eq!(classify_presence(Some(false), None), NetworkPresence::Inconclusive);
        assert_eq!(classify_presence(None, Some(false)), NetworkPresence::Inconclusive);
        assert_eq!(classify_presence(None, None), NetworkPresence::Inconclusive);
    }

    // ── 1. Arcade merklePath extraction ──────────────────────────────────────

    #[test]
    fn arcade_mined_with_merklepath_extracts_bump() {
        let bump_hex = single_tx_bump(TXID, HEIGHT).to_hex();
        let body = format!(
            r#"{{"txid":"{TXID}","txStatus":"MINED","blockHeight":{HEIGHT},"merklePath":"{bump_hex}"}}"#
        );
        assert_eq!(parse_arcade_merklepath(&body).as_deref(), Some(bump_hex.as_str()));
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

    use overlay_discovery::pot::storage::{MemoryPotStorage, PotRecord, PotStorage};

    /// A fetcher whose `verified_proof_for` returns a (dummy) verified bump ONLY
    /// for the txids in `minable` — models the chaintracks-verified vs unmined
    /// outcome without hitting the network (the concrete ChainProofFetcher is
    /// network-only). `fetch_ancestor` is never called by the pass.
    struct MockProofFetcher {
        minable: std::collections::HashSet<String>,
    }

    #[async_trait(?Send)]
    impl AncestorFetcher for MockProofFetcher {
        async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
            Err(GASPError::NodeNotFound(format!("mock: no ancestor for {txid}")))
        }
        async fn verified_proof_for(&self, txid: &str) -> Option<String> {
            self.minable.contains(txid).then(|| "beefbump".to_string())
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
        store.mark_spent("potA", 0, "settleA", false, None, None).await.unwrap();

        let fetcher = MockProofFetcher {
            minable: ["settleA".to_string()].into_iter().collect(),
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.scanned, 1);
        assert_eq!(s.confirmed, 1);
        assert_eq!(s.still_unconfirmed, 0);

        // The row is now SPV-confirmed and drops out of the candidate set.
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed, "a verified spend latches spentConfirmed");
        assert_eq!(r.spending_txid.as_deref(), Some("settleA"));
        assert!(store.find_spent_unconfirmed(10, 0).await.unwrap().is_empty());
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
        store.mark_spent("potA", 0, "settleA", false, None, None).await.unwrap();

        // The spending tx is NOT verifiably mined → fail-closed, no upgrade.
        let fetcher = MockProofFetcher {
            minable: std::collections::HashSet::new(),
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
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s, SpendConfirmSummary::default());
    }

    #[tokio::test]
    async fn spend_confirmation_only_upgrades_the_mined_row() {
        let store = MemoryPotStorage::new();
        for (txid, spender) in [("potA", "settleA"), ("potB", "settleB")] {
            store.store_record(&spent_unconfirmed(txid, spender)).await.unwrap();
        }
        // Only settleA is mined.
        let fetcher = MockProofFetcher {
            minable: ["settleA".to_string()].into_iter().collect(),
        };
        let s = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(s.scanned, 2);
        assert_eq!(s.confirmed, 1);
        assert_eq!(s.still_unconfirmed, 1);

        assert!(store.get_spent_status("potA", 0).await.unwrap().unwrap().spent_confirmed);
        assert!(!store.get_spent_status("potB", 0).await.unwrap().unwrap().spent_confirmed);
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

    /// 64-hex settle txids (a bump subject must be a real txid shape).
    const SETTLE_A: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SETTLE_B: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    // ── #284 decoded-params lazy backfill ────────────────────────────────

    use overlay_discovery::pot::{
        encode_covenant_param_pushes, CovenantParams, POC5_TEMPLATE_HEX,
    };

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
        tx.add_input(TransactionInput::new(hex::encode([salt; 32]), 0)).unwrap();
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
        tx.add_input(TransactionInput::new(pot_txid.to_string(), 0)).unwrap();
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
        let bare = store.get_spent_status(&bare_txid, 0).await.unwrap().unwrap();
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
        store.store_record(&undecoded_row("potNoBeef")).await.unwrap();
        let s = backfill_decoded_params(&store, 20).await;
        assert_eq!((s.scanned, s.decoded, s.missing_beef), (1, 0, 1));
        // Still a candidate next tick (retry forever, bounded per tick).
        let s2 = backfill_decoded_params(&store, 20).await;
        assert_eq!(s2.scanned, 1, "a missing-BEEF row is retried");
        let r = store.get_spent_status("potNoBeef", 0).await.unwrap().unwrap();
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
            &[(25, p2pkh_script(&p.rake_pkh)), (2375, p2pkh_script(&p.pay_pkh_b))],
        );

        // A pre-#284 row that is already SPENT (unconfirmed pointer), with
        // both BEEFs durably stored.
        store.store_record(&undecoded_row(&cov_txid)).await.unwrap();
        store
            .mark_spent(&cov_txid, 0, &settle_txid, false, None, None)
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
            &[(1800, p2pkh_script(&[0xAA; 20])), (1800, p2pkh_script(&[0xBB; 20]))],
        );
        store.store_record(&undecoded_row(&bare_txid)).await.unwrap();
        store
            .mark_spent(&bare_txid, 0, &bspend_txid, false, None, None)
            .await
            .unwrap();
        store.store_beef(&bare_txid, &bare_beef).await.unwrap();
        store.store_beef(&bspend_txid, &bspend).await.unwrap();
        let s = backfill_decoded_params(&store, 20).await;
        assert_eq!(s.verdicts, 0, "bare pots NEVER get a stored verdict");
        let r = store.get_spent_status(&bare_txid, 0).await.unwrap().unwrap();
        assert_eq!(r.lock_kind.as_deref(), Some("bare"));
        assert_eq!(r.verdict, None);
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
            store.store_record(&spent_unconfirmed(pot, SETTLE_A)).await.unwrap();
        }
        // A third pot spent by a DIFFERENT settle stays untouched.
        store.store_record(&spent_unconfirmed("potC", SETTLE_B)).await.unwrap();

        let bump_hex = single_tx_bump(SETTLE_A, HEIGHT).to_hex();
        let s = apply_pushed_proof_to_pot_stores(&store, SETTLE_A, &bump_hex).await;
        assert_eq!(s.spends_confirmed, 2, "both outpoints the settle spent are latched");

        for pot in ["potA", "potB"] {
            assert!(store.get_spent_status(pot, 0).await.unwrap().unwrap().spent_confirmed);
        }
        assert!(!store.get_spent_status("potC", 0).await.unwrap().unwrap().spent_confirmed);

        // The chaser (min_age 0 = widest possible candidate set) now sees only
        // potC — and with an unminable fetcher it upgrades nothing.
        let fetcher = MockProofFetcher {
            minable: std::collections::HashSet::new(),
        };
        let chase = complete_spend_confirmations(&store, &fetcher, 20, 0).await;
        assert_eq!(chase.scanned, 1, "pushed-latched rows are skipped entirely");
        assert_eq!(chase.sample, vec![SETTLE_B.to_string()]);
        assert_eq!(chase.confirmed, 0);
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
            store.find_pot_beefs_for_proof_check(10, 0).await.unwrap().len(),
            1,
            "proofless row is a candidate before the push"
        );

        let bump_hex = single_tx_bump(&txid, HEIGHT).to_hex();
        let s = apply_pushed_proof_to_pot_stores(&store, &txid, &bump_hex).await;
        assert!(s.pot_beef_compacted, "the pushed bump compacts the stored BEEF");

        // The stored BEEF now proves its own tx…
        let stored = store.get_beef(&txid).await.unwrap().unwrap();
        assert!(overlay_discovery::pot::storage::pot_beef_has_proof(&txid, &stored));
        // …and the poll pass has nothing left to do.
        assert!(store.find_pot_beefs_for_proof_check(10, 0).await.unwrap().is_empty());
        let pass_fetcher = ChainProofFetcher::new(None).with_budget(0);
        let pass = complete_pot_beef_proofs(&store, &pass_fetcher, 20, 0).await;
        assert_eq!(pass.scanned, 0, "a pushed-compacted BEEF is never re-polled");
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
        store.store_record(&spent_unconfirmed("potA", &txid)).await.unwrap();

        let s = apply_pushed_proof_to_pot_stores(&store, &txid, "deadbeef").await;
        assert_eq!(s, PushedPotSummary::default(), "a malformed bump latches NOTHING");
        // The stored BEEF is byte-identical, the spend row unlatched, and both
        // remain poll-backstop candidates.
        assert_eq!(store.get_beef(&txid).await.unwrap().unwrap(), beef);
        assert!(!store.get_spent_status("potA", 0).await.unwrap().unwrap().spent_confirmed);
        assert_eq!(
            store.find_pot_beefs_for_proof_check(10, 0).await.unwrap().len(),
            1,
            "the proofless row remains a backstop candidate"
        );
        assert_eq!(store.find_spent_unconfirmed(10, 0).await.unwrap().len(), 1);

        // A well-formed bump for a DIFFERENT txid is equally refused (its
        // root cannot be computed for OUR txid's leaf).
        let foreign = single_tx_bump(TXID, HEIGHT).to_hex();
        let s = apply_pushed_proof_to_pot_stores(&store, &txid, &foreign).await;
        assert_eq!(s, PushedPotSummary::default(), "a foreign bump latches NOTHING");
    }

    #[tokio::test]
    async fn spend_chaser_backstop_age_gate_young_waits_old_polls() {
        // no-push-then-backstop-polls + webhook-outage degradation at the pot
        // level: a fresh 0-conf spend is NOT polled while inside the backstop
        // window (its push is still expected); once the window passes with no
        // push, the SAME pass polls and confirms it exactly as pre-#228.
        let store = MemoryPotStorage::new();
        store.store_record(&spent_unconfirmed("potA", "settleA")).await.unwrap();
        // Re-record the spend at clock time so spentAt is stamped by the real
        // producer (mark_spent).
        store.mark_spent("potA", 0, "settleA", false, None, None).await.unwrap();

        let fetcher = MockProofFetcher {
            minable: ["settleA".to_string()].into_iter().collect(),
        };
        let min_age = PUSH_BACKSTOP_MIN_AGE_SECS;

        // Young: skipped entirely (not even scanned).
        let s = complete_spend_confirmations(&store, &fetcher, 20, min_age).await;
        assert_eq!(s.scanned, 0, "a young spend waits for its push");
        assert!(!store.get_spent_status("potA", 0).await.unwrap().unwrap().spent_confirmed);

        // The webhook never delivers; the row ages past the gate → the
        // backstop polls and confirms (degradation to polling, not nothing).
        store.advance_clock(min_age);
        let s = complete_spend_confirmations(&store, &fetcher, 20, min_age).await;
        assert_eq!(s.scanned, 1);
        assert_eq!(s.confirmed, 1, "the backstop completes what the push missed");
        assert!(store.get_spent_status("potA", 0).await.unwrap().unwrap().spent_confirmed);
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
            store.find_pot_beefs_for_proof_check(10, min_age).await.unwrap().is_empty(),
            "a young pot BEEF waits for its push"
        );
        store.advance_clock(min_age);
        let cands = store.find_pot_beefs_for_proof_check(10, min_age).await.unwrap();
        assert_eq!(cands.len(), 1, "past the window the backstop takes over");
        assert_eq!(cands[0].0, txid);
    }
}
