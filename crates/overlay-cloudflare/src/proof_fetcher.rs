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
            Some(_bump) => {
                // UPGRADE: latch spentConfirmed = 1. mark_spent(confirmed=true)
                // always writes and never downgrades a confirmed row.
                if let Err(e) = pot_storage
                    .mark_spent(&rec.txid, rec.output_index, spending_txid, true)
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
    match pot_storage.find_unconfirmed_by_spending_txid(txid).await {
        Ok(records) => {
            for rec in records {
                match pot_storage
                    .mark_spent(&rec.txid, rec.output_index, txid, true)
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
            })
            .await
            .unwrap();
        store.mark_spent("potA", 0, "settleA", false).await.unwrap();

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
            })
            .await
            .unwrap();
        store.mark_spent("potA", 0, "settleA", false).await.unwrap();

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
        store.mark_spent("potA", 0, "settleA", false).await.unwrap();

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
