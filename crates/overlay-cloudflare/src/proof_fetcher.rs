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
//! 1. **Arcade** (primary — LOW broadcasts via Arcade, so Arcade has our own
//!    txs' status + free BUMP): `GET /tx/{txid}` → if `txStatus == MINED` and a
//!    `merklePath` (a ready BRC-74 BUMP) is present.
//! 2. **WoC** (anomaly fallback): `GET /tx/{txid}/proof/tsc` (TSC JSON) + height
//!    from `GET /tx/hash/{txid}` → [`tsc_json_to_bump_hex`].
//! 3. **Bitails** (tertiary): `GET /tx/{txid}/proof/tsc` (same TSC shape).
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

/// `AncestorFetcher` backed by the Arcade→WoC→Bitails courier ladder with a
/// mandatory chaintracks re-verify before ANY bump is returned.
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

        // 2. WoC TSC (anomaly fallback — tx mined outside Arcade).
        if let Some(bump_hex) = self.woc_tsc_bump(txid).await {
            if verify_bump(tracker, &bump_hex, txid).await {
                return Some(bump_hex);
            }
            worker::console_log!("[proof] woc bump for {txid} FAILED chaintracks verify");
        }

        // 3. Bitails TSC (tertiary).
        if let Some(bump_hex) = self.bitails_tsc_bump(txid).await {
            if verify_bump(tracker, &bump_hex, txid).await {
                return Some(bump_hex);
            }
            worker::console_log!("[proof] bitails bump for {txid} FAILED chaintracks verify");
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
        let h = v.get("blockHeight").and_then(|h| h.as_u64())?;
        if h == 0 {
            return None;
        }
        u32::try_from(h).ok()
    }

    /// Fetch the raw tx hex from WoC (`GET /tx/{txid}/hex`), content-addressed.
    async fn fetch_raw_hex(&self, txid: &str) -> Result<String, GASPError> {
        let url = format!("{}/tx/{}/hex", self.woc_base, txid);
        let hdr = self.woc_api_key.as_deref().map(|k| ("woc-api-key", k));
        let (status, body) = http_get(&url, hdr)
            .await
            .map_err(|e| GASPError::RemoteError(format!("woc raw {txid}: {e}")))?;
        if status == 404 {
            return Err(GASPError::NodeNotFound(format!("WoC 404 raw {txid}")));
        }
        if !(200..300).contains(&status) {
            return Err(GASPError::RemoteError(format!("WoC {status} raw {txid}")));
        }
        Ok(body.trim().to_string())
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
/// fetch → chaintracks-verify (both inside [`ChainProofFetcher::fetch_ancestor`])
/// → stitch the BUMP → `trim_known_proven` → [`PotStorage::compact_pot_beef`]
/// (which BYPASSES the longer-wins guard AND re-checks the proof, fail-closed).
///
/// FAIL-CLOSED throughout: a candidate the fetcher can't verify is skipped
/// (retried next tick), never written proofless. Bounded by `limit`.
pub async fn complete_pot_beef_proofs(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    fetcher: &ChainProofFetcher,
    limit: u64,
) -> PotProofSummary {
    use overlay_engine::gasp::AncestorFetcher;

    let mut summary = PotProofSummary::default();

    let candidates = match pot_storage.find_pot_beefs_for_proof_check(limit).await {
        Ok(c) => c,
        Err(e) => {
            worker::console_log!("[pot-proof] candidate scan failed: {e}");
            return summary;
        }
    };
    summary.scanned = candidates.len();

    for (txid, stored_beef) in candidates {
        // fetch + chaintracks-verify (the fetcher returns a proof ONLY once its
        // root is verified against our PoW-anchored header source).
        let anc = match fetcher.fetch_ancestor(&txid).await {
            Ok(a) => a,
            Err(e) => {
                worker::console_log!("[pot-proof] {txid} fetch failed (retry): {e}");
                summary.fetch_failed += 1;
                continue;
            }
        };
        let Some(bump_hex) = anc.proof else {
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
}
