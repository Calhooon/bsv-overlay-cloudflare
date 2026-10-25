//! Worker I/O is staged before the synchronous tracker boundary. No HTTP
//! answer, missing header, or transport error is a chain word.
use bsv_rs::transaction::{MerklePath, MerklePathLeaf};
use bsv_tracker::{
    Clock, Header, HeaderError, Headers, Hint, HintSource, Proof, ProofFetcher, Reask, Verdict,
};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};

/// Chaintracks' served, verified header tip. `at` is the header's time in milliseconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedTip {
    pub height: u32,
    pub hash: String,
    pub at: u64,
}

thread_local! {
    // Diagnostics only: a failed current read never serves this as a fresh tip.
    static LAST_VERIFIED_TIP: RefCell<Option<VerifiedTip>> = const { RefCell::new(None) };
}

pub(crate) fn last_verified_tip() -> Option<VerifiedTip> {
    LAST_VERIFIED_TIP.with(|last| last.borrow().clone())
}

/// The one tip reader for the proof snapshot and the app layer's height reads.
/// Chaintracks serves this header under the same validation ceiling as its proof headers.
pub(crate) async fn verified_tip_with<G, F>(get: G) -> Result<VerifiedTip, HeaderError>
where
    G: FnOnce(&'static str) -> F,
    F: std::future::Future<Output = Result<(u16, Value), HeaderError>>,
{
    let (status, frame) = get("findChainTipHeaderHex").await?;
    let malformed = || HeaderError("unverified or malformed tip frame".into());
    let height = frame["value"]["height"]
        .as_u64()
        .and_then(|h| u32::try_from(h).ok())
        .ok_or_else(malformed)?;
    let header = header_response(height, status, &frame)?.ok_or_else(malformed)?;
    let time = frame["value"]["time"]
        .as_u64()
        .and_then(|t| u32::try_from(t).ok())
        .ok_or_else(malformed)?;
    let tip = VerifiedTip {
        height,
        hash: header.hash,
        at: u64::from(time) * 1_000,
    };
    LAST_VERIFIED_TIP.with(|last| *last.borrow_mut() = Some(tip.clone()));
    Ok(tip)
}

pub(crate) async fn chaintracks_get(
    env: &worker::Env,
    route: &str,
) -> Result<(u16, Value), HeaderError> {
    let svc = env
        .service("CHAINTRACKS")
        .map_err(|e| HeaderError(e.to_string()))?;
    let mut resp = svc
        .fetch(format!("https://chaintracks/{route}"), None)
        .await
        .map_err(|e| HeaderError(e.to_string()))?;
    let status = resp.status_code();
    if status == 404 {
        return Ok((status, Value::Null));
    }
    let frame = resp.json().await.map_err(|e| HeaderError(e.to_string()))?;
    Ok((status, frame))
}

/// Only the named heights fetched from CHAINTRACKS, with the answering tip.
#[derive(Clone, Debug)]
pub struct HeaderSnapshot {
    pub tip: Result<u32, HeaderError>,
    pub headers: BTreeMap<u32, Result<Option<Header>, HeaderError>>,
}
impl Headers for HeaderSnapshot {
    fn header_at(&self, height: u32) -> Result<Option<Header>, HeaderError> {
        if self.tip_height()? < height {
            return Ok(None);
        }
        self.headers.get(&height).cloned().unwrap_or(Ok(None))
    }
    fn tip_height(&self) -> Result<u32, HeaderError> {
        self.tip.clone()
    }
}

pub fn header_response(
    height: u32,
    status: u16,
    frame: &Value,
) -> Result<Option<Header>, HeaderError> {
    if status == 404 {
        return Ok(None);
    }
    if !(200..300).contains(&status) {
        return Err(HeaderError(format!("header HTTP {status}")));
    }
    crate::internal_events::parse_header(frame, u64::from(height))
        .map(|h| {
            Some(Header {
                hash: h.hash,
                merkle_root: h.merkle_root,
            })
        })
        .ok_or_else(|| HeaderError("unverified or malformed header frame".into()))
}

/// Fetch named heights only. The tip brackets the header fetches; a changing
/// tip is an unanswerable snapshot and is retried by the host, never trusted.
pub async fn headers(env: &worker::Env, heights: &[u32]) -> HeaderSnapshot {
    headers_with(
        |route| async move { chaintracks_get(env, &route).await },
        heights,
    )
    .await
}

pub(crate) async fn headers_with<G, F>(get: G, heights: &[u32]) -> HeaderSnapshot
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Result<(u16, Value), HeaderError>>,
{
    retry_snapshot(|| async {
        let before = verified_tip_with(|route| get(route.to_owned())).await;
        let mut rows = BTreeMap::new();
        for height in heights {
            let row = match get(format!("findHeaderHexForHeight?height={height}")).await {
                Ok((status, frame)) => header_response(*height, status, &frame),
                Err(e) => Err(e),
            };
            rows.insert(*height, row);
        }
        let after = verified_tip_with(|route| get(route.to_owned())).await;
        let tip = if before == after {
            after.map(|tip| tip.height)
        } else {
            Err(HeaderError("header snapshot changed".into()))
        };
        HeaderSnapshot { tip, headers: rows }
    })
    .await
}

/// Completed courier transports, in ladder order. `fetch` returns raw
/// proof inputs; State.step and Headers.check own all confirmation decisions.
pub struct CourierProofs(pub VecDeque<Result<Option<Proof>, String>>);
impl ProofFetcher for CourierProofs {
    type Error = String;
    fn fetch(&mut self, txid: &str, _reask: &Reask) -> Result<Option<Proof>, String> {
        match self.0.pop_front().unwrap_or(Ok(None))? {
            Some(p) if p.txid().eq_ignore_ascii_case(txid) => Ok(Some(p)),
            Some(_) => Err("courier proof txid mismatch".into()),
            None => Ok(None),
        }
    }
}

/// Exact production TSC shape, including duplicate nodes, reduced by SDK.
pub fn tsc_proof(txid: &str, height: u32, body: &[u8]) -> Result<Proof, String> {
    let value: Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let v = value.as_array().and_then(|a| a.first()).unwrap_or(&value);
    let id = v["txOrId"].as_str().ok_or("TSC txOrId missing")?;
    if !id.eq_ignore_ascii_case(txid) {
        return Err("TSC txid mismatch".into());
    }
    let mut offset = v["index"].as_u64().ok_or("TSC index missing")?;
    let nodes = v["nodes"].as_array().ok_or("TSC nodes missing")?;
    if nodes.is_empty() || nodes.len() > 64 {
        return Err("TSC path depth".into());
    }
    let mut path = Vec::new();
    for (level, node) in nodes.iter().enumerate() {
        let mut leaves = Vec::new();
        if level == 0 {
            leaves.push(MerklePathLeaf::new_txid(offset, txid.to_ascii_lowercase()));
        }
        let sibling = offset ^ 1;
        let hash = node.as_str().ok_or("TSC node shape")?;
        leaves.push(if hash == "*" {
            MerklePathLeaf::new_duplicate(sibling)
        } else {
            MerklePathLeaf::new(sibling, hash.to_ascii_lowercase())
        });
        leaves.sort_by_key(|l| l.offset);
        path.push(leaves);
        offset /= 2;
    }
    let path = MerklePath::new(height, path).map_err(|e| e.to_string())?;
    Proof::new(txid, path).map_err(|e| e.to_string())
}

pub struct BroadcasterHints(pub VecDeque<(String, Hint)>);
impl BroadcasterHints {
    pub fn arc(txid: &str, body: &[u8], observed: u64) -> Result<Self, bsv_tracker::VerdictError> {
        let hint = Verdict::from_arc_body(txid, body)?.into_hint("arcade", observed);
        Ok(Self(VecDeque::from([(txid.to_ascii_lowercase(), hint)])))
    }
}
impl HintSource for BroadcasterHints {
    type Error = std::convert::Infallible;
    fn next_hint(&mut self) -> Result<Option<(String, Hint)>, Self::Error> {
        Ok(self.0.pop_front())
    }
}

pub struct WorkerClock;
impl Clock for WorkerClock {
    fn now(&self) -> u64 {
        seconds(worker::Date::now().as_millis())
    }
}
/// Pure clock conversion shared with the fake platform pin.
pub fn seconds(millis: u64) -> u64 {
    millis / 1_000
}

/// Execute the courier ladder against fake or Worker transports. Header
/// verification occurs before moving to the next provider.
pub async fn execute_reask<G, F, H, HF>(
    txid: &str,
    first_party: bool,
    arcade: &str,
    reask: &Reask,
    get: G,
    header_snapshot: H,
) -> Result<Option<Proof>, String>
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Option<(u16, Vec<u8>)>>,
    H: Fn(u32) -> HF,
    HF: std::future::Future<Output = HeaderSnapshot>,
{
    async fn checked<H, HF>(
        txid: &str,
        proof: Proof,
        reask: &Reask,
        snapshot: &H,
    ) -> Result<Option<Proof>, String>
    where
        H: Fn(u32) -> HF,
        HF: std::future::Future<Output = HeaderSnapshot>,
    {
        let mut fetcher = CourierProofs(VecDeque::from([Ok(Some(proof))]));
        let proof = fetcher.fetch(txid, reask)?.ok_or("missing staged proof")?;
        let headers = snapshot(proof.height()).await;
        match headers.check(proof.clone()) {
            Ok(_) => Ok(Some(proof)),
            Err(_) => Ok(None),
        }
    }
    if first_party {
        if let Some((200, body)) = get(format!("{arcade}/tx/{txid}")).await {
            let terminal = Verdict::from_arc_body(txid, &body)
                .is_ok_and(|v| runtime::terminal_hint(&v.into_hint("arcade", 0)));
            // A terminal hint stops rotation only after couriers check for proof.
            if let Some(frame) = serde_json::from_slice::<Value>(&body)
                .ok()
                .filter(|_| !terminal)
            {
                if let Some(hex) = frame["merklePath"].as_str() {
                    if let Ok(path) = overlay_engine::beef_limits::merkle_path_from_hex(
                        hex,
                        overlay_engine::beef_limits::COURIER_PROOF_MAX_BYTES,
                    ) {
                        if let Ok(proof) = Proof::new(txid, path) {
                            if let Some(p) = checked(txid, proof, reask, &header_snapshot).await? {
                                return Ok(Some(p));
                            }
                        }
                    }
                }
            }
        }
    }
    // Existing overlay TSC contracts; claimed heights only select the check.
    let rungs = [
        (
            "woc",
            "https://api.whatsonchain.com/v1/bsv/main",
            "tx/hash",
            "proof/tsc",
        ),
        ("bitails", "https://api.bitails.io", "tx", "proof/tsc"),
        (
            "bananablocks",
            "https://bananablocks.com/api/v1",
            "tx",
            "proof",
        ),
    ];
    for rung in pacing::order() {
        let (source, base, detail, proof_route) = rungs[rung];
        if !pacing::allowed(rung, source) {
            continue;
        }
        pacing::note(source, "asked");
        let response = get(format!("{base}/{detail}/{txid}")).await;
        if response
            .as_ref()
            .is_none_or(|(status, _)| *status != 200 && *status != 404)
        {
            pacing::fault(rung, source);
        }
        if let Some((200, body)) = response {
            if let Ok(v) = serde_json::from_slice::<Value>(&body) {
                if let Some(height) = v["blockheight"]
                    .as_u64()
                    .or_else(|| v["blockHeight"].as_u64())
                    .or_else(|| v["block_height"].as_u64())
                    .and_then(|h| u32::try_from(h).ok())
                    .filter(|h| *h > 0)
                {
                    let response = get(format!("{base}/tx/{txid}/{proof_route}")).await;
                    if response
                        .as_ref()
                        .is_none_or(|(status, _)| *status != 200 && *status != 404)
                    {
                        pacing::fault(rung, source);
                    }
                    if let Some((200, bytes)) = response {
                        if let Ok(proof) = tsc_proof(txid, height, &bytes) {
                            if let Some(p) = checked(txid, proof, reask, &header_snapshot).await? {
                                return Ok(Some(p));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(None)
}
pub mod chain_events;
pub mod served;

pub mod stored;

pub mod pacing;
pub mod runtime;

/// One retry of a bracketed header read, then cannotCheck; no promotion.
pub async fn retry_snapshot<F, Fut>(mut read: F) -> HeaderSnapshot
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = HeaderSnapshot>,
{
    let first = read().await;
    if first.tip.is_ok() {
        first
    } else {
        read().await
    }
}
