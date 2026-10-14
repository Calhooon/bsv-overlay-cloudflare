//! Targeted peer lookups supplement GASP discovery with hosts for content
//! this overlay already carries. UHRP peers require a selector; they do not
//! promise a list-all query. Each pass samples at most 16 distinct live hashes
//! from local adverts, queries their bare UHRP URLs, and admits returned BEEF
//! through the normal engine checks. GASP enumerates unfamiliar content.

use overlay_engine::engine::Engine;
use overlay_engine::types::{SubmitMode, TaggedBEEF};
use serde::Deserialize;
use std::collections::HashMap;

/// One (peer_url, service_to_topic) mapping. `service_to_topic` is a
/// per-peer list of `(lookup_service_name, topic_manager_name)` pairs
/// — e.g. `[("ls_uhrp", "tm_uhrp"), ("ls_ship", "tm_ship")]`. Separate
/// per-peer because peers might carry different subsets of services.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub peer_url: String,
    pub service_to_topic: Vec<(String, String)>,
}

/// Summary of one crawl run.
#[derive(Debug, Default)]
pub struct CrawlResult {
    /// Per-peer, per-service admission counts.
    ///
    /// Keyed by `"{peer_url}|{service}"`. Value = number of outputs
    /// admitted by the corresponding tm_X on our engine.
    pub admitted_by: HashMap<String, usize>,
    /// Per-peer, per-service total submission attempts (admitted +
    /// rejected). `admitted_by[k] <= attempted[k]` always.
    pub attempted: HashMap<String, usize>,
    /// Errors per `"{peer_url}|{service}"`. Non-empty value = at least
    /// one submission failed but crawl continued.
    pub errors: HashMap<String, Vec<String>>,
    /// Peer-level errors (lookup failed entirely; couldn't even
    /// enumerate records).
    pub peer_errors: HashMap<String, String>,
    /// The engine walks of this crawl that could not run within the budget (bsv-low #592; the E592 lens fold, L3):
    /// a subject's (not now, nothing admitted, crawled again next tick) and a carried predecessor's landing's. Each
    /// is COUNTED inside the crawl as its submit returns (`crawl_peers`' `db`; the E592 delta lens, NOTE-3), so a
    /// crawl the cron drops at its budget keeps the counts of the submits it finished. This list is for tests and
    /// future callers: neither the cron's log line nor `/admin/crawlPeers` serves it (the E592 delta-2 lens, NOTE-4);
    /// the counters and the per-stop log lines carry the record.
    pub walks_could_not_run: Vec<overlay_engine::engine::WalkCouldNotRun>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OutputListResponse {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    outputs: Vec<OutputEntry>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OutputEntry {
    /// BEEF as a JSON number array — same shape the rest of the
    /// overlay family uses (TS SDK, Go SDK, our own /lookup handler).
    pub(crate) beef: Vec<u8>,
}

/// Crawl every configured peer, admitting whatever their `/lookup`
/// returns into our own engine. Fire-and-forget per-output — errors
/// are logged and stashed in the result, never raised.
///
/// `origin` is just for log prefixing ("cron" vs "admin") so you can
/// tell at a glance whether a log line came from the 15-min cron or
/// from an operator-triggered `/admin/crawlPeers`.
///
/// `db` is where each walk that could not run is counted, as its submit
/// returns (`ops::note_engine_walk`, `ops::note_engine_walk_not_now`): the
/// cron races the crawl against `PEER_CRAWL_BUDGET_MS` and a count made after
/// the race was lost with a dropped crawl (the E592 delta lens, NOTE-3).
pub async fn crawl_peers(
    engine: &Engine,
    peers: &[PeerConfig],
    origin: &str,
    db: Option<&worker::D1Database>,
) -> CrawlResult {
    let mut result = CrawlResult::default();
    for peer in peers {
        for (service, topic) in &peer.service_to_topic {
            let key = format!("{}|{}", peer.peer_url, service);
            match crawl_one(engine, &peer.peer_url, service, topic, origin, db).await {
                Ok((attempted, admitted, errors, walks)) => {
                    result.walks_could_not_run.extend(walks);
                    result.attempted.insert(key.clone(), attempted);
                    result.admitted_by.insert(key.clone(), admitted);
                    if !errors.is_empty() {
                        result.errors.insert(key, errors);
                    }
                }
                Err(e) => {
                    if !is_lookup_400(&e) || claim_400_log(db, &key).await {
                        worker::console_log!(
                            "[{origin}] peer-crawl: {peer}/{service} lookup failed: {err}",
                            peer = peer.peer_url,
                            service = service,
                            err = e,
                        );
                    }
                    result.peer_errors.insert(key, e);
                }
            }
        }
    }
    result
}

/// What one (peer, service) crawl did: attempted, admitted, the submit errors, and the walks that could not run.
type CrawledOne = (
    usize,
    usize,
    Vec<String>,
    Vec<overlay_engine::engine::WalkCouldNotRun>,
);

/// Crawl one (peer, service) and return `(attempted, admitted, errors, walks)`.
/// Errs only if the lookup itself failed; per-output submit failures are
/// bundled into the `errors` vec and keep the crawl going.
async fn crawl_one(
    engine: &Engine,
    peer_url: &str,
    service: &str,
    topic: &str,
    origin: &str,
    db: Option<&worker::D1Database>,
) -> Result<CrawledOne, String> {
    let mut outputs = Vec::new();
    if service == "ls_uhrp" {
        if let Some(db) = db {
            let seeds: Vec<CrawlSeed> = crate::d1::Query::new(CRAWL_SEEDS_SQL)
                .bind(worker::Date::now().as_millis() / 1000)
                .bind(UHRP_CRAWL_QUERY_LIMIT)
                .fetch_all(db)
                .await?;
            for seed in seeds {
                outputs.extend(fetch_lookup(peer_url, service, Some(&seed.url)).await?);
            }
        }
    } else {
        outputs = fetch_lookup(peer_url, service, None).await?;
    }
    let mut admitted_total: usize = 0;
    let mut errors: Vec<String> = Vec::new();
    let mut walks: Vec<overlay_engine::engine::WalkCouldNotRun> = Vec::new();
    for (i, entry) in outputs.iter().enumerate() {
        let tagged = TaggedBEEF {
            beef: entry.beef.clone(),
            topics: vec![topic.to_string()],
            off_chain_values: None,
        };
        // The E592 lens fold (H1, L3): the report is kept, so a carried predecessor's walk past the budget is
        // counted; the subject's own is "not now" (nothing admitted, counted, crawled again next tick).
        match engine
            .submit_with_report(&tagged, SubmitMode::CurrentTx)
            .await
        {
            Ok((steak, report)) => {
                let admitted: usize = steak.values().map(|a| a.outputs_to_admit.len()).sum();
                admitted_total += admitted;
                if let Some(db) = db.filter(|_| !report.landed_walks_could_not_run.is_empty()) {
                    crate::ops::note_engine_walk(db, &report, "peer-crawl").await;
                }
                walks.extend(report.landed_walks_could_not_run);
            }
            Err(overlay_engine::engine::EngineError::WalkCouldNotRun(stop)) => {
                worker::console_log!(
                    "[{origin}] peer-crawl: {peer}/{service} output[{i}]: the walk of {} could not run ({}): not now, retried next crawl",
                    stop.subject_txid,
                    stop.limb.as_str(),
                    peer = peer_url,
                    service = service,
                );
                if let Some(db) = db {
                    crate::ops::note_engine_walk_not_now(db, &stop, "peer-crawl").await;
                }
                walks.push(stop);
            }
            Err(e) => {
                let msg = format!("output[{i}] submit failed: {e}");
                worker::console_log!(
                    "[{origin}] peer-crawl: {peer}/{service} {msg}",
                    peer = peer_url,
                    service = service,
                );
                errors.push(msg);
            }
        }
    }
    worker::console_log!(
        "[{origin}] peer-crawl: {peer}/{service} attempted={attempted} admitted={admitted} errors={errs}",
        peer = peer_url,
        service = service,
        attempted = outputs.len(),
        admitted = admitted_total,
        errs = errors.len(),
    );
    Ok((outputs.len(), admitted_total, errors, walks))
}

/// POST `{peer_url}/lookup` with a `findAll` query and parse the
/// output-list response. Returns just the output entries — any
/// `free-text-reply` responses (not applicable for our services) are
/// treated as empty.
async fn fetch_lookup(
    peer_url: &str,
    service: &str,
    uhrp_url: Option<&str>,
) -> Result<Vec<OutputEntry>, String> {
    let url = format!("{}/lookup", peer_url.trim_end_matches('/'));

    let body_json = lookup_body(service, uhrp_url);
    let body_str = body_json.to_string();

    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Post);
    let headers = worker::Headers::new();
    let _ = headers.set("Content-Type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(body_str.into()));

    let request =
        worker::Request::new_with_init(&url, &init).map_err(|e| format!("request build: {e}"))?;

    let mut response = worker::Fetch::Request(request)
        .send()
        .await
        .map_err(|e| format!("fetch {url}: {e}"))?;

    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Err(format!(
            "peer {url} returned HTTP {status}; request body shape: {}",
            lookup_body_shape(service)
        ));
    }

    let text = response
        .text()
        .await
        .map_err(|e| format!("read body: {e}"))?;

    parse_output_list(&text)
}

const UHRP_CRAWL_QUERY_LIMIT: u64 = 16;
const CRAWL_SEEDS_SQL: &str = "SELECT DISTINCT uhrpUrl AS url FROM uhrp_records WHERE expiryTime > ?1 ORDER BY RANDOM() LIMIT ?2";
const CLAIM_400_SQL: &str = "INSERT INTO ops_counters (name, value) VALUES (?1, 1) ON CONFLICT(name) DO NOTHING RETURNING value";

#[derive(Deserialize)]
struct CrawlSeed {
    url: String,
}

thread_local! {
    static LOGGED_400: std::cell::RefCell<std::collections::HashSet<String>> = std::cell::RefCell::default();
}

pub(crate) fn is_lookup_400(error: &str) -> bool {
    error.contains("returned HTTP 400; request body shape:")
}

async fn claim_400_log(db: Option<&worker::D1Database>, peer_service: &str) -> bool {
    // One durable claim per configured peer, service and body shape version.
    // The isolate cache also bounds logging when the observation store faults.
    let key = format!(
        "peer_crawl_400_v1_{}",
        hex::encode(bsv_rs::primitives::hash::sha256(peer_service.as_bytes()))
    );
    if !LOGGED_400.with(|seen| seen.borrow_mut().insert(key.clone())) {
        return false;
    }
    let Some(db) = db else {
        return true;
    };
    crate::d1::Query::new(CLAIM_400_SQL)
        .bind(key)
        .fetch_optional::<serde_json::Value>(db)
        .await
        .map_or(true, |claimed| claimed.is_some())
}

fn lookup_body_shape(service: &str) -> &'static str {
    if service == "ls_uhrp" {
        "{service:string,query:{uhrpUrl:bare-string}}"
    } else {
        "{service:string,query:{findAll:boolean}}"
    }
}

fn lookup_body(service: &str, uhrp_url: Option<&str>) -> serde_json::Value {
    let query = match uhrp_url {
        Some(url) => {
            let bare = url
                .strip_prefix("uhrp://")
                .or_else(|| url.strip_prefix("uhrp:"))
                .unwrap_or(url);
            serde_json::json!({"uhrpUrl": bare})
        }
        None => serde_json::json!({"findAll": true}),
    };
    serde_json::json!({"service": service, "query": query})
}

#[test]
fn copy_crawl_body_uses_bare_uhrp_selector() {
    assert_eq!(
        lookup_body("ls_uhrp", Some("uhrp://fixture")),
        serde_json::json!({"service": "ls_uhrp", "query": {"uhrpUrl": "fixture"}})
    );
}

/// Pure parser for the peer's `/lookup` JSON response. Extracted for
/// unit-testability — avoids a network roundtrip in tests.
pub(crate) fn parse_output_list(body: &str) -> Result<Vec<OutputEntry>, String> {
    let parsed: OutputListResponse = serde_json::from_str(body)
        .map_err(|e| format!("parse output-list: {e}; body preview: {}", preview(body)))?;
    if parsed.kind != "output-list" {
        // Not all peers respond with output-list; some return
        // free-text-reply or an error object. Treat non-output-list
        // as "nothing to ingest" rather than failing — matches the
        // engine's lookup_service trait contract.
        return Ok(Vec::new());
    }
    Ok(parsed.outputs)
}

fn preview(s: &str) -> String {
    const N: usize = 200;
    if s.len() <= N {
        s.to_string()
    } else {
        format!("{}…", &s[..N])
    }
}

// ============================================================================
// Tests — wasm-free, no worker::Fetch
// ============================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn copy_crawl_sql_samples_live_content_and_claims_one_400_log() {
        use rusqlite::OptionalExtension;
        let db = rusqlite::Connection::open_in_memory().unwrap();
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            let _ = db.execute_batch(sql);
        }
        for i in 0..24 {
            db.execute("INSERT INTO uhrp_records (txid, outputIndex, uhrpUrl, identityKey, downloadUrl, expiryTime, contentLength) VALUES (?1,0,?2,'fixture','https://content.example/item',?3,1)",
                rusqlite::params![format!("tx{i}"), format!("hash{i}"), if i == 0 { 99 } else { 101 }]).unwrap();
        }
        let mut query = db.prepare(CRAWL_SEEDS_SQL).unwrap();
        let rows: Vec<String> = query
            .query_map(rusqlite::params![100, UHRP_CRAWL_QUERY_LIMIT], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(rows.len(), 16);
        assert!(!rows.iter().any(|url| url == "hash0"));
        for (key, expected) in [
            ("peer-a-shape-v1", Some(1)),
            ("peer-a-shape-v1", None),
            ("peer-b-shape-v1", Some(1)),
        ] {
            let claimed: Option<u32> = db
                .query_row(CLAIM_400_SQL, [key], |r| r.get(0))
                .optional()
                .unwrap();
            assert_eq!(claimed, expected);
        }
    }

    #[test]
    fn parses_output_list_with_two_entries() {
        let body = r#"{
            "type": "output-list",
            "outputs": [
                {"beef": [1, 2, 3, 4], "outputIndex": 0},
                {"beef": [9, 8, 7], "outputIndex": 1}
            ]
        }"#;
        let outputs = parse_output_list(body).unwrap();
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].beef, vec![1, 2, 3, 4]);
        assert_eq!(outputs[1].beef, vec![9, 8, 7]);
    }

    #[test]
    fn returns_empty_on_non_output_list_type() {
        // `free-text-reply` is a real LookupAnswer variant we may see
        // from some services. Don't crash; ingest nothing.
        let body = r#"{"type": "free-text-reply", "message": "no results"}"#;
        let outputs = parse_output_list(body).unwrap();
        assert!(outputs.is_empty());
    }

    #[test]
    fn returns_empty_on_missing_outputs_field() {
        let body = r#"{"type": "output-list"}"#;
        let outputs = parse_output_list(body).unwrap();
        assert!(outputs.is_empty());
    }

    #[test]
    fn rejects_invalid_json() {
        let body = r#"{not valid json"#;
        let err = parse_output_list(body).unwrap_err();
        assert!(err.contains("parse output-list"), "err: {err}");
        // preview() included to aid debugging in production logs
        assert!(err.contains("body preview"), "err: {err}");
    }

    #[test]
    fn preview_truncates_long_bodies() {
        let long = "x".repeat(500);
        let p = preview(&long);
        assert!(p.ends_with('…'));
        assert!(p.len() < long.len());
    }

    #[test]
    fn preview_returns_short_bodies_verbatim() {
        let p = preview("short");
        assert_eq!(p, "short");
    }

    #[test]
    fn peer_config_carries_multiple_services() {
        // Shape assertion — catches an accidental type-regression on
        // the config surface without a real crawl.
        let cfg = PeerConfig {
            peer_url: "https://x.example".to_string(),
            service_to_topic: vec![
                ("ls_uhrp".to_string(), "tm_uhrp".to_string()),
                ("ls_ship".to_string(), "tm_ship".to_string()),
                ("ls_slap".to_string(), "tm_slap".to_string()),
            ],
        };
        assert_eq!(cfg.service_to_topic.len(), 3);
    }

    #[test]
    fn crawl_result_keys_by_peer_service_pair() {
        // Lock the key shape — operators grep these in logs. Changing
        // the format is a breaking ops change.
        let key = format!("{}|{}", "https://overlay-us-1.bsvb.tech", "ls_uhrp");
        assert_eq!(key, "https://overlay-us-1.bsvb.tech|ls_uhrp");
    }
}
