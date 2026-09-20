//! bsv-low #497 (2026-09-20): THE EXCHANGE RATE FROM THE APP LAYER.
//!
//! The client used to fetch WhatsOnChain's rate itself through a same-origin Pages proxy with no fallback and no
//! shared cache, and every fleet day logged `[exchangeRate] WoC returned 429` (WoC rate-limits per egress IP; a
//! seat page asked every minute). The rule since 2026-08: the browser never calls a courier. The rate is a display
//! convenience, so a stale value beats a failed read, and there are several sources.
//!
//! ONE first-party `GET /rate` answers `{ usd, ageMs, source, at, stale, faulted }`: `usd` is USD per BSV from the
//! newest sample this isolate holds, `null` only when no sample was ever read or the sample is older than
//! [`UNAVAILABLE_AFTER_MS`] and every rung faulted. The client hides its USD hints while `usd` is null.
//!
//! THE LADDER, in the three-courier discipline (the owner's ruling 2026-09-04): a rotating start, a rung that faulted
//! [`RUNG_FAULTS_PER_TICK`] times in the current minute is skipped for the rest of it (counted), the first clean
//! answer wins, every call counted by the courier census (`courier::note`, caller `"rate"`). The rungs: WhatsOnChain,
//! CoinGecko, CoinPaprika, Gate.io (a USDT quote, the closest a spot exchange gives), CoinMarketCap (keyed:
//! `CMC_API_KEY`; skipped unkeyed). BananaBlocks and Bitails serve no rate (probed 2026-09-20: 404 / no such host).
//!
//! THE CACHE: one sample per isolate. Under [`FRESH_MS`] it is served as is; up to [`STALE_WHILE_REVALIDATE_MS`] it
//! is served stale while ONE refresh runs behind the answer (`wait_until`); past that the refresh runs inline. The
//! answer carries `Cache-Control: public, max-age=30`, so the edge shares one sample across isolates for half a
//! minute. `/health.rate` shows the sample, its age, its source and every rung's ok / fault / rate-limited / skipped
//! counts (Rule 13: surface, never consume).
use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Value};

/// A sample younger than this is served as is.
pub const FRESH_MS: f64 = 60_000.0;
/// A sample younger than this is served stale while one refresh runs behind the answer.
pub const STALE_WHILE_REVALIDATE_MS: f64 = 10.0 * 60_000.0;
/// A sample older than this serves `usd: null` when the refresh that ran for it faulted.
pub const UNAVAILABLE_AFTER_MS: f64 = 60.0 * 60_000.0;
/// The rung-fault tick: a rung that faulted [`RUNG_FAULTS_PER_TICK`] times inside one tick sits out the rest of it.
pub const TICK_MS: f64 = 60_000.0;
pub const RUNG_FAULTS_PER_TICK: u32 = 3;
/// The edge cache on the answer.
pub const CACHE_CONTROL_SECS: u32 = 30;
/// The rungs, in the ladder's index order. `cmc` runs only with `CMC_API_KEY` installed.
pub const RUNGS: [&str; 5] = ["woc", "coingecko", "coinpaprika", "gateio", "cmc"];
pub const CMC_KEY_HEADER: &str = "X-CMC_PRO_API_KEY";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub usd: f64,
    pub at_ms: f64,
    pub source: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Serve {
    /// the sample is fresh: serve it
    Fresh,
    /// the sample is stale: serve it, refresh behind the answer
    StaleRevalidate,
    /// no sample, or one too old to serve unrefreshed: refresh inline
    RefreshInline,
}

/// PURE: what the route does with the sample it holds at `now`.
pub fn serve_decision(sample_at_ms: Option<f64>, now_ms: f64) -> Serve {
    match sample_at_ms {
        None => Serve::RefreshInline,
        Some(at) => {
            let age = now_ms - at;
            if age < FRESH_MS {
                Serve::Fresh
            } else if age < STALE_WHILE_REVALIDATE_MS {
                Serve::StaleRevalidate
            } else {
                Serve::RefreshInline
            }
        }
    }
}

/// PURE: the ladder's order for one attempt — a rotating start over the enabled rungs.
pub fn ladder_order(start: usize, enabled: &[bool]) -> Vec<usize> {
    let n = enabled.len();
    if n == 0 {
        return Vec::new();
    }
    (0..n)
        .map(|i| (start + i) % n)
        .filter(|&i| enabled[i])
        .collect()
}

/// PURE: the tick a millisecond stamp falls in.
pub fn tick_index(now_ms: f64) -> u64 {
    (now_ms / TICK_MS).max(0.0) as u64
}

/// PURE: a rung that faulted this many times in the current tick sits out the rest of it.
pub fn rung_skipped(faults_in_tick: u32) -> bool {
    faults_in_tick >= RUNG_FAULTS_PER_TICK
}

/// PURE: the rung's URL.
pub fn rung_url(rung: &str) -> Option<&'static str> {
    match rung {
        "woc" => Some("https://api.whatsonchain.com/v1/bsv/main/exchangerate"),
        "coingecko" => Some("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin-cash-sv&vs_currencies=usd"),
        "coinpaprika" => Some("https://api.coinpaprika.com/v1/tickers/bsv-bitcoin-sv?quotes=USD"),
        "gateio" => Some("https://api.gateio.ws/api/v4/spot/tickers?currency_pair=BSV_USDT"),
        "cmc" => Some("https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?symbol=BSV&convert=USD"),
        _ => None,
    }
}

/// A rate the felt may show: finite, positive, and not absurd (a courier's typo never becomes a USD hint).
fn usd_sane(v: f64) -> bool {
    v.is_finite() && v > 0.01 && v < 1_000_000.0
}

/// PURE: the USD rate in one rung's answer body (each provider's own shape), or `None` for an unreadable body.
pub fn parse_rate(rung: &str, body: &[u8]) -> Option<f64> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let usd = match rung {
        "woc" => v.get("rate")?.as_f64()?,
        "coingecko" => v.get("bitcoin-cash-sv")?.get("usd")?.as_f64()?,
        "coinpaprika" => v.get("quotes")?.get("USD")?.get("price")?.as_f64()?,
        "gateio" => v
            .as_array()?
            .first()?
            .get("last")?
            .as_str()?
            .trim()
            .parse::<f64>()
            .ok()?,
        "cmc" => v
            .get("data")?
            .get("BSV")?
            .as_array()?
            .first()?
            .get("quote")?
            .get("USD")?
            .get("price")?
            .as_f64()?,
        _ => return None,
    };
    usd_sane(usd).then_some(usd)
}

/// PURE: the answer body. A sample older than [`UNAVAILABLE_AFTER_MS`] carries `usd: null` (the client hides
/// the hint); `faulted` says the refresh that ran for this answer found no rung.
pub fn body_json(sample: Option<Sample>, now_ms: f64, faulted: bool) -> Value {
    match sample {
        Some(s) => {
            let age = (now_ms - s.at_ms).max(0.0);
            let usable = age < UNAVAILABLE_AFTER_MS;
            json!({
                "usd": if usable { json!(s.usd) } else { Value::Null },
                "ageMs": age as u64,
                "source": s.source,
                "at": s.at_ms as u64,
                "stale": age >= FRESH_MS,
                "faulted": faulted,
            })
        }
        None => {
            json!({ "usd": null, "ageMs": null, "source": null, "at": null, "stale": true, "faulted": faulted })
        }
    }
}

thread_local! {
    static SAMPLE: RefCell<Option<Sample>> = const { RefCell::new(None) };
    static REVALIDATING: Cell<bool> = const { Cell::new(false) };
    static ROTATION: Cell<usize> = const { Cell::new(0) };
    /// (the tick index, the faults per rung inside it)
    static TICK: RefCell<(u64, [u32; 5])> = const { RefCell::new((0, [0; 5])) };
}

const fn z() -> AtomicU64 {
    AtomicU64::new(0)
}
static RUNG_OK: [AtomicU64; 5] = [z(), z(), z(), z(), z()];
static RUNG_FAULT: [AtomicU64; 5] = [z(), z(), z(), z(), z()];
static RUNG_RATELIMITED: [AtomicU64; 5] = [z(), z(), z(), z(), z()];
static RUNG_SKIPPED: [AtomicU64; 5] = [z(), z(), z(), z(), z()];
static REFRESHES: AtomicU64 = z();
/// served fresh / stale / unavailable (usd null)
static SERVED: [AtomicU64; 3] = [z(), z(), z()];

fn faults_in_tick(now_ms: f64, i: usize) -> u32 {
    let t = tick_index(now_ms);
    TICK.with(|c| {
        let mut g = c.borrow_mut();
        if g.0 != t {
            *g = (t, [0; 5]);
        }
        g.1[i]
    })
}

fn note_fault(now_ms: f64, i: usize) {
    let t = tick_index(now_ms);
    TICK.with(|c| {
        let mut g = c.borrow_mut();
        if g.0 != t {
            *g = (t, [0; 5]);
        }
        g.1[i] = g.1[i].saturating_add(1);
    });
    RUNG_FAULT[i].fetch_add(1, Ordering::Relaxed);
}

/// The sample this isolate holds.
pub fn held() -> Option<Sample> {
    SAMPLE.with(|c| *c.borrow())
}

/// Walk the ladder once from the rotating start; the first clean answer becomes the held sample.
/// `cmc_key`: the CoinMarketCap key when installed (its rung is skipped otherwise).
pub(crate) async fn refresh(cmc_key: Option<&str>) -> Option<Sample> {
    let now = worker::Date::now().as_millis() as f64;
    REFRESHES.fetch_add(1, Ordering::Relaxed);
    let enabled: Vec<bool> = RUNGS
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let keyed = *r != "cmc" || cmc_key.is_some();
            let skipped = rung_skipped(faults_in_tick(now, i));
            if keyed && skipped {
                RUNG_SKIPPED[i].fetch_add(1, Ordering::Relaxed);
            }
            keyed && !skipped
        })
        .collect();
    let start = ROTATION.with(|r| {
        let s = r.get();
        r.set((s + 1) % RUNGS.len());
        s
    });
    for i in ladder_order(start, &enabled) {
        let rung = RUNGS[i];
        let Some(url) = rung_url(rung) else { continue };
        let extra = if rung == "cmc" {
            cmc_key.map(|k| (CMC_KEY_HEADER, k))
        } else {
            None
        };
        match crate::routes::provider_get_with("rate", url, extra).await {
            Some((status, body)) if (200..300).contains(&status) => match parse_rate(rung, &body) {
                Some(usd) => {
                    RUNG_OK[i].fetch_add(1, Ordering::Relaxed);
                    let s = Sample {
                        usd,
                        at_ms: worker::Date::now().as_millis() as f64,
                        source: rung,
                    };
                    SAMPLE.with(|c| *c.borrow_mut() = Some(s));
                    return Some(s);
                }
                None => {
                    worker::console_log!("[rate] {rung} answered {status} with an unreadable body");
                    note_fault(now, i);
                }
            },
            Some((429, _)) => {
                RUNG_RATELIMITED[i].fetch_add(1, Ordering::Relaxed);
                note_fault(now, i);
            }
            Some((status, _)) => {
                worker::console_log!("[rate] {rung} answered {status}");
                note_fault(now, i);
            }
            None => note_fault(now, i),
        }
    }
    worker::console_log!("[rate] every rung faulted or sat out this tick (start {start})");
    None
}

/// `GET /rate`.
pub(crate) async fn serve(
    ctx: &worker::RouteContext<crate::auth::AuthState>,
) -> worker::Result<worker::Response> {
    let now = worker::Date::now().as_millis() as f64;
    let cmc = ctx
        .env
        .secret("CMC_API_KEY")
        .ok()
        .map(|s| s.to_string())
        .filter(|s| !s.trim().is_empty());
    let held_now = held();
    let (sample, faulted) = match serve_decision(held_now.map(|s| s.at_ms), now) {
        Serve::Fresh => {
            SERVED[0].fetch_add(1, Ordering::Relaxed);
            (held_now, false)
        }
        Serve::StaleRevalidate => {
            SERVED[1].fetch_add(1, Ordering::Relaxed);
            if !REVALIDATING.with(|r| r.get()) {
                match ctx.data.wait.clone() {
                    Some(w) => {
                        REVALIDATING.with(|r| r.set(true));
                        w.wait_until(async move {
                            let _ = refresh(cmc.as_deref()).await;
                            REVALIDATING.with(|r| r.set(false));
                        });
                    }
                    None => {
                        // no background context on this door: refresh inline instead of never
                        let _ = refresh(cmc.as_deref()).await;
                    }
                }
            }
            (held(), false)
        }
        Serve::RefreshInline => match refresh(cmc.as_deref()).await {
            Some(s) => {
                SERVED[0].fetch_add(1, Ordering::Relaxed);
                (Some(s), false)
            }
            None => {
                SERVED[2].fetch_add(1, Ordering::Relaxed);
                (held_now, true)
            }
        },
    };
    let body = body_json(sample, worker::Date::now().as_millis() as f64, faulted);
    crate::routes::json_response_cached(body.to_string(), 200, CACHE_CONTROL_SECS)
}

/// `/health.rate`: the sample, its age, and every rung's counts (this isolate's).
pub fn health_json(now_ms: f64) -> Value {
    let s = held();
    let mut rungs = serde_json::Map::new();
    for (i, r) in RUNGS.iter().enumerate() {
        rungs.insert(
            (*r).to_string(),
            json!({
                "ok": RUNG_OK[i].load(Ordering::Relaxed),
                "fault": RUNG_FAULT[i].load(Ordering::Relaxed),
                "ratelimited": RUNG_RATELIMITED[i].load(Ordering::Relaxed),
                "skipped": RUNG_SKIPPED[i].load(Ordering::Relaxed),
            }),
        );
    }
    json!({
        "usd": s.map(|x| x.usd),
        "source": s.map(|x| x.source),
        "ageMs": s.map(|x| (now_ms - x.at_ms).max(0.0) as u64),
        "refreshes": REFRESHES.load(Ordering::Relaxed),
        "served": {
            "fresh": SERVED[0].load(Ordering::Relaxed),
            "stale": SERVED[1].load(Ordering::Relaxed),
            "unavailable": SERVED[2].load(Ordering::Relaxed),
        },
        "rungs": Value::Object(rungs),
        "ladder": RUNGS,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_serve_decision_follows_the_samples_age() {
        assert_eq!(serve_decision(None, 1e12), Serve::RefreshInline);
        assert_eq!(
            serve_decision(Some(1e12), 1e12 + FRESH_MS - 1.0),
            Serve::Fresh
        );
        assert_eq!(
            serve_decision(Some(1e12), 1e12 + FRESH_MS),
            Serve::StaleRevalidate
        );
        assert_eq!(
            serve_decision(Some(1e12), 1e12 + STALE_WHILE_REVALIDATE_MS - 1.0),
            Serve::StaleRevalidate
        );
        assert_eq!(
            serve_decision(Some(1e12), 1e12 + STALE_WHILE_REVALIDATE_MS),
            Serve::RefreshInline
        );
    }

    #[test]
    fn the_ladder_rotates_its_start_and_skips_the_disabled_rungs() {
        assert_eq!(ladder_order(0, &[true; 5]), vec![0, 1, 2, 3, 4]);
        assert_eq!(ladder_order(3, &[true; 5]), vec![3, 4, 0, 1, 2]);
        assert_eq!(
            ladder_order(1, &[true, false, true, true, false]),
            vec![2, 3, 0]
        );
        assert!(ladder_order(0, &[]).is_empty());
        assert!(!rung_skipped(RUNG_FAULTS_PER_TICK - 1));
        assert!(rung_skipped(RUNG_FAULTS_PER_TICK));
        assert_eq!(tick_index(0.0), 0);
        assert_eq!(tick_index(TICK_MS * 7.0 + 1.0), 7);
        assert_eq!(tick_index(-5.0), 0);
        for r in RUNGS {
            assert!(rung_url(r).is_some(), "{r} has a URL");
        }
        assert!(
            rung_url("bananablocks").is_none(),
            "no rate endpoint there (probed 2026-09-20)"
        );
    }

    /// The five providers' REAL bodies as probed on 2026-09-20 14:36Z (CMC's from its documented shape).
    #[test]
    fn every_rungs_body_shape_parses_and_junk_does_not() {
        assert_eq!(
            parse_rate(
                "woc",
                br#"{"rate":16.78,"time":1789914943,"currency":"USD"}"#
            ),
            Some(16.78)
        );
        assert_eq!(
            parse_rate(
                "coingecko",
                br#"{"bitcoin-cash-sv":{"usd":16.77,"last_updated_at":1789914880}}"#
            ),
            Some(16.77)
        );
        assert_eq!(
            parse_rate(
                "coinpaprika",
                br#"{"id":"bsv-bitcoin-sv","symbol":"BSV","quotes":{"USD":{"price":16.754726224254327,"volume_24h":1.0}}}"#
            ),
            Some(16.754726224254327)
        );
        assert_eq!(
            parse_rate(
                "gateio",
                br#"[{"currency_pair":"BSV_USDT","last":"16.78","lowest_ask":"16.78"}]"#
            ),
            Some(16.78)
        );
        assert_eq!(
            parse_rate(
                "cmc",
                br#"{"data":{"BSV":[{"quote":{"USD":{"price":16.8}}}]}}"#
            ),
            Some(16.8)
        );
        // an unreadable body, a wrong shape, a zero, a negative, an absurd value: never a rate
        assert_eq!(parse_rate("woc", b"not json"), None);
        assert_eq!(
            parse_rate("woc", br#"{"bitcoin-cash-sv":{"usd":16.77}}"#),
            None
        );
        assert_eq!(parse_rate("woc", br#"{"rate":0}"#), None);
        assert_eq!(parse_rate("woc", br#"{"rate":-3}"#), None);
        assert_eq!(parse_rate("woc", br#"{"rate":1e9}"#), None);
        assert_eq!(parse_rate("gateio", br#"[{"last":"abc"}]"#), None);
        assert_eq!(parse_rate("gateio", br#"[]"#), None);
        assert_eq!(parse_rate("nope", br#"{"rate":1}"#), None);
    }

    #[test]
    fn the_answer_body_hides_the_rate_past_an_hour_and_says_stale_past_a_minute() {
        let s = Sample {
            usd: 16.5,
            at_ms: 1e12,
            source: "woc",
        };
        let fresh = body_json(Some(s), 1e12 + 1_000.0, false);
        assert_eq!(fresh["usd"], 16.5);
        assert_eq!(fresh["stale"], false);
        assert_eq!(fresh["source"], "woc");
        assert_eq!(fresh["ageMs"], 1000);
        let stale = body_json(Some(s), 1e12 + FRESH_MS + 1.0, false);
        assert_eq!(stale["usd"], 16.5);
        assert_eq!(stale["stale"], true);
        let old = body_json(Some(s), 1e12 + UNAVAILABLE_AFTER_MS, true);
        assert!(
            old["usd"].is_null(),
            "an hour-old sample with a faulted refresh hides the hint"
        );
        assert_eq!(old["faulted"], true);
        let none = body_json(None, 1e12, true);
        assert!(none["usd"].is_null() && none["ageMs"].is_null() && none["source"].is_null());
        assert_eq!(none["stale"], true);
    }

    #[test]
    fn the_health_block_carries_every_rung() {
        let h = health_json(1e12);
        for r in RUNGS {
            assert!(h["rungs"][r]["ok"].is_number(), "{r}");
            assert!(h["rungs"][r]["skipped"].is_number(), "{r}");
        }
        assert_eq!(h["ladder"].as_array().map(|a| a.len()), Some(5));
        assert!(h["served"]["unavailable"].is_number());
    }
}
