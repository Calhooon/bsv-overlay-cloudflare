//! bsv-low #451 slice B (2026-09-17): THE COURIER CENSUS. Every third-party chain read this worker makes
//! (WhatsOnChain, BananaBlocks, Bitails, Arcade) is COUNTED by provider, outcome and the route that asked, and LOGGED
//! as one line naming its caller, so a hand's courier cost is a number read before and after — never a guess:
//!   • the durable tally rides the overlay's `ops_counters` (the same table the beef guard writes; every row is served
//!     on the overlay's `/health/invariants.counters`): `applayer_courier_<provider>_<outcome>_total` and
//!     `applayer_courier_calls_<caller>_total`, accumulated PER ISOLATE and flushed as ONE D1 batch in
//!     `ctx.wait_until` after an answer left — the isolate's first [`COURIER_FLUSH_EXACT_HEAD`] flushes at once (a
//!     fresh isolate's first hand is counted exactly), then only once [`COURIER_FLUSH_AT_CALLS`] calls or
//!     [`COURIER_FLUSH_INTERVAL_MS`] have accumulated (the beef guard's L3 rule: never one write per read, the
//!     D1-CALLBACK-FLOOD class; a recycled isolate's unflushed tail is an under-count, never a wrong answer);
//!   • the isolate's running tally is on this worker's own `/health.couriers` (resets on recycle: a soak surface);
//!   • the worker log carries `[courier] <provider> <outcome> <status> <ms>ms caller=<caller> <url>` per call, so
//!     `scripts/worker-logs.py --grep "[courier]"` over a hand's window IS the per-hand census by caller.
//! Rule 13: surface, don't consume. The owner's question ("how many times did we hit WoC this hand?") is answered by
//! two `/health/invariants` reads around the hand.
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// The providers this worker reads, in the tally's index order.
pub const PROVIDERS: [&str; 5] = ["woc", "bananablocks", "bitails", "arcade", "other"];
/// The outcome classes, in the tally's index order.
pub const OUTCOMES: [&str; 4] = ["ok", "notfound", "ratelimited", "fault"];
/// The routes that ask, in the tally's index order (`"other"` catches a label this list does not know).
pub const CALLERS: [&str; 6] = [
    "spent_any",
    "hops_view",
    "tx_any",
    "tx_any_unconfirmable",
    "owed",
    "other",
];

/// PURE: the provider a courier URL belongs to, by host.
pub fn provider_of(url: &str) -> &'static str {
    let host = url
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if host.ends_with("whatsonchain.com") {
        "woc"
    } else if host.ends_with("bananablocks.com") {
        "bananablocks"
    } else if host.ends_with("bitails.io") {
        "bitails"
    } else if host.ends_with("bsvblockchain.tech") {
        "arcade"
    } else {
        "other"
    }
}

/// PURE: the outcome class of one courier answer (`None` = a transport fault: no status at all).
pub fn outcome_of(status: Option<u16>) -> &'static str {
    match status {
        None => "fault",
        Some(s) if (200..300).contains(&s) => "ok",
        Some(404) | Some(410) => "notfound",
        Some(429) => "ratelimited",
        Some(_) => "fault",
    }
}

/// bsv-low #625 (the observability bar, issue 1): the reader this worker is in the overlay's per-reader courier
/// census, `courier_<reader>_<provider>_<outcome>_total` (the overlay's readers are `door` and `janitor`).
pub const READER: &str = "applayer";
/// The per-reader outcomes: the overlay's four (`ok`, `notfound`, `fault`, `shape`) and this reader's `ratelimited`
/// (a 429 is its own class here, as on the `applayer_courier_*` family; the overlay folds it into `fault`).
pub const SHAPED_OUTCOMES: [&str; 5] = ["ok", "notfound", "ratelimited", "fault", "shape"];

/// PURE: does a 2xx body read as its route's format? A `/hex` route answers hex text, Bitails' `/download/tx/`
/// raw bytes; every other route this worker asks answers JSON (any JSON value: a WoC mempool body with no
/// `confirmations` is a well-formed answer, the #606 shape, never a fault).
pub fn body_reads(url: &str, body: &[u8]) -> bool {
    let path = url.split(['?', '#']).next().unwrap_or("");
    if path.ends_with("/hex") {
        let text = std::str::from_utf8(body)
            .unwrap_or("")
            .trim()
            .trim_matches('"');
        return !text.is_empty() && text.bytes().all(|b| b.is_ascii_hexdigit());
    }
    if path.contains("/download/tx/") {
        return !body.is_empty();
    }
    serde_json::from_slice::<serde_json::Value>(body).is_ok()
}

/// PURE: the per-reader outcome of one courier answer. A transport fault (no status) is `fault`; a 2xx whose body
/// does not read as its route's format is `shape`, apart from a transport fault (the #606 discriminator); a 404 or
/// 410 is `notfound` (Bitails is pruned: its 404 on an unmined subject is an absence of record, never a fault).
pub fn shaped_outcome(url: &str, answer: Option<(u16, &[u8])>) -> &'static str {
    match answer {
        Some((s, body)) if (200..300).contains(&s) => {
            if body_reads(url, body) {
                "ok"
            } else {
                "shape"
            }
        }
        Some((s, _)) => outcome_of(Some(s)),
        None => "fault",
    }
}

/// PURE: the per-reader durable name of one answer.
pub fn shaped_counter_name(provider: &str, outcome: &str) -> String {
    format!("courier_{READER}_{provider}_{outcome}_total")
}

/// Every per-reader name this worker can write (the overlay seeds each at 0).
pub fn shaped_counter_names() -> Vec<String> {
    PROVIDERS
        .iter()
        .flat_map(|p| {
            SHAPED_OUTCOMES
                .iter()
                .map(move |o| shaped_counter_name(p, o))
        })
        .collect()
}

/// PURE: the two durable counter names one call bumps.
pub fn counter_names(provider: &str, outcome: &str, caller: &str) -> [String; 2] {
    [
        format!("applayer_courier_{provider}_{outcome}_total"),
        format!("applayer_courier_calls_{caller}_total"),
    ]
}

/// PURE: fold notes into the deltas one flush writes — the same fold `note` applies per request.
pub fn fold_pending(notes: &[(&str, &str, &str)]) -> BTreeMap<String, u64> {
    let mut map = BTreeMap::new();
    for (provider, outcome, caller) in notes {
        add_pending(&mut map, provider, outcome, caller);
    }
    map
}

fn add_pending(map: &mut BTreeMap<String, u64>, provider: &str, outcome: &str, caller: &str) {
    for name in counter_names(provider, outcome, caller) {
        *map.entry(name).or_insert(0) += 1;
    }
}

fn index_of(list: &[&str], want: &str) -> usize {
    list.iter()
        .position(|x| *x == want)
        .unwrap_or(list.len() - 1)
}

thread_local! {
    /// The isolate's durable deltas since its last flush (taken by the fetch entry when a flush is due).
    static PENDING: RefCell<BTreeMap<String, u64>> = const { RefCell::new(BTreeMap::new()) };
    /// Calls noted since the last flush.
    static PENDING_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Flushes this isolate has made.
    static FLUSHES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// When the last flush went out (ms).
    static LAST_FLUSH_MS: std::cell::Cell<f64> = const { std::cell::Cell::new(0.0) };
}

/// The isolate's first flushes go out at once (its first hand is counted exactly).
pub const COURIER_FLUSH_EXACT_HEAD: u64 = 4;
/// After the exact head: flush once this many calls have accumulated…
pub const COURIER_FLUSH_AT_CALLS: u64 = 64;
/// …or this long has passed since the last flush.
pub const COURIER_FLUSH_INTERVAL_MS: f64 = 60_000.0;

/// PURE: is a flush due (the gate's MEDIUM-2: the beef guard's L3 batching, with an exact head)?
pub fn should_flush(
    pending_calls: u64,
    flushes_so_far: u64,
    now_ms: f64,
    last_flush_ms: f64,
) -> bool {
    if pending_calls == 0 {
        return false;
    }
    flushes_so_far < COURIER_FLUSH_EXACT_HEAD
        || pending_calls >= COURIER_FLUSH_AT_CALLS
        || now_ms - last_flush_ms >= COURIER_FLUSH_INTERVAL_MS
}

const fn z() -> AtomicU64 {
    AtomicU64::new(0)
}
/// The isolate's tally: `[provider][outcome]`.
static TALLY: [[AtomicU64; 4]; 5] = [
    [z(), z(), z(), z()],
    [z(), z(), z(), z()],
    [z(), z(), z(), z()],
    [z(), z(), z(), z()],
    [z(), z(), z(), z()],
];
/// The isolate's calls by caller.
static BY_CALLER: [AtomicU64; 6] = [z(), z(), z(), z(), z(), z()];

/// Semantic outcomes share the existing batched durable counter flush.
pub fn note_presence(name: &str) {
    note_presence_n(name, 1);
}

/// Add `n` to a semantic counter in one borrow (a per-request total, e.g. the
/// rows a `/leaderboard` served past its decoration budget, #685). `n == 0` is
/// a no-op, so the caller need not guard it. Counts as ONE call for the flush
/// gate — it is one request's tally, not `n` courier calls.
pub fn note_presence_n(name: &str, n: u64) {
    if n == 0 {
        return;
    }
    PENDING.with(|p| *p.borrow_mut().entry(name.to_string()).or_insert(0) += n);
    PENDING_CALLS.with(|c| c.set(c.get() + 1));
}

/// Count one courier call: the isolate tally, this request's pending durable deltas, and the log line.
pub fn note(url: &str, caller: &str, answer: Option<(u16, &[u8])>, ms: f64) {
    let (provider, outcome, shaped) = record(url, caller, answer);
    let st = answer
        .map(|(s, _)| s.to_string())
        .unwrap_or_else(|| "-".to_string());
    let shape = if shaped == "shape" { " shape" } else { "" };
    worker::console_log!(
        "[courier] {provider} {outcome}{shape} {st} {ms:.0}ms caller={caller} {url}"
    );
}

/// The counting half of [`note`] (no log line, so a native pin runs it): the isolate tally, the
/// `applayer_courier_*` deltas and, bsv-low #625, the per-reader `courier_applayer_<provider>_<outcome>_total`.
/// Answers (provider, the `applayer_courier_*` outcome, the per-reader outcome).
pub fn record(
    url: &str,
    caller: &str,
    answer: Option<(u16, &[u8])>,
) -> (&'static str, &'static str, &'static str) {
    let provider = provider_of(url);
    let outcome = outcome_of(answer.map(|(s, _)| s));
    let shaped = shaped_outcome(url, answer);
    TALLY[index_of(&PROVIDERS, provider)][index_of(&OUTCOMES, outcome)]
        .fetch_add(1, Ordering::Relaxed);
    BY_CALLER[index_of(&CALLERS, caller)].fetch_add(1, Ordering::Relaxed);
    PENDING.with(|p| {
        let mut p = p.borrow_mut();
        add_pending(&mut p, provider, outcome, caller);
        *p.entry(shaped_counter_name(provider, shaped)).or_insert(0) += 1;
    });
    PENDING_CALLS.with(|c| c.set(c.get() + 1));
    (provider, outcome, shaped)
}

/// Take the isolate's pending durable deltas when a flush is due (`should_flush`), else nothing.
pub fn take_pending_if_due(now_ms: f64) -> Vec<(String, u64)> {
    let due = should_flush(
        PENDING_CALLS.with(std::cell::Cell::get),
        FLUSHES.with(std::cell::Cell::get),
        now_ms,
        LAST_FLUSH_MS.with(std::cell::Cell::get),
    );
    if !due {
        return Vec::new();
    }
    FLUSHES.with(|f| f.set(f.get() + 1));
    LAST_FLUSH_MS.with(|t| t.set(now_ms));
    PENDING_CALLS.with(|c| c.set(0));
    PENDING
        .with(|p| std::mem::take(&mut *p.borrow_mut()))
        .into_iter()
        .collect()
}

/// E649 L4 N9: take the pending deltas when a flush is due and hand them to `flush`, both INSIDE the returned
/// future, so a caller that races it against a deadline and never polls it (the deadline fell first) leaves the
/// deltas queued for the next take. Taken outside the race, an expired drain emptied the map and dropped the vector.
pub async fn take_and_flush<F, Fut>(now_ms: f64, flush: F)
where
    F: FnOnce(Vec<(String, u64)>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let pending = take_pending_if_due(now_ms);
    if !pending.is_empty() {
        flush(pending).await;
    }
}

/// Flush the deltas into the overlay's `ops_counters`: one prepared upsert per name, ONE D1 batch. Off the critical
/// path (the caller runs it under `wait_until`); a failure is logged, never surfaced — a lost delta is a census
/// under-count, not a wrong answer.
pub async fn flush(db: Option<worker::D1Database>, pending: Vec<(String, u64)>) {
    let Some(db) = db else { return };
    let mut stmts = Vec::with_capacity(pending.len());
    for (name, delta) in &pending {
        match db.prepare(crate::beef_guard::BUMP_COUNTER_SQL).bind(&[
            worker::wasm_bindgen::JsValue::from_str(name),
            worker::wasm_bindgen::JsValue::from_f64(*delta as f64),
        ]) {
            Ok(s) => stmts.push(s),
            Err(e) => worker::console_warn!("[courier] counter {name} bind failed: {e}"),
        }
    }
    if stmts.is_empty() {
        return;
    }
    if let Err(e) = crate::d1_ledger::counted_batch(&db, stmts).await {
        worker::console_warn!(
            "[courier] counter flush failed ({} rows): {e}",
            pending.len()
        );
    }
}

/// The isolate's running tally for `/health.couriers`.
pub fn health_json() -> serde_json::Value {
    let mut by_provider = serde_json::Map::new();
    for (pi, provider) in PROVIDERS.iter().enumerate() {
        let mut row = serde_json::Map::new();
        for (oi, outcome) in OUTCOMES.iter().enumerate() {
            row.insert(
                (*outcome).to_string(),
                serde_json::json!(TALLY[pi][oi].load(Ordering::Relaxed)),
            );
        }
        by_provider.insert((*provider).to_string(), serde_json::Value::Object(row));
    }
    let mut by_caller = serde_json::Map::new();
    for (ci, caller) in CALLERS.iter().enumerate() {
        by_caller.insert(
            (*caller).to_string(),
            serde_json::json!(BY_CALLER[ci].load(Ordering::Relaxed)),
        );
    }
    serde_json::json!({
        "scope": "isolate",
        "byProvider": serde_json::Value::Object(by_provider),
        "byCaller": serde_json::Value::Object(by_caller),
        "durable": "the overlay's /health/invariants.counters applayer_courier_*",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_is_read_off_the_host() {
        assert_eq!(
            provider_of("https://api.whatsonchain.com/v1/bsv/main/tx/hash/ab"),
            "woc"
        );
        assert_eq!(
            provider_of("https://bananablocks.com/api/v1/txo/ab/0/spend"),
            "bananablocks"
        );
        assert_eq!(
            provider_of("https://api.bitails.io/download/tx/ab"),
            "bitails"
        );
        assert_eq!(
            provider_of("https://arcade-v2-us-1.bsvblockchain.tech/tx/ab"),
            "arcade"
        );
        assert_eq!(provider_of("https://example.com/x"), "other");
        assert_eq!(
            provider_of("https://bsvarcade.com/tx/ab"),
            "other",
            "our own site is not the Arcade courier (LOW-3)"
        );
    }

    #[test]
    fn outcome_classes() {
        assert_eq!(outcome_of(Some(200)), "ok");
        assert_eq!(outcome_of(Some(204)), "ok");
        assert_eq!(outcome_of(Some(404)), "notfound");
        assert_eq!(outcome_of(Some(410)), "notfound");
        assert_eq!(outcome_of(Some(429)), "ratelimited");
        assert_eq!(outcome_of(Some(500)), "fault");
        assert_eq!(outcome_of(Some(403)), "fault");
        assert_eq!(outcome_of(None), "fault");
    }

    #[test]
    fn a_request_folds_its_calls_into_named_deltas() {
        let folded = fold_pending(&[
            ("woc", "ok", "spent_any"),
            ("woc", "ok", "spent_any"),
            ("bananablocks", "notfound", "spent_any"),
            ("bitails", "fault", "tx_any"),
        ]);
        let want: BTreeMap<String, u64> = [
            ("applayer_courier_woc_ok_total", 2),
            ("applayer_courier_bananablocks_notfound_total", 1),
            ("applayer_courier_bitails_fault_total", 1),
            ("applayer_courier_calls_spent_any_total", 3),
            ("applayer_courier_calls_tx_any_total", 1),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(folded, want, "judged {folded:?}");
        assert!(fold_pending(&[]).is_empty());
    }

    /// the gate's MEDIUM-2: an exact head, then batched by calls or by time; nothing pending flushes nothing.
    #[test]
    fn flushes_exactly_at_first_then_by_calls_or_time() {
        assert!(
            !should_flush(0, 0, 1_000.0, 0.0),
            "nothing pending: no write"
        );
        assert!(
            should_flush(1, 0, 1_000.0, 0.0),
            "the exact head flushes one call at once"
        );
        assert!(should_flush(1, COURIER_FLUSH_EXACT_HEAD - 1, 1_000.0, 0.0));
        assert!(
            !should_flush(1, COURIER_FLUSH_EXACT_HEAD, 1_000.0, 1_000.0),
            "past the head: one call waits"
        );
        assert!(
            !should_flush(COURIER_FLUSH_AT_CALLS - 1, 10, 30_000.0, 0.0),
            "63 calls inside the minute wait"
        );
        assert!(
            should_flush(COURIER_FLUSH_AT_CALLS, 10, 30_000.0, 0.0),
            "64 calls flush"
        );
        assert!(
            should_flush(1, 10, 61_000.0, 1_000.0),
            "a minute since the last flush flushes one call"
        );
        assert!(!should_flush(1, 10, 60_999.0, 1_000.0));
    }

    #[test]
    fn an_unknown_caller_lands_in_other_never_out_of_bounds() {
        assert_eq!(index_of(&CALLERS, "nope"), CALLERS.len() - 1);
        assert_eq!(index_of(&PROVIDERS, "other"), PROVIDERS.len() - 1);
        assert_eq!(index_of(&CALLERS, "hops_view"), 1);
    }

    /// `note_presence_n` adds the whole per-request total in one borrow and
    /// counts as ONE call; `n == 0` is a no-op (so the #685 budget-hit caller
    /// need not guard it). Runs on this test's own thread, so PENDING is fresh.
    #[test]
    fn note_presence_n_adds_the_total_and_zero_is_a_no_op() {
        note_presence_n("leaderboard_decoration_budget_hit_total", 3);
        note_presence_n("leaderboard_decoration_budget_hit_total", 2);
        note_presence_n("ignored_total", 0);
        let drained = take_pending_if_due(1_000.0);
        let got: BTreeMap<String, u64> = drained.into_iter().collect();
        assert_eq!(
            got.get("leaderboard_decoration_budget_hit_total").copied(),
            Some(5),
            "both bumps fold into the one durable delta"
        );
        assert!(
            !got.contains_key("ignored_total"),
            "a zero bump writes nothing"
        );
    }
}
