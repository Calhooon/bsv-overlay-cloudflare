//! bsv-low #499: the D1 ROWS LEDGER of one request (rows read, rows written, statements).
//!
//! Every D1 statement answers `meta.rows_read` / `meta.rows_written`; this module sums them per request so a
//! route's D1 cost is a number on its answer (`Server-Timing: d1;desc="reads=N writes=M stmts=K"`) and on the
//! health surface (`d1Budget`: the per-route running maxima since the isolate booted). Three D1 incidents in three
//! weeks (the callback flood, the 965,918-row header select, the t=0 burst) were each a per-request read or write
//! count nobody saw until D1 refused; the ceilings in `tools/lane-499` red the change that would have caused one.
//!
//! THE SCOPE. The counter is request-scoped without threading a handle through every function that holds a
//! `D1Database`: [`Scoped`] wraps the request's future and installs its tally as the CURRENT one around each
//! `poll` (a task-local, the way tokio's `task_local!` scopes one). A Worker isolate serves several requests at
//! once, but wasm is single-threaded and a statement's meta is read inside the poll of the future that awaited it,
//! so each statement lands on the request that issued it. Work handed to `wait_until` is polled outside any scope
//! and lands on `unscoped` (the after-answer refreshes, the courier flush): it is the isolate's, not the answer's.
//!
//! THE SEAM. Both workers count at ONE place each: the overlay's `d1::Query` (every engine and discovery
//! statement) and the app layer's [`Counted`] calls (every `prepare(..)` it awaits). A statement awaited through
//! the bare `worker` API is not counted: the doctrine of the courier census ("count a courier read or it did not
//! happen") applied to D1, so a new bare call is the hole a ceiling cannot see.
//!
//! This file is the overlay's; `low-app-layer` compiles the same source (`#[path]`), it does not link the overlay.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

/// What one request (or the unscoped remainder) cost D1.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    /// Rows read.
    pub reads: u64,
    /// Rows written.
    pub writes: u64,
    /// Statements run.
    pub stmts: u64,
    /// Largest single statement, so a fleet scan cannot hide in a request total.
    pub max_reads: u64,
}

impl Tally {
    /// One statement's meta folded in (a statement with no meta still counts as a statement).
    pub fn add(&mut self, reads: u64, writes: u64) {
        self.reads = self.reads.saturating_add(reads);
        self.max_reads = self.max_reads.max(reads);
        self.writes = self.writes.saturating_add(writes);
        self.stmts = self.stmts.saturating_add(1);
    }
}

/// One route's figures since the isolate booted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RouteBudget {
    /// Requests recorded.
    pub requests: u64,
    /// Each field its own running maximum (the request that read the most need not be the one that wrote the most).
    pub max: Tally,
    /// The last request's figures.
    pub last: Tally,
    /// The sums over every request (`max_reads` its maximum).
    pub total: Tally,
}

impl RouteBudget {
    /// Fold one request's tally in.
    pub fn record(&mut self, t: Tally) {
        self.requests = self.requests.saturating_add(1);
        self.max.reads = self.max.reads.max(t.reads);
        self.max.max_reads = self.max.max_reads.max(t.max_reads);
        self.max.writes = self.max.writes.max(t.writes);
        self.max.stmts = self.max.stmts.max(t.stmts);
        self.last = t;
        self.total.reads = self.total.reads.saturating_add(t.reads);
        self.total.max_reads = self.total.max_reads.max(t.max_reads);
        self.total.writes = self.total.writes.saturating_add(t.writes);
        self.total.stmts = self.total.stmts.saturating_add(t.stmts);
    }
}

thread_local! {
    /// NS-COST-BAR-1: the pass ledger of the [`Passes`] future being polled, if any (see [`passes`]).
    static PASS: RefCell<Option<Rc<RefCell<PassLedger>>>> = const { RefCell::new(None) };
    static CURRENT: RefCell<Option<Rc<Cell<Tally>>>> = const { RefCell::new(None) };
    static UNSCOPED: Cell<Tally> = const { Cell::new(Tally { reads: 0, writes: 0, stmts: 0, max_reads: 0 }) };
    static ROUTES: RefCell<BTreeMap<&'static str, RouteBudget>> = const { RefCell::new(BTreeMap::new()) };
    /// bsv-low #831 (the FIX-831-B lens's MEDIUM-2): every statement this isolate noted, scoped or not.
    static NOTED: Cell<u64> = const { Cell::new(0) };
}

/// bsv-low #831 (the FIX-831-B lens's MEDIUM-2): the statements this isolate has noted since it booted, whatever
/// scope each landed on. A pass that bounds its own D1 cost reads it before and after its work (the retire pass's
/// statement budget): the difference is its statements, and a concurrent request's in the same isolate, so the
/// bound errs toward stopping early, never toward running past it.
pub fn statements_noted() -> u64 {
    NOTED.with(|n| n.get())
}

/// Fold one statement into the request in scope, or into `unscoped` when none is (and, NS-COST-BAR-1, into the
/// pass in scope, if any: the two are independent, so a pass inside a request counts on both).
pub fn note(reads: u64, writes: u64) {
    NOTED.with(|n| n.set(n.get().saturating_add(1)));
    note_cost(reads, writes);
    if let Some(p) = PASS.with(|c| c.borrow().clone()) {
        let mut p = p.borrow_mut();
        let label = p.label;
        p.by_pass.entry(label).or_default().add(reads, writes);
    }
    let scoped = CURRENT.with(|c| c.borrow().clone());
    match scoped {
        Some(t) => {
            let mut v = t.get();
            v.add(reads, writes);
            t.set(v);
        }
        None => UNSCOPED.with(|u| {
            let mut v = u.get();
            v.add(reads, writes);
            u.set(v);
        }),
    }
}

/// Fold a statement's `meta` (absent fields count 0; the statement still counts).
pub fn note_meta(meta: Option<&worker::D1ResultMeta>) {
    let reads = meta.and_then(|m| m.rows_read).unwrap_or(0) as u64;
    let writes = meta.and_then(|m| m.rows_written).unwrap_or(0) as u64;
    note(reads, writes);
}

/// A future whose D1 statements land on its own tally (see the module doc).
pub struct Scoped<F> {
    tally: Rc<Cell<Tally>>,
    fut: Pin<Box<F>>,
}

impl<F: Future> Future for Scoped<F> {
    type Output = (F::Output, Tally);
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mine = self.tally.clone();
        let prev = CURRENT.with(|c| c.borrow_mut().replace(mine));
        let out = self.fut.as_mut().poll(cx);
        CURRENT.with(|c| *c.borrow_mut() = prev);
        match out {
            Poll::Ready(v) => Poll::Ready((v, self.tally.get())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Run `fut` under a fresh tally; resolves to its output and what it cost D1.
pub fn scoped<F: Future>(fut: F) -> Scoped<F> {
    Scoped {
        tally: Rc::new(Cell::new(Tally::default())),
        fut: Box::pin(fut),
    }
}

/// NS-COST-BAR-1: what one run of a pass family (one maintenance tick's steps) cost D1, by the pass that issued
/// each statement. The labels are a closed set chosen by the caller ([`label`]), so the map cannot grow from input.
#[derive(Debug, Default)]
pub struct PassLedger {
    label: &'static str,
    by_pass: BTreeMap<&'static str, Tally>,
}

/// NS-COST-BAR-1: name the pass the next statements of the [`Passes`] future in scope belong to (a no-op outside
/// one). Called between a tick's steps: they run one after another inside one future, so each statement lands on the
/// step that issued it. Work a step hands to `wait_until` is polled outside the scope and is not a pass's.
pub fn label(name: &'static str) {
    if let Some(p) = PASS.with(|c| c.borrow().clone()) {
        p.borrow_mut().label = name;
    }
}

/// A future whose D1 statements are summed per [`label`] (see [`passes`]).
pub struct Passes<F> {
    ledger: Rc<RefCell<PassLedger>>,
    fut: Pin<Box<F>>,
}

impl<F: Future> Future for Passes<F> {
    type Output = (F::Output, BTreeMap<&'static str, Tally>);
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mine = self.ledger.clone();
        let prev = PASS.with(|c| c.borrow_mut().replace(mine));
        let out = self.fut.as_mut().poll(cx);
        PASS.with(|c| *c.borrow_mut() = prev);
        match out {
            Poll::Ready(v) => {
                Poll::Ready((v, std::mem::take(&mut self.ledger.borrow_mut().by_pass)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// NS-COST-BAR-1: run `fut` with its statements summed per pass, starting under `first`; resolves to its output and
/// the per-pass tallies. Independent of the request's tally ([`scoped`]): both count every statement.
pub fn passes<F: Future>(first: &'static str, fut: F) -> Passes<F> {
    Passes {
        ledger: Rc::new(RefCell::new(PassLedger {
            label: first,
            by_pass: BTreeMap::new(),
        })),
        fut: Box::pin(fut),
    }
}

/// NS-COST-BAR-1: the tally of the request in scope so far (`None` outside one): what a handler has cost D1 up to
/// the call, for a handler that records its own figure durably (`/health/invariants`).
pub fn current() -> Option<Tally> {
    CURRENT.with(|c| c.borrow().as_ref().map(|t| t.get()))
}

/// The ledger's key for a path: the known route it names (exact, or the route followed by `/` for a route with a
/// path parameter), else `"other"`. A closed set, so a stranger's paths cannot grow the map.
pub fn route_of(path: &str, routes: &[&'static str]) -> &'static str {
    for r in routes {
        if path == *r {
            return r;
        }
        if let Some(rest) = path.strip_prefix(r) {
            if rest.starts_with('/') {
                return r;
            }
        }
    }
    "other"
}

/// Record one finished request under its route.
pub fn record(route: &'static str, t: Tally) {
    ROUTES.with(|m| m.borrow_mut().entry(route).or_default().record(t));
}

/// bsv-low #499 (lens fold L2): the key of the once-per-isolate schema work (the overlay's migrations, the app
/// layer's latch columns). That work runs inside the FIRST request a fresh isolate serves; keyed apart, it no longer
/// inflates whichever route happened to wake the isolate (the fixture's `/listTopicManagers` read 5909 rows by it).
pub const BOOT_KEY: &str = "(boot)";

/// Run the once-per-isolate schema work `fut` under its own ledger, recorded under [`BOOT_KEY`] when it issued a
/// statement (a warm isolate's call issues none and records nothing).
pub async fn boot<F: Future>(fut: F) -> F::Output {
    let (out, t) = scoped(fut).await;
    if t.stmts > 0 {
        record(BOOT_KEY, t);
    }
    out
}

/// bsv-low #499 (lens fold L5): the answer to a handler error that escaped the fetch body, built HERE so it carries
/// the request's figures. It is the runtime's own answer (`#[event(fetch)]` logs the error and answers
/// `INTERNAL SERVER ERROR` with 500), so status and body are what they were; only the `Server-Timing` is added.
pub fn escaped(e: &worker::Error, t: Tally) -> worker::Result<worker::Response> {
    worker::console_error!("{e}");
    let mut resp = worker::Response::error("INTERNAL SERVER ERROR", 500)?;
    stamp(&mut resp, t);
    Ok(resp)
}

/// The `Server-Timing` segment of one request.
pub fn server_timing_segment(t: Tally) -> String {
    named_segment("d1", t)
}

/// A segment of the same shape under another name (the app layer's view actor serves its compute's figures as
/// `d1view`: that compute is detached from the request that reads its copy).
pub fn named_segment(name: &str, t: Tally) -> String {
    format!(
        "{name};desc=\"reads={} writes={} stmts={} maxReads={}\"",
        t.reads, t.writes, t.stmts, t.max_reads
    )
}

/// PURE: the figures of the segment `name` in a `Server-Timing` value, if it carries one of our shape.
pub fn parse_segment(header: &str, name: &str) -> Option<Tally> {
    for seg in header.split(',') {
        let seg = seg.trim();
        let Some(rest) = seg.strip_prefix(name) else {
            continue;
        };
        let Some(desc) = rest.trim_start().strip_prefix(";desc=\"") else {
            continue;
        };
        let desc = desc.strip_suffix('"')?;
        let mut t = Tally::default();
        let mut seen = 0;
        for kv in desc.split_whitespace() {
            let (k, v) = kv.split_once('=')?;
            let v: u64 = v.parse().ok()?;
            match k {
                "reads" => t.reads = v,
                "writes" => t.writes = v,
                "stmts" => t.stmts = v,
                "maxReads" => t.max_reads = v,
                _ => return None,
            }
            seen += 1;
        }
        return (seen == 3 || seen == 4).then_some(t);
    }
    None
}

/// The response's `Server-Timing` with the d1 segment appended to whatever the route already set.
pub fn with_d1_segment(existing: Option<&str>, t: Tally) -> String {
    let seg = server_timing_segment(t);
    match existing.map(str::trim).filter(|s| !s.is_empty()) {
        Some(e) => format!("{e}, {seg}"),
        None => seg,
    }
}

/// `Access-Control-Expose-Headers` with `Server-Timing` in it (left as is when it already names it).
pub fn expose_server_timing(existing: Option<&str>) -> String {
    match existing.map(str::trim).filter(|s| !s.is_empty()) {
        Some(e)
            if e.split(',')
                .any(|h| h.trim().eq_ignore_ascii_case("server-timing")) =>
        {
            e.to_string()
        }
        Some(e) => format!("{e}, Server-Timing"),
        None => "Server-Timing".to_string(),
    }
}

/// Stamp the d1 segment (and its CORS expose) on a finished response. Stamped LAST, after any signing or sealing
/// built the response, so the segment rides OUTSIDE the BRC-103 signature and the lane's MAC: a diagnostic, never
/// an attested figure.
pub fn stamp(resp: &mut worker::Response, t: Tally) {
    let h = resp.headers_mut();
    let timing = with_d1_segment(h.get("Server-Timing").ok().flatten().as_deref(), t);
    let _ = h.set("Server-Timing", &timing);
    let expose = expose_server_timing(
        h.get("Access-Control-Expose-Headers")
            .ok()
            .flatten()
            .as_deref(),
    );
    let _ = h.set("Access-Control-Expose-Headers", &expose);
}

fn tally_json(t: Tally) -> serde_json::Value {
    serde_json::json!({ "reads": t.reads, "writes": t.writes, "stmts": t.stmts, "maxReads": t.max_reads })
}

/// PURE: the `d1Budget` body from a snapshot (pinned).
pub fn budget_json_of(
    routes: &BTreeMap<&'static str, RouteBudget>,
    unscoped: Tally,
) -> serde_json::Value {
    let mut by_route = serde_json::Map::new();
    for (r, b) in routes {
        by_route.insert(
            (*r).to_string(),
            serde_json::json!({
                "requests": b.requests,
                "max": tally_json(b.max),
                "last": tally_json(b.last),
                "total": tally_json(b.total),
            }),
        );
    }
    serde_json::json!({
        "scope": "this isolate since boot (per-route running maxima of one request's D1 meta.rows_read / rows_written / statements)",
        "routes": by_route,
        "unscoped": tally_json(unscoped),
    })
}

/// The `d1Budget` body of this isolate.
pub fn budget_json() -> serde_json::Value {
    let routes = ROUTES.with(|m| m.borrow().clone());
    budget_json_of(&routes, UNSCOPED.with(|u| u.get()))
}

/// The counted forms of a prepared statement's three awaits (the app layer's seam; the overlay's is `d1::Query`).
/// `counted_first` runs the statement with `all()` and takes the first row: D1 executes the whole statement for a
/// `first()` too, so the rows read are the same, and `all()` is the call whose answer carries the meta. Every row
/// it answers is marshalled to wasm, though, so first-shaped SQL keeps its `LIMIT 1` (or a key that holds one row).
#[allow(async_fn_in_trait)]
pub trait Counted {
    /// Run the statement (`all()`), counted.
    async fn counted_all(&self) -> worker::Result<worker::D1Result>;
    /// Run the statement for its effect (`run()`), counted.
    async fn counted_run(&self) -> worker::Result<worker::D1Result>;
    /// Run the statement and take its leading row, counted.
    async fn counted_first<T: serde::de::DeserializeOwned>(&self) -> worker::Result<Option<T>>;
}

impl Counted for worker::D1PreparedStatement {
    async fn counted_all(&self) -> worker::Result<worker::D1Result> {
        overlay_engine::meter::d1_queries(1);
        let r = self.all().await?;
        note_meta(r.meta().ok().flatten().as_ref());
        Ok(r)
    }
    async fn counted_run(&self) -> worker::Result<worker::D1Result> {
        overlay_engine::meter::d1_queries(1);
        let r = self.run().await?;
        note_meta(r.meta().ok().flatten().as_ref());
        Ok(r)
    }
    async fn counted_first<T: serde::de::DeserializeOwned>(&self) -> worker::Result<Option<T>> {
        use worker::js_sys::{Array, Reflect};
        use worker::wasm_bindgen::{JsCast, JsValue};
        overlay_engine::meter::d1_queries(1);
        let js = worker::wasm_bindgen_futures::JsFuture::from(self.inner().all()?).await?;
        let meta = Reflect::get(&js, &JsValue::from_str("meta"))
            .ok()
            .and_then(|m| worker::serde_wasm_bindgen::from_value::<worker::D1ResultMeta>(m).ok());
        note_meta(meta.as_ref());
        let results = Reflect::get(&js, &JsValue::from_str("results"))?;
        let Ok(rows) = results.dyn_into::<Array>() else {
            return Ok(None);
        };
        if rows.length() == 0 {
            return Ok(None);
        }
        Ok(worker::serde_wasm_bindgen::from_value::<Option<T>>(
            rows.get(0),
        )?)
    }
}

/// The counted form of `D1Database::batch` (one statement per member, each with its own meta).
pub async fn counted_batch(
    db: &worker::D1Database,
    stmts: Vec<worker::D1PreparedStatement>,
) -> worker::Result<Vec<worker::D1Result>> {
    overlay_engine::meter::d1_queries(stmts.len() as u64);
    let out = db.batch(stmts).await?;
    for r in &out {
        note_meta(r.meta().ok().flatten().as_ref());
    }
    Ok(out)
}

/// A GASP page's batch (engine 0.2.2): one `D1Database::batch`, one
/// subrequest of the invocation, so the meter counts it as ONE query; its
/// statements are counted by the engine (`SyncCost::storage_statements`).
/// Each member's meta is noted on the ledger as `counted_batch` notes it.
pub async fn counted_page_batch(
    db: &worker::D1Database,
    stmts: Vec<worker::D1PreparedStatement>,
) -> worker::Result<Vec<worker::D1Result>> {
    overlay_engine::meter::d1_queries(1);
    let out = db.batch(stmts).await?;
    for r in &out {
        note_meta(r.meta().ok().flatten().as_ref());
    }
    Ok(out)
}

// bsv-low #635 (B-COST-1): THE COST BAR. What this isolate cost the plan (fetch requests, D1 rows read, rows
// written, statements), summed in a WINDOW and written durably to `worker_cost_daily` (one row per UTC day and
// Worker, an additive upsert), so `/health/invariants.costBar` serves each Worker's day against the plan's included
// quotas. The window is written ONE statement at a time: when it holds `COST_FLUSH_REQUESTS` requests, when
// `COST_FLUSH_MS` have passed since this isolate's last take, or when the UTC day moved; never one write per
// request. Per isolate and minute that is at most `ceil(R / 64) + 1` writes for R requests (the first take of a
// fresh isolate is due at once). The window counts every statement this ledger notes ([`note`]: scoped or not, the
// cron's and the queue's included), and the flush's own write lands in the next window. Stated: an isolate that
// ends with a window unwritten loses it (an UNDER-count, never a wrong answer); a statement awaited through the bare
// `worker` API is not noted (the ledger's seam, above); a window lands on the UTC day of its first request (or, with
// no request, of its take), so one straddling midnight puts at most one window (60 s or 64 requests) on its first day.

/// bsv-low #635: a window is written once it holds this many requests.
pub const COST_FLUSH_REQUESTS: u64 = 64;
/// bsv-low #635: a window is written once this long has passed since the isolate's last take.
pub const COST_FLUSH_MS: f64 = 60_000.0;
const DAY_MS: f64 = 86_400_000.0;

/// bsv-low #635: the daily cost rows, one per UTC day and Worker (`worker` is the crate's package name: the overlay
/// and the app layer share one D1). Both Workers write it through this one source; the overlay creates it.
pub const COST_DAILY_CREATE: &str = "CREATE TABLE IF NOT EXISTS worker_cost_daily (day TEXT NOT NULL, worker TEXT NOT NULL, requests INTEGER NOT NULL DEFAULT 0, rowsRead INTEGER NOT NULL DEFAULT 0, rowsWritten INTEGER NOT NULL DEFAULT 0, statements INTEGER NOT NULL DEFAULT 0, updatedAt INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (day, worker))";

/// bsv-low #635: one window added to its day's row (a key search on the primary key; one row written).
/// bsv-low #911 (D2-ACTOR-A): the queue's family rides the same row (`queueOps`, [`COST_DAILY_QUEUE_ALTER`]).
pub const COST_DAILY_UPSERT_SQL: &str = "INSERT INTO worker_cost_daily (day, worker, requests, rowsRead, rowsWritten, statements, updatedAt, queueOps) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(day, worker) DO UPDATE SET requests = requests + excluded.requests, rowsRead = rowsRead + excluded.rowsRead, rowsWritten = rowsWritten + excluded.rowsWritten, statements = statements + excluded.statements, updatedAt = excluded.updatedAt, queueOps = queueOps + excluded.queueOps";

/// bsv-low #911 (D2-ACTOR-A): the queue operations column of the daily cost row (appended; an old worker's upsert
/// leaves it at its default).
pub const COST_DAILY_QUEUE_ALTER: &str =
    "ALTER TABLE worker_cost_daily ADD COLUMN queueOps INTEGER NOT NULL DEFAULT 0";

/// bsv-low #635: the name this Worker writes its rows under.
pub const COST_WORKER: &str = env!("CARGO_PKG_NAME");

/// bsv-low #635: one window of this isolate's cost, not yet written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CostWindow {
    /// The UTC day (days since the epoch) of the window's first request; `None` when no request opened it.
    pub day: Option<i64>,
    /// Fetch requests.
    pub requests: u64,
    /// D1 rows read.
    pub reads: u64,
    /// D1 rows written.
    pub writes: u64,
    /// D1 statements.
    pub stmts: u64,
    /// bsv-low #911 (D2-ACTOR-A): queue operations (messages sent; the consumer's deliveries are its invocations).
    pub queue_ops: u64,
}

impl CostWindow {
    /// Nothing to write.
    pub fn is_empty(&self) -> bool {
        self.requests == 0 && self.stmts == 0 && self.queue_ops == 0
    }
}

/// bsv-low #635: one window taken for its write, under the UTC day it lands on (`YYYY-MM-DD`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostFlush {
    /// The UTC day, `YYYY-MM-DD`.
    pub day: String,
    /// The window's figures.
    pub window: CostWindow,
}

thread_local! {
    static COST: Cell<CostWindow> = const { Cell::new(CostWindow { day: None, requests: 0, reads: 0, writes: 0, stmts: 0, queue_ops: 0 }) };
    static COST_LAST_TAKE_MS: Cell<f64> = const { Cell::new(f64::NEG_INFINITY) };
}

/// PURE: the UTC day (days since the epoch) of a unix-ms instant.
pub fn utc_day(now_ms: f64) -> i64 {
    (now_ms / DAY_MS).floor() as i64
}

/// PURE: a day since the epoch as `YYYY-MM-DD` (the proleptic Gregorian calendar, H. Hinnant's `civil_from_days`).
pub fn utc_date(day: i64) -> String {
    let z = day + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

/// PURE (bsv-low #635): is the window due for its write?
pub fn cost_due(w: &CostWindow, now_ms: f64, last_take_ms: f64) -> bool {
    if w.is_empty() {
        return false;
    }
    w.requests >= COST_FLUSH_REQUESTS
        || now_ms - last_take_ms >= COST_FLUSH_MS
        || w.day.is_some_and(|d| d != utc_day(now_ms))
}

/// bsv-low #635: fold one statement into the isolate's cost window (called by [`note`]).
fn note_cost(reads: u64, writes: u64) {
    COST.with(|c| {
        let mut w = c.get();
        w.reads = w.reads.saturating_add(reads);
        w.writes = w.writes.saturating_add(writes);
        w.stmts = w.stmts.saturating_add(1);
        c.set(w);
    });
}

/// bsv-low #911 (D2-ACTOR-A): count `n` queue operations (messages sent) in the window.
pub fn note_queue_ops(n: u64) {
    COST.with(|c| {
        let mut w = c.get();
        w.queue_ops = w.queue_ops.saturating_add(n);
        c.set(w);
    });
}

/// bsv-low #635: count one fetch request in the window (it opens the window's day when it is the first).
pub fn note_request(now_ms: f64) {
    COST.with(|c| {
        let mut w = c.get();
        if w.requests == 0 && w.day.is_none() {
            w.day = Some(utc_day(now_ms));
        }
        w.requests = w.requests.saturating_add(1);
        c.set(w);
    });
}

/// Count a read without sampling a clock or scheduling a flush. The next
/// writer or scheduled pass dates an as-yet undated window when taking it.
pub fn note_deferred_request() {
    COST.with(|c| {
        let mut window = c.get();
        window.requests = window.requests.saturating_add(1);
        c.set(window);
    });
}

/// bsv-low #635: take the window when it is due ([`cost_due`]), leaving an empty one; else `None`.
pub fn take_cost_if_due(now_ms: f64) -> Option<CostFlush> {
    let last = COST_LAST_TAKE_MS.with(Cell::get);
    let w = COST.with(Cell::get);
    if !cost_due(&w, now_ms, last) {
        return None;
    }
    COST_LAST_TAKE_MS.with(|t| t.set(now_ms));
    COST.with(|c| c.set(CostWindow::default()));
    Some(CostFlush {
        day: utc_date(w.day.unwrap_or_else(|| utc_day(now_ms))),
        window: w,
    })
}

/// bsv-low #635: the one call at a fetch's door: take the window written so far when it is due, then count this
/// request in the window that follows (so a day's first request opens its own day's window).
pub fn cost_on_request(now_ms: f64) -> Option<CostFlush> {
    let taken = take_cost_if_due(now_ms);
    note_request(now_ms);
    taken
}

/// bsv-low #635: a window whose write faulted goes back into the isolate's window (a later take writes it), so a
/// D1 fault or a table not yet migrated is a delay, not a loss. It joins the current window's day when that has one.
pub fn restore_cost(f: &CostFlush) {
    COST.with(|c| {
        let mut w = c.get();
        w.day = w.day.or(f.window.day);
        w.requests = w.requests.saturating_add(f.window.requests);
        w.reads = w.reads.saturating_add(f.window.reads);
        w.writes = w.writes.saturating_add(f.window.writes);
        w.stmts = w.stmts.saturating_add(f.window.stmts);
        w.queue_ops = w.queue_ops.saturating_add(f.window.queue_ops);
        c.set(w);
    });
}

/// bsv-low #635: the window not yet written (`/health.costWindow`, and the pins).
pub fn cost_window() -> CostWindow {
    COST.with(Cell::get)
}

/// bsv-low #635: write one taken window: ONE statement ([`COST_DAILY_UPSERT_SQL`]), counted like any other. A fault
/// is logged and the window restored ([`restore_cost`]).
pub async fn flush_cost(db: &worker::D1Database, f: CostFlush, now_ms: f64) {
    use worker::wasm_bindgen::JsValue;
    let bound = db.prepare(COST_DAILY_UPSERT_SQL).bind(&[
        JsValue::from_str(&f.day),
        JsValue::from_str(COST_WORKER),
        JsValue::from_f64(f.window.requests as f64),
        JsValue::from_f64(f.window.reads as f64),
        JsValue::from_f64(f.window.writes as f64),
        JsValue::from_f64(f.window.stmts as f64),
        JsValue::from_f64(now_ms.floor()),
        JsValue::from_f64(f.window.queue_ops as f64),
    ]);
    let result = match bound {
        Ok(stmt) => stmt.counted_run().await.map(|_| ()),
        Err(e) => Err(e),
    };
    if let Err(e) = result {
        worker::console_log!("[cost] the window of {} could not be written: {e}", f.day);
        restore_cost(&f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Waker;

    fn noop_waker() -> Waker {
        Waker::noop().clone()
    }

    /// A future that notes `n` statements, one per poll, yielding between them (an await on D1).
    struct Statements {
        left: u32,
        reads_each: u64,
    }
    impl Future for Statements {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.left == 0 {
                return Poll::Ready(());
            }
            note(self.reads_each, 1);
            self.left -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    #[test]
    fn ns_cost_each_statement_lands_on_the_pass_that_issued_it_and_on_the_request() {
        // NS-COST-BAR-1: a tick's steps run one after another inside one `passes` future; each statement lands on the
        // label in force when it was noted, a neighbour request polled between them is not a pass's, and the
        // request's own tally still counts the pass's statements.
        struct Steps {
            at: u32,
        }
        impl Future for Steps {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                match self.at {
                    0 => note(5, 0),
                    1 => {
                        label("gc");
                        note(7, 1);
                    }
                    2 => note(11, 0),
                    3 => {
                        label("relatch");
                        note(13, 0);
                    }
                    _ => return Poll::Ready(()),
                }
                self.at += 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut request = Box::pin(scoped(passes("first", Steps { at: 0 })));
        let mut neighbour = Box::pin(scoped(Statements {
            left: 4,
            reads_each: 1000,
        }));
        let mut by_pass = None;
        let mut total = None;
        while by_pass.is_none() {
            if let Poll::Ready((((), p), t)) = request.as_mut().poll(&mut cx) {
                by_pass = Some(p);
                total = Some(t);
            }
            let _ = neighbour.as_mut().poll(&mut cx);
        }
        label("ignored"); // outside any pass: a no-op
        let by_pass = by_pass.unwrap();
        let reads: Vec<_> = by_pass
            .iter()
            .map(|(k, t)| (*k, t.reads, t.stmts))
            .collect();
        assert_eq!(reads, [("first", 5, 1), ("gc", 18, 2), ("relatch", 13, 1)]);
        assert_eq!(
            total.unwrap().reads,
            36,
            "the request counts the pass's statements too"
        );
        assert_eq!(current(), None, "no request in scope here");
    }

    #[test]
    fn two_interleaved_requests_each_get_their_own_statements() {
        // bsv-low #499: an isolate polls several requests in turn; a statement lands on the request whose poll
        // read its meta, never on its neighbour (and nothing leaks to `unscoped`).
        let before = UNSCOPED.with(|u| u.get());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut a = Box::pin(scoped(Statements {
            left: 3,
            reads_each: 10,
        }));
        let mut b = Box::pin(scoped(Statements {
            left: 5,
            reads_each: 1,
        }));
        let (mut ta, mut tb) = (None, None);
        while ta.is_none() || tb.is_none() {
            if ta.is_none() {
                if let Poll::Ready(((), t)) = a.as_mut().poll(&mut cx) {
                    ta = Some(t);
                }
            }
            if tb.is_none() {
                if let Poll::Ready(((), t)) = b.as_mut().poll(&mut cx) {
                    tb = Some(t);
                }
            }
        }
        assert_eq!(
            ta,
            Some(Tally {
                reads: 30,
                writes: 3,
                stmts: 3,
                max_reads: 10
            })
        );
        assert_eq!(
            tb,
            Some(Tally {
                reads: 5,
                writes: 5,
                stmts: 5,
                max_reads: 1
            })
        );
        assert_eq!(
            UNSCOPED.with(|u| u.get()),
            before,
            "nothing noted inside a scope may land on unscoped"
        );
        // and outside every scope it does
        note(7, 0);
        let after = UNSCOPED.with(|u| u.get());
        assert_eq!(
            (after.reads - before.reads, after.stmts - before.stmts),
            (7, 1)
        );
    }

    /// bsv-low #499 (lens fold L1): NO D1 STATEMENT IS AWAITED OUTSIDE THE COUNTED SEAM. Every await form of the
    /// `worker` D1 API (`all()`, `run()`, `raw()`, `first(..)`, `batch(..)`) appears in this file only; the crate under
    /// test is walked (this file is compiled into both workers, so each crate's run walks its own `src`). Building a
    /// statement (`prepare(..)`, `bind(..)`) issues nothing and is not forbidden: the cost is the await. RED on
    /// `a0db9fb`: the `/tx-any` index leg awaited `stmt.first(None)` (low-app-layer `routes.rs`, 2 statements a
    /// request uncounted). Not pinned: `D1Database::exec` (its name is the overlay's `ShadowDb` method too, which goes
    /// through `d1::Query`); no production code calls it.
    #[test]
    fn no_d1_statement_is_awaited_outside_the_ledger() {
        const BARE: &[&str] = &[
            ".all()",
            ".run()",
            ".raw()",
            ".raw::<",
            ".first(None",
            ".first(Some",
            ".first::<",
            ".batch(",
        ];
        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for e in std::fs::read_dir(dir).expect("read src") {
                let p = e.expect("entry").path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push(p);
                }
            }
        }
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        walk(&src, &mut files);
        assert!(files.len() > 5, "the crate's sources are walked");
        let mut holes = Vec::new();
        for f in files
            .iter()
            .filter(|f| f.file_name().is_some_and(|n| n != "d1_ledger.rs"))
        {
            let text = std::fs::read_to_string(f).expect("read source");
            for (i, line) in text.lines().enumerate() {
                let code = line.split("//").next().unwrap_or("");
                if BARE.iter().any(|b| code.contains(b)) {
                    holes.push(format!("{}:{}: {}", f.display(), i + 1, line.trim()));
                }
            }
        }
        assert!(
            holes.is_empty(),
            "D1 awaited outside the counted seam (use d1_ledger::Counted / counted_batch):\n{}",
            holes.join("\n")
        );
        // the pin bites on the form it was written for
        assert!(BARE
            .iter()
            .any(|b| "let row: Option<BeefTrustRow> = match stmt.first(None).await {".contains(b)));
    }

    #[test]
    fn the_route_maxima_are_per_field_and_the_set_is_closed() {
        let mut b = RouteBudget::default();
        b.record(Tally {
            reads: 100,
            writes: 0,
            stmts: 2,
            max_reads: 0,
        });
        b.record(Tally {
            reads: 5,
            writes: 9,
            stmts: 3,
            max_reads: 0,
        });
        assert_eq!(b.requests, 2);
        assert_eq!(
            b.max,
            Tally {
                reads: 100,
                writes: 9,
                stmts: 3,
                max_reads: 0
            }
        );
        assert_eq!(
            b.last,
            Tally {
                reads: 5,
                writes: 9,
                stmts: 3,
                max_reads: 0
            }
        );
        assert_eq!(
            b.total,
            Tally {
                reads: 105,
                writes: 9,
                stmts: 5,
                max_reads: 0
            }
        );
        const R: &[&str] = &["/owed", "/tx-any", "/beef"];
        assert_eq!(route_of("/owed", R), "/owed");
        assert_eq!(route_of("/tx-any/ab", R), "/tx-any");
        assert_eq!(
            route_of("/beefy", R),
            "other",
            "a prefix that is not a path segment is not the route"
        );
        assert_eq!(route_of("/anything/else", R), "other");
    }

    #[test]
    fn the_segment_appends_to_the_routes_own_timing_and_the_expose_names_it_once() {
        let t = Tally {
            reads: 12,
            writes: 3,
            stmts: 4,
            max_reads: 0,
        };
        assert_eq!(
            server_timing_segment(t),
            "d1;desc=\"reads=12 writes=3 stmts=4 maxReads=0\""
        );
        assert_eq!(with_d1_segment(None, t), server_timing_segment(t));
        assert_eq!(
            with_d1_segment(Some("admit;dur=1.0"), t),
            format!("admit;dur=1.0, {}", server_timing_segment(t))
        );
        assert_eq!(
            parse_segment(&with_d1_segment(Some("admit;dur=1.0"), t), "d1"),
            Some(t)
        );
        assert_eq!(
            parse_segment(&named_segment("d1view", t), "d1view"),
            Some(t)
        );
        assert_eq!(
            parse_segment(&named_segment("d1view", t), "d1"),
            None,
            "d1 is not d1view"
        );
        assert_eq!(
            parse_segment("d1;desc=\"reads=1 writes=x stmts=1\"", "d1"),
            None
        );
        assert_eq!(expose_server_timing(None), "Server-Timing");
        assert_eq!(
            expose_server_timing(Some("x-bsv-auth-nonce")),
            "x-bsv-auth-nonce, Server-Timing"
        );
        assert_eq!(
            expose_server_timing(Some("Server-Timing, X-Overlay-Mutation")),
            "Server-Timing, X-Overlay-Mutation"
        );
        let mut m = BTreeMap::new();
        m.entry("/owed")
            .or_insert_with(RouteBudget::default)
            .record(t);
        let j = budget_json_of(&m, Tally::default());
        assert_eq!(j["routes"]["/owed"]["max"]["reads"], 12);
        assert_eq!(j["routes"]["/owed"]["requests"], 1);
        assert_eq!(j["unscoped"]["stmts"], 0);
    }
}

/// bsv-low #635 (B-COST-1): the cost window's pins. Each runs on its own test thread, so its thread-local window is
/// its own.
#[cfg(test)]
mod b_cost_1 {
    use super::*;

    #[test]
    fn b_cost_1_every_statement_scoped_or_not_and_every_request_lands_in_the_window() {
        assert_eq!(cost_window(), CostWindow::default());
        // A statement of a request in scope, one outside any (the cron's, `wait_until`'s), and a request.
        let mut fut = Box::pin(scoped(async { note(7, 2) }));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(fut.as_mut().poll(&mut cx).is_ready());
        note(3, 0);
        note_request(1_792_799_999_000.0);
        assert_eq!(
            cost_window(),
            CostWindow {
                day: Some(20_749),
                requests: 1,
                reads: 10,
                writes: 2,
                stmts: 2,
                queue_ops: 0
            },
            "the window holds every noted statement and the request"
        );
    }

    #[test]
    fn b_cost_1_the_window_is_written_per_64_requests_or_minute_never_per_request_and_nothing_is_lost(
    ) {
        let t0 = 1_792_756_800_000.0; // 2026-10-23T12:00:00Z
        let mut takes = Vec::new();
        for i in 0..1_000u64 {
            note(4, 1); // the request's own statement
            if let Some(f) = cost_on_request(t0 + i as f64) {
                takes.push(f);
            }
        }
        // 1,000 requests in one second: the isolate's first take (due at once), then one per 64 requests.
        assert!(
            takes.len() as u64 <= 1_000u64.div_ceil(COST_FLUSH_REQUESTS) + 1,
            "{} writes",
            takes.len()
        );
        assert!(takes.len() >= 15, "{} writes", takes.len());
        let rest = cost_window();
        let requests: u64 = takes.iter().map(|f| f.window.requests).sum::<u64>() + rest.requests;
        let reads: u64 = takes.iter().map(|f| f.window.reads).sum::<u64>() + rest.reads;
        assert_eq!(
            (requests, reads),
            (1_000, 4_000),
            "every request and row is in a write or the window"
        );
        assert!(takes.iter().all(|f| f.day == "2026-10-23"));
        // The minute: one request after the last take is not due before 60 s, and is at 60 s.
        let last = t0 + 999.0 + 10.0;
        let _ = take_cost_if_due(last + COST_FLUSH_MS); // drain whatever is left
        note_request(last + COST_FLUSH_MS + 1.0);
        assert_eq!(
            take_cost_if_due(last + 2.0 * COST_FLUSH_MS - 1.0),
            None,
            "59,999 ms after the last take"
        );
        assert!(
            take_cost_if_due(last + 2.0 * COST_FLUSH_MS).is_some(),
            "60,000 ms after the last take"
        );
        // An empty window is never written.
        assert_eq!(take_cost_if_due(last + 10.0 * COST_FLUSH_MS), None);
    }

    #[test]
    fn b_cost_1_a_window_lands_on_its_first_requests_day_and_a_new_day_is_due_at_once() {
        let before_midnight = 1_792_799_999_000.0; // 2026-10-23T23:59:59Z
        let _ = take_cost_if_due(before_midnight - 10.0 * COST_FLUSH_MS);
        let _ = cost_on_request(before_midnight - 1_000.0);
        assert_eq!(
            cost_on_request(before_midnight),
            Some(CostFlush {
                day: "2026-10-23".into(),
                window: CostWindow {
                    day: Some(20_749),
                    requests: 1,
                    ..Default::default()
                },
            })
        );
        // 1.5 s later it is the next UTC day: the window (opened yesterday) is due though the minute is not up.
        let taken = cost_on_request(before_midnight + 1_500.0).expect("a new day is due");
        assert_eq!(
            (taken.day.as_str(), taken.window.requests),
            ("2026-10-23", 1)
        );
        assert_eq!(
            cost_window().day,
            Some(20_750),
            "today's first request opens today's window"
        );
    }

    #[test]
    fn b_cost_1_a_faulted_write_is_restored_not_lost() {
        let t = 1_792_756_800_000.0;
        note(9, 3);
        note_request(t);
        let f = take_cost_if_due(t).expect("the isolate's first take is due");
        assert!(cost_window().is_empty());
        restore_cost(&f);
        assert_eq!(cost_window(), f.window);
    }

    #[test]
    fn b_cost_1_utc_dates() {
        assert_eq!(utc_date(0), "1970-01-01");
        assert_eq!(utc_date(-1), "1969-12-31");
        assert_eq!(utc_date(11_016), "2000-02-29");
        assert_eq!(utc_date(20_749), "2026-10-23");
        assert_eq!(utc_day(1_792_799_999_000.0), 20_749);
        assert_eq!(COST_WORKER, env!("CARGO_PKG_NAME"));
    }
}
