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
    pub reads: u64,
    pub writes: u64,
    pub stmts: u64,
}

impl Tally {
    /// One statement's meta folded in (a statement with no meta still counts as a statement).
    pub fn add(&mut self, reads: u64, writes: u64) {
        self.reads = self.reads.saturating_add(reads);
        self.writes = self.writes.saturating_add(writes);
        self.stmts = self.stmts.saturating_add(1);
    }
}

/// One route's figures since the isolate booted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RouteBudget {
    pub requests: u64,
    /// Each field its own running maximum (the request that read the most need not be the one that wrote the most).
    pub max: Tally,
    pub last: Tally,
    pub total: Tally,
}

impl RouteBudget {
    pub fn record(&mut self, t: Tally) {
        self.requests = self.requests.saturating_add(1);
        self.max.reads = self.max.reads.max(t.reads);
        self.max.writes = self.max.writes.max(t.writes);
        self.max.stmts = self.max.stmts.max(t.stmts);
        self.last = t;
        self.total.reads = self.total.reads.saturating_add(t.reads);
        self.total.writes = self.total.writes.saturating_add(t.writes);
        self.total.stmts = self.total.stmts.saturating_add(t.stmts);
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<Cell<Tally>>>> = const { RefCell::new(None) };
    static UNSCOPED: Cell<Tally> = const { Cell::new(Tally { reads: 0, writes: 0, stmts: 0 }) };
    static ROUTES: RefCell<BTreeMap<&'static str, RouteBudget>> = const { RefCell::new(BTreeMap::new()) };
}

/// Fold one statement into the request in scope, or into `unscoped` when none is.
pub fn note(reads: u64, writes: u64) {
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
    Scoped { tally: Rc::new(Cell::new(Tally::default())), fut: Box::pin(fut) }
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

/// The `Server-Timing` segment of one request.
pub fn server_timing_segment(t: Tally) -> String {
    named_segment("d1", t)
}

/// A segment of the same shape under another name (the app layer's view actor serves its compute's figures as
/// `d1view`: that compute is detached from the request that reads its copy).
pub fn named_segment(name: &str, t: Tally) -> String {
    format!("{name};desc=\"reads={} writes={} stmts={}\"", t.reads, t.writes, t.stmts)
}

/// PURE: the figures of the segment `name` in a `Server-Timing` value, if it carries one of our shape.
pub fn parse_segment(header: &str, name: &str) -> Option<Tally> {
    for seg in header.split(',') {
        let seg = seg.trim();
        let Some(rest) = seg.strip_prefix(name) else { continue };
        let Some(desc) = rest.trim_start().strip_prefix(";desc=\"") else { continue };
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
                _ => return None,
            }
            seen += 1;
        }
        return (seen == 3).then_some(t);
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
        Some(e) if e.split(',').any(|h| h.trim().eq_ignore_ascii_case("server-timing")) => e.to_string(),
        Some(e) => format!("{e}, Server-Timing"),
        None => "Server-Timing".to_string(),
    }
}

/// Stamp the d1 segment (and its CORS expose) on a finished response.
pub fn stamp(resp: &mut worker::Response, t: Tally) {
    let h = resp.headers_mut();
    let timing = with_d1_segment(h.get("Server-Timing").ok().flatten().as_deref(), t);
    let _ = h.set("Server-Timing", &timing);
    let expose = expose_server_timing(h.get("Access-Control-Expose-Headers").ok().flatten().as_deref());
    let _ = h.set("Access-Control-Expose-Headers", &expose);
}

fn tally_json(t: Tally) -> serde_json::Value {
    serde_json::json!({ "reads": t.reads, "writes": t.writes, "stmts": t.stmts })
}

/// PURE: the `d1Budget` body from a snapshot (pinned).
pub fn budget_json_of(routes: &BTreeMap<&'static str, RouteBudget>, unscoped: Tally) -> serde_json::Value {
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
/// `first()` too, so the rows read are the same, and `all()` is the call whose answer carries the meta.
#[allow(async_fn_in_trait)]
pub trait Counted {
    async fn counted_all(&self) -> worker::Result<worker::D1Result>;
    async fn counted_run(&self) -> worker::Result<worker::D1Result>;
    async fn counted_first<T: serde::de::DeserializeOwned>(&self) -> worker::Result<Option<T>>;
}

impl Counted for worker::D1PreparedStatement {
    async fn counted_all(&self) -> worker::Result<worker::D1Result> {
        let r = self.all().await?;
        note_meta(r.meta().ok().flatten().as_ref());
        Ok(r)
    }
    async fn counted_run(&self) -> worker::Result<worker::D1Result> {
        let r = self.run().await?;
        note_meta(r.meta().ok().flatten().as_ref());
        Ok(r)
    }
    async fn counted_first<T: serde::de::DeserializeOwned>(&self) -> worker::Result<Option<T>> {
        use worker::js_sys::{Array, Reflect};
        use worker::wasm_bindgen::{JsCast, JsValue};
        let js = worker::wasm_bindgen_futures::JsFuture::from(self.inner().all()?).await?;
        let meta = Reflect::get(&js, &JsValue::from_str("meta"))
            .ok()
            .and_then(|m| worker::serde_wasm_bindgen::from_value::<worker::D1ResultMeta>(m).ok());
        note_meta(meta.as_ref());
        let results = Reflect::get(&js, &JsValue::from_str("results"))?;
        let Ok(rows) = results.dyn_into::<Array>() else { return Ok(None) };
        if rows.length() == 0 {
            return Ok(None);
        }
        Ok(worker::serde_wasm_bindgen::from_value::<Option<T>>(rows.get(0))?)
    }
}

/// The counted form of `D1Database::batch` (one statement per member, each with its own meta).
pub async fn counted_batch(
    db: &worker::D1Database,
    stmts: Vec<worker::D1PreparedStatement>,
) -> worker::Result<Vec<worker::D1Result>> {
    let out = db.batch(stmts).await?;
    for r in &out {
        note_meta(r.meta().ok().flatten().as_ref());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    fn noop_waker() -> Waker {
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
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
    fn two_interleaved_requests_each_get_their_own_statements() {
        // bsv-low #499: an isolate polls several requests in turn; a statement lands on the request whose poll
        // read its meta, never on its neighbour (and nothing leaks to `unscoped`).
        let before = UNSCOPED.with(|u| u.get());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut a = Box::pin(scoped(Statements { left: 3, reads_each: 10 }));
        let mut b = Box::pin(scoped(Statements { left: 5, reads_each: 1 }));
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
        assert_eq!(ta, Some(Tally { reads: 30, writes: 3, stmts: 3 }));
        assert_eq!(tb, Some(Tally { reads: 5, writes: 5, stmts: 5 }));
        assert_eq!(UNSCOPED.with(|u| u.get()), before, "nothing noted inside a scope may land on unscoped");
        // and outside every scope it does
        note(7, 0);
        let after = UNSCOPED.with(|u| u.get());
        assert_eq!((after.reads - before.reads, after.stmts - before.stmts), (7, 1));
    }

    #[test]
    fn the_route_maxima_are_per_field_and_the_set_is_closed() {
        let mut b = RouteBudget::default();
        b.record(Tally { reads: 100, writes: 0, stmts: 2 });
        b.record(Tally { reads: 5, writes: 9, stmts: 3 });
        assert_eq!(b.requests, 2);
        assert_eq!(b.max, Tally { reads: 100, writes: 9, stmts: 3 });
        assert_eq!(b.last, Tally { reads: 5, writes: 9, stmts: 3 });
        assert_eq!(b.total, Tally { reads: 105, writes: 9, stmts: 5 });
        const R: &[&str] = &["/owed", "/tx-any", "/beef"];
        assert_eq!(route_of("/owed", R), "/owed");
        assert_eq!(route_of("/tx-any/ab", R), "/tx-any");
        assert_eq!(route_of("/beefy", R), "other", "a prefix that is not a path segment is not the route");
        assert_eq!(route_of("/anything/else", R), "other");
    }

    #[test]
    fn the_segment_appends_to_the_routes_own_timing_and_the_expose_names_it_once() {
        let t = Tally { reads: 12, writes: 3, stmts: 4 };
        assert_eq!(server_timing_segment(t), "d1;desc=\"reads=12 writes=3 stmts=4\"");
        assert_eq!(with_d1_segment(None, t), server_timing_segment(t));
        assert_eq!(with_d1_segment(Some("admit;dur=1.0"), t), format!("admit;dur=1.0, {}", server_timing_segment(t)));
        assert_eq!(parse_segment(&with_d1_segment(Some("admit;dur=1.0"), t), "d1"), Some(t));
        assert_eq!(parse_segment(&named_segment("d1view", t), "d1view"), Some(t));
        assert_eq!(parse_segment(&named_segment("d1view", t), "d1"), None, "d1 is not d1view");
        assert_eq!(parse_segment("d1;desc=\"reads=1 writes=x stmts=1\"", "d1"), None);
        assert_eq!(expose_server_timing(None), "Server-Timing");
        assert_eq!(expose_server_timing(Some("x-bsv-auth-nonce")), "x-bsv-auth-nonce, Server-Timing");
        assert_eq!(expose_server_timing(Some("Server-Timing, X-Overlay-Mutation")), "Server-Timing, X-Overlay-Mutation");
        let mut m = BTreeMap::new();
        m.entry("/owed").or_insert_with(RouteBudget::default).record(t);
        let j = budget_json_of(&m, Tally::default());
        assert_eq!(j["routes"]["/owed"]["max"]["reads"], 12);
        assert_eq!(j["routes"]["/owed"]["requests"], 1);
        assert_eq!(j["unscoped"]["stmts"], 0);
    }
}
