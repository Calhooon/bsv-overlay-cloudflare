//! E649 L4 (the E649 north-star plan, lane E649-HOOK, 2026-10-13): the write hooks `/internal/pot-changed` and
//! `/internal/hop-changed` are WRITES. Beta measured the pot hook at 15.3 s p50 and 30 s p90 (Observability,
//! 12:05:30 to 12:06:30Z): step 0 awaited one relay `/broadcast` per outpoint, whose answer waits on the relay's
//! sequential fan-out (11 to 15 s p50 under the fleet), then a full `/results` gather per attributed seat.
//!
//! The rule here: the answer path is D1 alone, in batches; no relay call and no read-model walk is awaited before
//! the answer. What the hook decides is [`apply_pot_changed`] / [`apply_hop_changed`], over a [`HookStore`] port
//! (the worker's is `routes::D1HookStore`; a queue consumer is the next caller, L3's seam). The publish is
//! [`announce`], run DETACHED (`ctx.wait_until`) under one wall ([`ANNOUNCE_WALL_MS`]) and counted. The owed
//! recompute stays where #469 and #648 put it: after the answer, coalesced and shed per identity.
//!
//! The seat's `pot` event no longer carries a gathered `/results` entry. No reader reads it: the client's parser
//! (`app/src/lib/lowEvents.ts` `parseLowEvent`) requires `entry` to be an OBJECT, and every consumer
//! (`engine/orchestrator.ts` `potEvent`, `pages/History.tsx`, `components/UnfinishedBusiness.tsx`,
//! `hooks/useIdentityEvents.ts`) reads the kind and the outpoint and re-reads the served rows. So the event
//! carries `entry: {}` ([`carrier_entry`]) and the seat's own read serves the entry.
use serde_json::{json, Value};
use std::cell::RefCell;
use std::future::Future;

/// The pots-room announce and the seats' events of one body run under ONE wall, under the platform's 30 s
/// `waitUntil` window with room for the counter flush after it. The relay call has no timeout of its own today;
/// this is it.
pub const ANNOUNCE_WALL_MS: u64 = 20_000;

/// Relay calls of the detached announce that answered 2xx.
pub const COUNTER_ANNOUNCE_SENT: &str = "pot_changed_announce_sent_total";
/// Relay calls not made: the deploy has no `RELAY_URL` / `BROADCAST_TOKEN` (/ `SERVER_PRIVATE_KEY` for a seat).
pub const COUNTER_ANNOUNCE_SKIPPED: &str = "pot_changed_announce_skipped_total";
/// Relay calls that failed (a transport fault, a non-2xx) or had not answered at the wall.
pub const COUNTER_ANNOUNCE_FAILED: &str = "pot_changed_announce_failed_total";

/// What one relay call came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    Sent,
    NotConfigured,
    Failed,
}

/// What the store read for one outpoint of a body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutpointRead {
    /// The decoded-pots read faulted (the whole batch, or this outpoint's statement).
    Failed(String),
    /// No decoded params (an evicted pot, a released hop): the identities the seats' own markers name.
    NoParams { marker_named: Vec<String> },
    /// Decoded: the seats `attribute_seats` proved (lowercase), and the marker-named identities.
    Decoded {
        seats: Vec<String>,
        marker_named: Vec<String>,
    },
}

/// One bound value of a [`Stmt`].
#[derive(Debug, Clone, PartialEq)]
pub enum Bind {
    Text(String),
    Int(i64),
    Null,
}

/// One statement of a batch: the SQL and its binds, in `?N` order.
#[derive(Debug, Clone, PartialEq)]
pub struct Stmt {
    pub sql: String,
    pub binds: Vec<Bind>,
}

impl Stmt {
    fn new(sql: impl Into<String>, binds: Vec<Bind>) -> Self {
        Stmt {
            sql: sql.into(),
            binds,
        }
    }
}

/// What one statement of a batch answered: its rows (JSON objects by column name) and the rows it changed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StmtOut {
    pub rows: Vec<Value>,
    pub changes: u64,
}

/// The SQL under [`HookD1`]: ONE `batch` is one D1 round trip (the worker's `D1Sql` runs `counted_batch`), atomic
/// as D1's is (one statement that faults fails the batch). The native pins run the same statements over real
/// SQLite with the overlay's migrations.
#[allow(async_fn_in_trait)]
pub trait SqlPort {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String>;
    /// Add counter deltas (`ops_counters`), best effort.
    async fn counters(&self, deltas: Vec<(String, u64)>);
    fn now_ms(&self) -> u64;
    fn log(&self, line: &str);
}

/// The D1 side of the hooks. The worker's is [`HookD1`] over `routes::D1Sql`.
#[allow(async_fn_in_trait)]
pub trait HookStore {
    /// Every outpoint's attribution, in the order of `outpoints`.
    async fn read_outpoints(&self, outpoints: &[(String, u32)]) -> Vec<OutpointRead>;
    /// Every stale mark of the body, in ONE batch. `Err` is logged by the caller; the recompute still runs.
    async fn mark_stale(&self, marks: &[(String, &'static str)]) -> Result<(), String>;
    async fn counters(&self, deltas: Vec<(String, u64)>);
    fn now_ms(&self) -> u64;
    fn log(&self, line: &str);
}

/// The marker arms (they were `owed_identities_by_marker`, a statement each, per outpoint): the identities the
/// seats' OWN markers name for an outpoint. A POT event is answered by the party arm and the refund-backup arm
/// (both keyed by the pot); a released-HOP event by the hop arm (keyed by the hop container's outpoint, which never
/// matches a pot).
pub const MARKER_ARMS: [(&str, &str); 3] = [
    ("pot", crate::owed::OWED_ATTRIBUTE_BY_POT_SQL),
    ("hop", crate::owed::OWED_ATTRIBUTE_BY_HOP_SQL),
    ("potrefund", crate::owed::OWED_ATTRIBUTE_BY_POTREFUND_SQL),
];

/// The hooks' D1 store: the shipped statements over a [`SqlPort`].
pub struct HookD1<P: SqlPort> {
    pub sql: P,
}

fn identity_rows(rows: &[Value], ids: &mut Vec<String>) {
    for r in rows {
        if let Some(id) = r.get("identity").and_then(Value::as_str) {
            push_unique(ids, &id.to_ascii_lowercase());
        }
    }
}

fn decoded_of(out: &StmtOut) -> Result<Option<crate::results::CovenantParams>, String> {
    match out.rows.first() {
        None => Ok(None),
        Some(row) => serde_json::from_value::<crate::routes::DecodedPotRowD1>(row.clone())
            .map(|r| r.covenant_params())
            .map_err(|e| e.to_string()),
    }
}

impl<P: SqlPort> HookD1<P> {
    /// The decoded pots' verified seat markers, ONE batch of its chunks. A fault leaves the attribution partial.
    async fn seat_markers(
        &self,
        params_by_pot: &std::collections::HashMap<(String, u32), crate::results::CovenantParams>,
    ) -> std::collections::HashMap<(String, u32), Vec<crate::results::SeatMarkerRow>> {
        let mut out = std::collections::HashMap::new();
        let mut stmts = Vec::new();
        for chunk in crate::results::seat_marker_chunks(params_by_pot) {
            let sql =
                crate::results::seat_markers_sql(chunk.len(), crate::results::SEAT_MARKERS_PER_KEY);
            let mut binds =
                Vec::with_capacity(chunk.len() * crate::results::SEAT_MARKERS_BINDS_PER_POT);
            for b in &chunk {
                binds.push(Bind::Text(b.pot_txid.clone()));
                binds.push(Bind::Int(i64::from(b.pot_vout)));
                binds.push(Bind::Text(b.pub_a_hex.clone()));
                binds.push(Bind::Text(b.pub_b_hex.clone()));
            }
            stmts.push(Stmt::new(sql, binds));
        }
        if stmts.is_empty() {
            return out;
        }
        match self.sql.run_batch(stmts).await {
            Ok(results) => {
                for r in results {
                    let rows: Result<Vec<crate::routes::SeatMarkerRowD1>, _> =
                        r.rows.into_iter().map(serde_json::from_value).collect();
                    match rows {
                        Ok(fetched) => crate::routes::note_seat_marker_rows(&mut out, fetched),
                        Err(e) => self.sql.log(&format!(
                            "[pot-changed] seat-marker rows unreadable (attribution partial): {e}"
                        )),
                    }
                }
            }
            Err(e) => self.sql.log(&format!(
                "[pot-changed] seat-marker batch failed (attribution partial): {e}"
            )),
        }
        out
    }
}

impl<P: SqlPort> HookStore for HookD1<P> {
    async fn read_outpoints(&self, outpoints: &[(String, u32)]) -> Vec<OutpointRead> {
        let failed_all = |e: String| -> Vec<OutpointRead> {
            outpoints
                .iter()
                .map(|_| OutpointRead::Failed(e.clone()))
                .collect()
        };
        // (1) ONE batch: per outpoint, its decoded row and the three marker arms.
        let decoded_sql = crate::results::decoded_pots_sql(1);
        let per = 1 + MARKER_ARMS.len();
        let mut stmts = Vec::with_capacity(outpoints.len() * per);
        for (txid, vout) in outpoints {
            let binds = vec![Bind::Text(txid.clone()), Bind::Int(i64::from(*vout))];
            stmts.push(Stmt::new(decoded_sql.clone(), binds.clone()));
            for (_, sql) in MARKER_ARMS {
                stmts.push(Stmt::new(sql, binds.clone()));
            }
        }
        let results = match self.sql.run_batch(stmts).await {
            Ok(r) => r,
            Err(e) => {
                self.sql
                    .log(&format!("[pot-changed] attribution batch failed: {e}"));
                return failed_all(e);
            }
        };
        let mut decoded = Vec::with_capacity(outpoints.len());
        let mut named: Vec<Vec<String>> = Vec::with_capacity(outpoints.len());
        for i in 0..outpoints.len() {
            let group = &results[i * per..(i + 1) * per];
            decoded.push(decoded_of(&group[0]));
            let mut ids = Vec::new();
            for r in &group[1..] {
                identity_rows(&r.rows, &mut ids);
            }
            named.push(ids);
        }
        // (2) ONE batch: the decoded pots' seat markers, then `attribute_seats` per pot.
        let mut params_by_pot = std::collections::HashMap::new();
        for ((txid, vout), d) in outpoints.iter().zip(&decoded) {
            if let Ok(Some(p)) = d {
                params_by_pot.insert((txid.clone(), *vout), p.clone());
            }
        }
        let markers = self.seat_markers(&params_by_pot).await;
        outpoints
            .iter()
            .zip(decoded)
            .zip(named)
            .map(|(((txid, vout), d), marker_named)| match d {
                Err(e) => OutpointRead::Failed(e),
                Ok(None) => OutpointRead::NoParams { marker_named },
                Ok(Some(params)) => OutpointRead::Decoded {
                    seats: seats_of(&params, txid, *vout, &markers),
                    marker_named,
                },
            })
            .collect()
    }

    async fn mark_stale(&self, marks: &[(String, &'static str)]) -> Result<(), String> {
        let stmts = marks
            .iter()
            .map(|(id, _)| {
                Stmt::new(
                    crate::owed::OWED_STALE_FOR_IDENTITY_SQL,
                    vec![Bind::Text(id.clone())],
                )
            })
            .collect();
        self.sql.run_batch(stmts).await.map(|_| ())
    }

    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.sql.counters(deltas).await
    }
    fn now_ms(&self) -> u64 {
        self.sql.now_ms()
    }
    fn log(&self, line: &str) {
        self.sql.log(line)
    }
}

fn seats_of(
    params: &crate::results::CovenantParams,
    txid: &str,
    vout: u32,
    markers: &std::collections::HashMap<(String, u32), Vec<crate::results::SeatMarkerRow>>,
) -> Vec<String> {
    let key = (txid.to_ascii_lowercase(), vout);
    let attr = crate::results::attribute_seats(
        params,
        txid,
        vout,
        markers.get(&key).map(Vec::as_slice).unwrap_or(&[]),
    );
    [attr.identity_a, attr.identity_b]
        .into_iter()
        .flatten()
        .map(|s| s.to_ascii_lowercase())
        .collect()
}

/// The relay side of the detached announce.
#[allow(async_fn_in_trait)]
pub trait RelayPort {
    async fn broadcast(&self, room: &'static str, body: Value) -> Delivery;
    async fn push(&self, recipient: &str, body: Value) -> Delivery;
}

/// What the detached work of one body is: the outpoints to announce in the pots room, and the seats to tell.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnnouncePlan {
    pub outpoints: Vec<(String, u32)>,
    /// (identity, txid, vout): one `pot` event into the seat's `low_events` box.
    pub seat_events: Vec<(String, String, u32)>,
    pub at_ms: u64,
}

impl AnnouncePlan {
    pub fn calls(&self) -> u64 {
        (self.outpoints.len() + self.seat_events.len()) as u64
    }
}

/// What [`apply_pot_changed`] decided: the answer's lists, the identities whose owed rows the detached recompute
/// re-derives, and the detached announce.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PotChangedOutcome {
    pub filed: Vec<Value>,
    pub skipped: Vec<Value>,
    pub owed_identities: Vec<String>,
    pub stale_marks: Vec<(String, &'static str)>,
    pub stale_fault: Option<String>,
    pub plan: AnnouncePlan,
}

fn push_unique(v: &mut Vec<String>, id: &str) {
    if !v.iter().any(|x| x == id) {
        v.push(id.to_string());
    }
}

/// The pot hook's whole decision for one parsed body: one read through the store, one stale-mark batch, and the
/// plan of what runs after the answer. No relay call, no read-model walk, no owed recompute.
///
/// Per outpoint, the branches of the handler before L4: a read fault is skipped; with no decoded params, or with
/// decoded params and no attributed seat, the marker-named identities are marked stale and queued for the
/// recompute (fleet loop 11 and 15); an attributed pot queues a `pot` event for each seat and its recompute.
pub async fn apply_pot_changed<S: HookStore>(
    store: &S,
    outpoints: &[(String, u32)],
    at_ms: u64,
) -> PotChangedOutcome {
    let mut out = PotChangedOutcome {
        plan: AnnouncePlan {
            outpoints: outpoints.to_vec(),
            seat_events: Vec::new(),
            at_ms,
        },
        ..PotChangedOutcome::default()
    };
    let reads = store.read_outpoints(outpoints).await;
    for ((txid, vout), read) in outpoints.iter().zip(reads) {
        let (txid, vout) = (txid.as_str(), *vout);
        match read {
            OutpointRead::Failed(e) => {
                out.skipped.push(json!({ "txid": txid, "vout": vout, "why": format!("[pot-changed] decoded-pots read failed for {txid}:{vout}: {e}") }));
            }
            OutpointRead::NoParams { marker_named } => {
                for id in &marker_named {
                    out.stale_marks
                        .push((id.clone(), "pot-changed (by marker)"));
                    push_unique(&mut out.owed_identities, id);
                }
                out.skipped.push(json!({ "txid": txid, "vout": vout, "why": format!("[pot-changed] {txid}:{vout} has no decoded params yet, nothing to file ({} seat(s) attributed by marker)", marker_named.len()) }));
            }
            OutpointRead::Decoded {
                seats,
                marker_named,
            } if seats.is_empty() => {
                for id in &marker_named {
                    out.stale_marks
                        .push((id.clone(), "pot-changed (unattributed, by marker)"));
                    push_unique(&mut out.owed_identities, id);
                }
                out.skipped.push(json!({ "txid": txid, "vout": vout, "why": format!("[pot-changed] {txid}:{vout} has no attributed seats yet, nothing to file ({} seat(s) marked stale by marker)", marker_named.len()) }));
            }
            OutpointRead::Decoded { seats, .. } => {
                for id in seats {
                    out.plan
                        .seat_events
                        .push((id.clone(), txid.to_string(), vout));
                    push_unique(&mut out.owed_identities, &id);
                    out.filed
                        .push(json!({ "txid": txid, "vout": vout, "identity": id }));
                }
            }
        }
    }
    if !out.stale_marks.is_empty() {
        if let Err(e) = store.mark_stale(&out.stale_marks).await {
            out.stale_fault = Some(e);
        }
    }
    out
}

/// The hop hook's decision: every parsed identity marked stale in ONE batch; the recompute runs after the answer.
pub async fn apply_hop_changed<S: HookStore>(
    store: &S,
    identities: &[String],
) -> Result<(), String> {
    let marks: Vec<(String, &'static str)> = identities
        .iter()
        .map(|id| (id.clone(), "hop-changed"))
        .collect();
    if marks.is_empty() {
        return Ok(());
    }
    store.mark_stale(&marks).await
}

/// The `entry` of a seat's `pot` event since L4: an empty object (the client's parser requires an object; no
/// reader reads a field of it, the seat re-reads its served rows).
pub fn carrier_entry() -> Value {
    json!({})
}

/// What the detached announce of one body came to, in relay calls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AnnounceTally {
    pub sent: u64,
    pub skipped: u64,
    pub failed: u64,
    /// The wall fell before every call answered (the unanswered ones are in `failed`).
    pub walled: bool,
}

impl AnnounceTally {
    fn note(&mut self, d: Delivery) {
        match d {
            Delivery::Sent => self.sent += 1,
            Delivery::NotConfigured => self.skipped += 1,
            Delivery::Failed => self.failed += 1,
        }
    }

    /// The counter rows to add (nonzero only), in the file's counter style (`ops_counters`).
    pub fn counters(&self) -> Vec<(String, u64)> {
        [
            (COUNTER_ANNOUNCE_SENT, self.sent),
            (COUNTER_ANNOUNCE_SKIPPED, self.skipped),
            (COUNTER_ANNOUNCE_FAILED, self.failed),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| (k.to_string(), n))
        .collect()
    }
}

/// The detached announce of one body: every changed outpoint into the pots room (the deployed client matches
/// `potOutpoint` alone, so one body per outpoint; the calls of one hook run together, not one after another),
/// and one `pot` event into each attributed seat's box, all raced against ONE `wall`. A call that has not answered
/// at the wall is dropped and counted failed.
pub async fn announce<R: RelayPort, W: Future<Output = ()>>(
    relay: &R,
    plan: &AnnouncePlan,
    wall: W,
) -> AnnounceTally {
    use futures_util::future::{join_all, select, Either};
    let tally = RefCell::new(AnnounceTally::default());
    let pots = plan.outpoints.iter().map(|(txid, vout)| {
        let body = crate::internal_events::pot_changed_event_body(txid, *vout, plan.at_ms);
        let tally = &tally;
        async move {
            let d = relay
                .broadcast(crate::internal_events::POTS_ROOM, body)
                .await;
            tally.borrow_mut().note(d);
        }
    });
    let seats = plan.seat_events.iter().map(|(id, txid, vout)| {
        let body = crate::internal_events::pot_event_body(txid, *vout, carrier_entry(), plan.at_ms);
        let tally = &tally;
        async move {
            let d = relay.push(id, body).await;
            tally.borrow_mut().note(d);
        }
    });
    let all = async {
        futures_util::future::join(join_all(pots), join_all(seats)).await;
    };
    futures_util::pin_mut!(all, wall);
    let walled = matches!(select(all, wall).await, Either::Right(_));
    let mut t = *tally.borrow();
    if walled {
        let answered = t.sent + t.skipped + t.failed;
        t.failed += plan.calls().saturating_sub(answered);
        t.walled = true;
    }
    t
}

/// The worker's relay: the `RELAY` binding through `internal_events`.
pub struct WorkerRelay {
    pub env: worker::Env,
}

impl RelayPort for WorkerRelay {
    async fn broadcast(&self, room: &'static str, body: Value) -> Delivery {
        crate::internal_events::push_broadcast(&self.env, room, body).await
    }
    async fn push(&self, recipient: &str, body: Value) -> Delivery {
        crate::internal_events::first_party_push(&self.env, recipient, body).await
    }
}

/// One unit of work handed past the answer (the worker's is `ctx.wait_until`).
pub type Detached = std::pin::Pin<Box<dyn Future<Output = ()>>>;

/// The detached announce of one body: the announce under the wall, its log line and its counters.
pub async fn announce_and_report<S: HookStore, R: RelayPort, W: Future<Output = ()>>(
    store: &S,
    relay: &R,
    plan: &AnnouncePlan,
    wall: W,
) -> AnnounceTally {
    let t0 = store.now_ms();
    let t = announce(relay, plan, wall).await;
    store.log(&format!(
        "[pot-changed] announce: {} outpoint(s), {} seat event(s): sent={} skipped={} failed={}{} in {} ms",
        plan.outpoints.len(),
        plan.seat_events.len(),
        t.sent,
        t.skipped,
        t.failed,
        if t.walled { " (WALL)" } else { "" },
        store.now_ms().saturating_sub(t0)
    ));
    let counters = t.counters();
    if !counters.is_empty() {
        store.counters(counters).await;
    }
    t
}

/// THE POT HOOK, after its bearer and its parse: the decision over the store ([`apply_pot_changed`]), then the
/// announce handed to `detach` (never awaited here), then the outcome the answer is built from. The worker's
/// `routes::internal_pot_changed` calls it with `HookD1` over D1, the `RELAY` binding and `ctx.wait_until`; the
/// pins call it with the same store over SQLite and a recording `detach`.
pub async fn serve_pot_changed<S, R, W>(
    store: std::rc::Rc<S>,
    relay: std::rc::Rc<R>,
    wall: W,
    detach: &dyn Fn(Detached),
    outpoints: &[(String, u32)],
    at_ms: u64,
) -> PotChangedOutcome
where
    S: HookStore + 'static,
    R: RelayPort + 'static,
    W: Future<Output = ()> + 'static,
{
    let outcome = apply_pot_changed(&*store, outpoints, at_ms).await;
    let plan = outcome.plan.clone();
    detach(Box::pin(async move {
        announce_and_report(&*store, &*relay, &plan, wall).await;
    }));
    outcome
}

/// The `Server-Timing` segment of a hook's answer.
pub fn hook_timing(ms: u64) -> String {
    format!("hook;dur={ms}")
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::{Duration, Instant};

    fn code_only(s: &str) -> String {
        s.lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n")
    }
    fn squash(s: &str) -> String {
        s.split_whitespace().collect()
    }
    /// One item of `routes.rs`, squashed: from `open` to the next `pub(crate)` async fn or struct.
    fn routes_item(open: &str) -> String {
        let routes = squash(&code_only(include_str!("routes.rs")));
        let start = routes.find(&squash(open)).expect("the item");
        let rest = &routes[start + 1..];
        let end = ["pub(crate) async fn", "pub(crate) struct"]
            .iter()
            .filter_map(|m| rest.find(&squash(m)))
            .min()
            .unwrap_or(rest.len());
        routes[start..start + 1 + end].to_string()
    }

    const A: &str = "02a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";

    fn op(i: u32) -> (String, u32) {
        (format!("{i:064x}"), i % 3)
    }

    /// THE REAL STORE'S SQL over real SQLite: the overlay's production migrations, a batch atomic as D1's (one
    /// statement that faults rolls the batch back and fails it), every batch and statement counted, the clock
    /// settable, the counters and the log lines kept.
    pub(crate) struct SqliteSql {
        pub conn: rusqlite::Connection,
        pub batches: Cell<u32>,
        pub stmts: Cell<u32>,
        pub now: Cell<u64>,
        pub counted: RefCell<Vec<(String, u64)>>,
        pub lines: RefCell<Vec<String>>,
    }

    impl SqliteSql {
        pub(crate) fn migrated() -> Self {
            let conn = rusqlite::Connection::open_in_memory().expect("sqlite");
            for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
                if let Err(e) = conn.execute_batch(sql) {
                    assert!(
                        e.to_string()
                            .to_ascii_lowercase()
                            .contains("duplicate column"),
                        "production migration failed under real SQLite: {e}\n{sql}"
                    );
                }
            }
            SqliteSql {
                conn,
                batches: Cell::new(0),
                stmts: Cell::new(0),
                now: Cell::new(1_000_000),
                counted: RefCell::default(),
                lines: RefCell::default(),
            }
        }
        pub(crate) fn counter(&self, name: &str) -> u64 {
            self.counted
                .borrow()
                .iter()
                .filter(|(k, _)| k == name)
                .map(|(_, n)| n)
                .sum()
        }
        fn run(&self, st: &Stmt) -> Result<StmtOut, String> {
            let binds: Vec<rusqlite::types::Value> = st
                .binds
                .iter()
                .map(|b| match b {
                    Bind::Text(t) => rusqlite::types::Value::Text(t.clone()),
                    Bind::Int(n) => rusqlite::types::Value::Integer(*n),
                    Bind::Null => rusqlite::types::Value::Null,
                })
                .collect();
            let mut q = self.conn.prepare(&st.sql).map_err(|e| e.to_string())?;
            let cols: Vec<String> = q.column_names().iter().map(|c| c.to_string()).collect();
            let mut rows = Vec::new();
            {
                let mut it = q
                    .query(rusqlite::params_from_iter(binds.iter()))
                    .map_err(|e| e.to_string())?;
                while let Some(r) = it.next().map_err(|e| e.to_string())? {
                    let mut m = serde_json::Map::new();
                    for (k, c) in cols.iter().enumerate() {
                        let v = match r.get_ref(k).map_err(|e| e.to_string())? {
                            rusqlite::types::ValueRef::Null => Value::Null,
                            rusqlite::types::ValueRef::Integer(n) => json!(n),
                            rusqlite::types::ValueRef::Real(f) => json!(f),
                            rusqlite::types::ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
                            rusqlite::types::ValueRef::Blob(b) => json!(hex::encode(b)),
                        };
                        m.insert(c.clone(), v);
                    }
                    rows.push(Value::Object(m));
                }
            }
            Ok(StmtOut {
                rows,
                changes: self.conn.changes(),
            })
        }
    }

    impl SqlPort for SqliteSql {
        async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
            if stmts.is_empty() {
                return Ok(Vec::new());
            }
            self.batches.set(self.batches.get() + 1);
            self.stmts.set(self.stmts.get() + stmts.len() as u32);
            self.conn.execute_batch("BEGIN").unwrap();
            let mut out = Vec::with_capacity(stmts.len());
            for st in &stmts {
                match self.run(st) {
                    Ok(o) => out.push(o),
                    Err(e) => {
                        self.conn.execute_batch("ROLLBACK").unwrap();
                        return Err(e);
                    }
                }
            }
            self.conn.execute_batch("COMMIT").unwrap();
            Ok(out)
        }
        async fn counters(&self, deltas: Vec<(String, u64)>) {
            self.counted.borrow_mut().extend(deltas);
        }
        fn now_ms(&self) -> u64 {
            self.now.get()
        }
        fn log(&self, line: &str) {
            self.lines.borrow_mut().push(line.to_string());
        }
    }

    /// A seat identity (and its settle key: the fixture's keys are the identities, as the budget seed's are).
    fn seat(side: u8, i: u32) -> String {
        format!("0{side}{}", format!("{:02x}", 0x10 + i).repeat(32))
    }
    fn pot(i: u32) -> (String, u32) {
        (format!("e{i:063x}"), 0)
    }
    const MARKER_POT: &str = "f0000000000000000000000000000000000000000000000000000000000000ff";
    fn marker_seat(k: u32) -> String {
        format!("02{}", format!("{:02x}", 0xc0 + k).repeat(32))
    }

    /// THE FLEET-SHAPED BODY of the review's H2 (8 outpoints, 19 identities): seven decoded pots with two verified
    /// (latched) seats each, one undecoded pot whose party AND refund-backup markers name five identities; every
    /// identity holds an owed-state row.
    pub(crate) fn mixed_fixture() -> (SqliteSql, Vec<(String, u32)>) {
        let db = SqliteSql::migrated();
        let mut sql = Vec::new();
        for i in 0..7 {
            let (txid, _) = pot(i);
            let (a, b) = (seat(2, i), seat(3, i));
            sql.push(format!(
                "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind, paramsDecoded, pubA, pubB, pubTower, payPkhA, payPkhB, rakePkh, stakeA, stakeB, feeSats, recoveryHeight, potSats) \
                 VALUES ('{txid}', 0, 0, 1, 'covenant', 1, '{a}', '{b}', '{A}', '{}', '{}', '{}', 1, 1, 1, 958846, 2)",
                "11".repeat(20), "22".repeat(20), "33".repeat(20)
            ));
            for (k, (me, them)) in [(&a, &b), (&b, &a)].into_iter().enumerate() {
                sql.push(format!(
                    "INSERT INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, txid, outputIndex, createdAt, seatSettlePubkey, seatSigHex, sigValid) \
                     VALUES ('{me}', '{them}', 'g{i}', '{txid}', 0, 958846, '3045ab', 'd{k}{i:062x}', 0, 1, '{me}', '00', 1)"
                ));
            }
        }
        sql.push(format!("INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES ('{MARKER_POT}', 0, 0, 1)"));
        for k in 0..5 {
            let me = marker_seat(k);
            sql.push(format!(
                "INSERT INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, txid, outputIndex, createdAt) \
                 VALUES ('{me}', 'x', 'gm', '{MARKER_POT}', 0, 958846, '3045ab', 'c{k}{:062x}', 0, 1)", 0
            ));
            sql.push(format!(
                "INSERT INTO potrefund_records (potTxid, potVout, gameId, identity, refundRawHex, sigHex, txid, outputIndex, createdAt) \
                 VALUES ('{MARKER_POT}', 0, 'gm', '{me}', '00', '3045ab', 'b{k}{:062x}', 0, 1)", 0
            ));
        }
        let mut ids: Vec<String> = (0..7).flat_map(|i| [seat(2, i), seat(3, i)]).collect();
        ids.extend((0..5).map(marker_seat));
        for id in &ids {
            sql.push(format!(
                "INSERT INTO owed_state (identity, computedAtMs, tip, rows, stale, truncated) VALUES ('{id}', 1, 1, 0, 0, 0)"
            ));
        }
        for q in sql {
            db.conn
                .execute_batch(&q)
                .unwrap_or_else(|e| panic!("{e}: {q}"));
        }
        let mut ops: Vec<(String, u32)> = (0..7).map(pot).collect();
        ops.push((MARKER_POT.to_string(), 0));
        (db, ops)
    }

    /// The relay as a fake: every call answers `Sent` after 10 ms, except a call naming a `hung` txid, which sleeps
    /// 40 s; each call's start and delivery recorded with its target.
    #[derive(Default)]
    pub(crate) struct Relay {
        pub hung: Vec<String>,
        pub started: Cell<u32>,
        pub delivered: RefCell<Vec<(String, String, Instant)>>,
    }
    impl Relay {
        async fn call(&self, to: String, body: Value) -> Delivery {
            self.started.set(self.started.get() + 1);
            let txid = body["potOutpoint"]["txid"]
                .as_str()
                .or(body["outpoint"]["txid"].as_str())
                .or(body["txid"].as_str())
                .unwrap_or_default()
                .to_string();
            let slow =
                self.hung.contains(&txid) && to == crate::internal_events::POTS_ROOM;
            tokio::time::sleep(if slow {
                Duration::from_secs(40)
            } else {
                Duration::from_millis(10)
            })
            .await;
            self.delivered.borrow_mut().push((to, txid, Instant::now()));
            Delivery::Sent
        }
    }
    impl RelayPort for Relay {
        async fn broadcast(&self, room: &'static str, body: Value) -> Delivery {
            self.call(room.to_string(), body).await
        }
        async fn push(&self, recipient: &str, body: Value) -> Delivery {
            self.call(recipient.to_string(), body).await
        }
    }

    /// A recording Worker context: what the handler hands to `wait_until`, kept unpolled until the test runs it.
    #[derive(Default)]
    pub(crate) struct Recorded(pub RefCell<Vec<Detached>>);
    impl Recorded {
        pub(crate) async fn run_all(&self) {
            let all: Vec<Detached> = self.0.borrow_mut().drain(..).collect();
            futures_util::future::join_all(all).await;
        }
    }

    /// (M2 a) THE PRODUCTION HANDLER AWAITS NO RELAY COMPLETION. `serve_pot_changed`, the function the worker's
    /// `internal_pot_changed` calls, run with the real store's SQL over SQLite and a recording `wait_until`, on the
    /// mixed body with the eighth outpoint's room broadcast HUNG (seven fast, one 40 s): the answer is in hand with
    /// NO relay call started and its D1 work in at most three round trips; the announce was handed to `wait_until`;
    /// run after the answer, the seven rooms and fourteen seat events are delivered and the hung one runs into the
    /// wall. Source: the worker's handler calls this function and hands `ctx.wait_until` as `detach`.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f_m2_a_the_production_handler_answers_before_any_relay_call_completes() {
        let h = routes_item("pub(crate) async fn internal_pot_changed(");
        assert!(
            h.contains(&squash(
                "crate::pot_hook::serve_pot_changed(store, relay, wall, &detach, &outpoints, t0)"
            )),
            "the worker's handler is serve_pot_changed"
        );
        assert!(
            h.contains(&squash(
                "let detach = |f: crate::pot_hook::Detached| ctx.wait_until(f);"
            )),
            "detach is ctx.wait_until"
        );
        for banned in [
            "push_broadcast(",
            "first_party_push(",
            "gather_result_entries(",
            "counted_all()",
            "counted_batch(",
        ] {
            assert!(
                !h.contains(&squash(banned)),
                "the pot-changed handler calls {banned} on its answer path"
            );
        }
        let (db, ops) = mixed_fixture();
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(Relay {
            hung: vec![MARKER_POT.to_string()],
            ..Relay::default()
        });
        let rec = Recorded::default();
        let detach = |f: Detached| rec.0.borrow_mut().push(f);
        let t0 = Instant::now();
        let out = serve_pot_changed(
            store.clone(),
            relay.clone(),
            tokio::time::sleep(Duration::from_millis(300)),
            &detach,
            &ops,
            7,
        )
        .await;
        let answered = Instant::now();
        assert!(
            answered - t0 < Duration::from_secs(1),
            "answered in {:?}",
            answered - t0
        );
        assert_eq!(
            relay.started.get(),
            0,
            "relay calls started before the answer"
        );
        assert!(
            store.sql.batches.get() <= 3,
            "the answer's D1 work: {} round trips",
            store.sql.batches.get()
        );
        assert_eq!(
            out.filed.len(),
            14,
            "every verified seat filed: {:?}",
            out.skipped
        );
        assert_eq!(out.owed_identities.len(), 19);
        assert_eq!(
            rec.0.borrow().len(),
            1,
            "the announce was handed to wait_until"
        );
        rec.run_all().await;
        let delivered = relay.delivered.borrow();
        assert!(
            delivered.iter().all(|(_, _, at)| *at > answered),
            "every delivery after the answer"
        );
        assert_eq!(
            delivered.len(),
            7 + 14,
            "seven rooms and fourteen seats delivered"
        );
        assert!(!delivered
            .iter()
            .any(|(to, t, _)| t == MARKER_POT && to == crate::internal_events::POTS_ROOM));
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_SENT), 21);
        assert_eq!(
            store.sql.counter(COUNTER_ANNOUNCE_FAILED),
            1,
            "the hung call counted at the wall"
        );
    }

    /// The relay as a fake: every call sleeps `delay`, then answers `answer`.
    struct SlowRelay {
        delay: Duration,
        answer: Delivery,
    }
    impl SlowRelay {
        fn new(delay: Duration, answer: Delivery) -> Self {
            SlowRelay { delay, answer }
        }
    }
    impl RelayPort for SlowRelay {
        async fn broadcast(&self, _room: &'static str, _body: Value) -> Delivery {
            tokio::time::sleep(self.delay).await;
            self.answer
        }
        async fn push(&self, _recipient: &str, _body: Value) -> Delivery {
            tokio::time::sleep(self.delay).await;
            self.answer
        }
    }

    /// (d) THE DETACHED ANNOUNCE IS COUNTED on success, at the wall, on a failure, and when the relay is not
    /// configured; the counters are the file's `ops_counters` rows, nonzero only.
    #[tokio::test(flavor = "current_thread")]
    async fn e649_hook_d_the_detached_announce_is_counted_on_success_the_wall_and_a_failure() {
        let plan = AnnouncePlan {
            outpoints: vec![op(1), op(2)],
            seat_events: vec![(A.into(), op(1).0, 1)],
            at_ms: 7,
        };
        let wall = || tokio::time::sleep(Duration::from_millis(200));
        let ok = announce(
            &SlowRelay::new(Duration::ZERO, Delivery::Sent),
            &plan,
            wall(),
        )
        .await;
        assert_eq!(
            ok,
            AnnounceTally {
                sent: 3,
                skipped: 0,
                failed: 0,
                walled: false
            }
        );
        assert_eq!(ok.counters(), vec![(COUNTER_ANNOUNCE_SENT.to_string(), 3)]);
        let walled = announce(
            &SlowRelay::new(Duration::from_secs(40), Delivery::Sent),
            &plan,
            wall(),
        )
        .await;
        assert_eq!(
            walled,
            AnnounceTally {
                sent: 0,
                skipped: 0,
                failed: 3,
                walled: true
            }
        );
        assert_eq!(
            walled.counters(),
            vec![(COUNTER_ANNOUNCE_FAILED.to_string(), 3)]
        );
        let failed = announce(
            &SlowRelay::new(Duration::ZERO, Delivery::Failed),
            &plan,
            wall(),
        )
        .await;
        assert_eq!(
            failed,
            AnnounceTally {
                sent: 0,
                skipped: 0,
                failed: 3,
                walled: false
            }
        );
        assert_eq!(
            failed.counters(),
            vec![(COUNTER_ANNOUNCE_FAILED.to_string(), 3)]
        );
        let skipped = announce(
            &SlowRelay::new(Duration::ZERO, Delivery::NotConfigured),
            &plan,
            wall(),
        )
        .await;
        assert_eq!(
            skipped.counters(),
            vec![(COUNTER_ANNOUNCE_SKIPPED.to_string(), 3)]
        );
        assert_eq!(
            [
                COUNTER_ANNOUNCE_SENT,
                COUNTER_ANNOUNCE_SKIPPED,
                COUNTER_ANNOUNCE_FAILED
            ],
            [
                "pot_changed_announce_sent_total",
                "pot_changed_announce_skipped_total",
                "pot_changed_announce_failed_total"
            ]
        );
        const {
            assert!(
                ANNOUNCE_WALL_MS < 30_000,
                "the wall sits under the platform's waitUntil window"
            )
        };
        let h = routes_item("pub(crate) async fn internal_pot_changed(");
        assert!(h.contains(&squash(
            "worker::Delay::from(core::time::Duration::from_millis(crate::pot_hook::ANNOUNCE_WALL_MS,))"
        )));
        let src = squash(&code_only(include_str!("pot_hook.rs")));
        let det = &src[src.find("pubasyncfnannounce_and_report<").unwrap()..];
        assert!(
            det.contains(&squash("store.counters(counters).await;")),
            "the counters are flushed"
        );
    }

    /// The deploy lane reads the hooks' timing: `Server-Timing: hook;dur=<ms>` and one log line per answer.
    #[test]
    fn e649_hook_f_each_hook_answers_its_timing_and_its_log_line() {
        assert_eq!(hook_timing(12), "hook;dur=12");
        for (name, line) in [
            (
                "internal_pot_changed",
                "\"[pot-changed] answered in {ms} ms, announce detached ({} outpoints)\"",
            ),
            (
                "internal_hop_changed",
                "\"[hop-changed] answered in {ms} ms, recompute detached ({} identities)\"",
            ),
        ] {
            let h = routes_item(&format!("pub(crate) async fn {name}("));
            assert!(h.contains(&squash(line)), "{name} logs its answer time");
            assert!(
                h.contains(&squash(
                    ".set(\"Server-Timing\", &crate::pot_hook::hook_timing(ms))?;"
                )),
                "{name} answers Server-Timing"
            );
        }
    }
}
