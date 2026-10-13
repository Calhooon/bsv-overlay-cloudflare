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
/// A marker arm's batch that faulted (H2): the outpoints are attributed from the arms that answered.
pub const COUNTER_MARKER_ARM_FAILED: &str = "pot_changed_marker_arm_failed_total";
/// Relay calls that failed (a transport fault, a non-2xx) or had not answered at the wall.
pub const COUNTER_ANNOUNCE_FAILED: &str = "pot_changed_announce_failed_total";

/// Wake-ups left PARKED after the detached announce (a relay fault, or no answer at the wall): the scheduled drain
/// delivers them (H1).
pub const COUNTER_ANNOUNCE_PARKED: &str = "pot_changed_announce_parked_total";
/// Parked wake-ups the drain delivered.
pub const COUNTER_ANNOUNCE_DRAINED: &str = "pot_changed_announce_drained_total";
/// Parked wake-ups the drain tried and could not deliver (a relay fault, or no answer at the drain's wall).
pub const COUNTER_ANNOUNCE_RETRY_FAILED: &str = "pot_changed_announce_retry_failed_total";
/// Parked wake-ups marked exhausted after [`PARK_MAX_ATTEMPTS`] attempts.
pub const COUNTER_ANNOUNCE_EXHAUSTED: &str = "pot_changed_announce_exhausted_total";
/// Exhausted parked wake-ups swept after [`PARK_SWEEP_AFTER_MS`].
pub const COUNTER_ANNOUNCE_SWEPT: &str = "pot_changed_announce_swept_total";
/// A park, settle or drain statement batch that faulted.
pub const COUNTER_ANNOUNCE_PARK_FAULTS: &str = "pot_changed_announce_park_faults_total";

/// H1: the park of the pots-room wake-ups and the seats' events (overlay migration 222; its index 223 is the
/// overlay's alone, as the catch-up list is CREATE TABLE statements by contract). One row per (outpoint, kind,
/// recipient): `kind` is `room` (the pots-room broadcast, `recipient` empty) or `seat` (a `pot` event into that
/// identity's box). `generation` orders the parks of one key: a later hook's park replaces an earlier one's, and a
/// settle touches only the generation it read.
pub const PARKED_CREATE: &str = "CREATE TABLE IF NOT EXISTS pot_announce_parked (outpoint TEXT NOT NULL, kind TEXT NOT NULL, recipient TEXT NOT NULL, at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at_ms INTEGER NOT NULL, generation INTEGER NOT NULL, exhausted_at_ms INTEGER, PRIMARY KEY (outpoint, kind, recipient))";
/// The drain's order (live rows by `next_attempt_at_ms`) and the sweep's range (exhausted rows by their stamp).
pub const PARKED_DUE_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_pot_announce_parked_due ON pot_announce_parked(exhausted_at_ms, next_attempt_at_ms)";
/// Park (or re-park) one wake-up. A park of an OLDER generation never replaces a newer one.
pub const PARK_SQL: &str = "INSERT INTO pot_announce_parked (outpoint, kind, recipient, at, attempts, next_attempt_at_ms, generation, exhausted_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL) ON CONFLICT (outpoint, kind, recipient) DO UPDATE SET at = excluded.at, attempts = excluded.attempts, next_attempt_at_ms = excluded.next_attempt_at_ms, generation = excluded.generation, exhausted_at_ms = NULL WHERE excluded.generation >= pot_announce_parked.generation";
/// A delivered wake-up leaves the park (only the generation that was delivered).
pub const PARKED_DELIVERED_SQL: &str = "DELETE FROM pot_announce_parked WHERE outpoint = ?1 AND kind = ?2 AND recipient = ?3 AND generation = ?4";
/// A drained wake-up that failed again: its next attempt, or its exhausted stamp.
pub const PARKED_RETRY_SQL: &str = "UPDATE pot_announce_parked SET attempts = ?5, next_attempt_at_ms = ?6, exhausted_at_ms = ?7 WHERE outpoint = ?1 AND kind = ?2 AND recipient = ?3 AND generation = ?4";
/// The drain's read: the live wake-ups due now, oldest due first, bounded.
pub const PARKED_DUE_SQL: &str = "SELECT outpoint, kind, recipient, at, attempts, generation FROM pot_announce_parked WHERE exhausted_at_ms IS NULL AND next_attempt_at_ms <= ?1 ORDER BY next_attempt_at_ms LIMIT ?2";
/// The sweep: exhausted wake-ups older than the bound, bounded per pass.
pub const PARKED_SWEEP_SQL: &str = "DELETE FROM pot_announce_parked WHERE rowid IN (SELECT rowid FROM pot_announce_parked WHERE exhausted_at_ms IS NOT NULL AND exhausted_at_ms < ?1 LIMIT ?2)";

/// A wake-up parked before the answer is not due until the detached announce has had its window (the platform's
/// 30 s `waitUntil`), so the drain does not race it; the announce's settle takes it out or re-parks it.
pub const PARK_GRACE_MS: u64 = 30_000;
/// The first retry of a wake-up that failed; it doubles per attempt up to [`PARK_RETRY_MAX_MS`].
pub const PARK_RETRY_BASE_MS: u64 = 60_000;
pub const PARK_RETRY_MAX_MS: u64 = 3_600_000;
/// Attempts (the detached announce's included) before a wake-up is marked exhausted.
pub const PARK_MAX_ATTEMPTS: u32 = 8;
/// An exhausted wake-up is swept a day after it was marked.
pub const PARK_SWEEP_AFTER_MS: u64 = 86_400_000;
/// The drain's bound per scheduled pass: the wake-ups of one full body (8 rooms, 16 seats).
pub const DRAIN_MAX_PER_PASS: u32 = 24;
/// The sweep's bound per pass.
pub const SWEEP_MAX_PER_PASS: u32 = 50;

/// When attempt `attempts` (1-based) may be retried, from `now`.
pub fn retry_at(now: u64, attempts: u32) -> u64 {
    let shift = attempts.saturating_sub(1).min(16);
    now + (PARK_RETRY_BASE_MS << shift).min(PARK_RETRY_MAX_MS)
}

/// One wake-up of a body: the pots-room broadcast of an outpoint, or one seat's `pot` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wake {
    pub txid: String,
    pub vout: u32,
    /// `None`: the pots room.
    pub recipient: Option<String>,
}

impl Wake {
    fn outpoint(&self) -> String {
        format!("{}:{}", self.txid, self.vout)
    }
    fn kind(&self) -> &'static str {
        if self.recipient.is_some() {
            "seat"
        } else {
            "room"
        }
    }
    fn key_binds(&self) -> Vec<Bind> {
        vec![
            Bind::Text(self.outpoint()),
            Bind::Text(self.kind().to_string()),
            Bind::Text(self.recipient.clone().unwrap_or_default()),
        ]
    }
}

/// One parked row as the drain reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parked {
    pub wake: Wake,
    pub at_ms: u64,
    pub attempts: u32,
    pub generation: u64,
}

/// One statement of the park's write set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParkOp {
    /// Park `wake` of `generation` with `attempts` made, due at `next_ms`.
    Park {
        wake: Wake,
        at_ms: u64,
        attempts: u32,
        next_ms: u64,
        generation: u64,
    },
    Delivered {
        wake: Wake,
        generation: u64,
    },
    Retry {
        wake: Wake,
        generation: u64,
        attempts: u32,
        next_ms: u64,
        exhausted_at_ms: Option<u64>,
    },
    Sweep {
        before_ms: u64,
        limit: u32,
    },
}

impl ParkOp {
    fn stmt(&self) -> Stmt {
        let int = |n: u64| Bind::Int(n as i64);
        match self {
            ParkOp::Park {
                wake,
                at_ms,
                attempts,
                next_ms,
                generation,
            } => {
                let mut b = wake.key_binds();
                b.extend([
                    int(*at_ms),
                    int(u64::from(*attempts)),
                    int(*next_ms),
                    int(*generation),
                ]);
                Stmt::new(PARK_SQL, b)
            }
            ParkOp::Delivered { wake, generation } => {
                let mut b = wake.key_binds();
                b.push(int(*generation));
                Stmt::new(PARKED_DELIVERED_SQL, b)
            }
            ParkOp::Retry {
                wake,
                generation,
                attempts,
                next_ms,
                exhausted_at_ms,
            } => {
                let mut b = wake.key_binds();
                b.extend([
                    int(*generation),
                    int(u64::from(*attempts)),
                    int(*next_ms),
                    exhausted_at_ms.map(int).unwrap_or(Bind::Null),
                ]);
                Stmt::new(PARKED_RETRY_SQL, b)
            }
            ParkOp::Sweep { before_ms, limit } => Stmt::new(
                PARKED_SWEEP_SQL,
                vec![int(*before_ms), int(u64::from(*limit))],
            ),
        }
    }
}

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
    fn nonce(&self) -> u64;
    fn log(&self, line: &str);
}

/// The D1 side of the hooks. The worker's is [`HookD1`] over `routes::D1Sql`.
#[allow(async_fn_in_trait)]
pub trait HookStore {
    /// Every outpoint's attribution, in the order of `outpoints`.
    async fn read_outpoints(&self, outpoints: &[(String, u32)]) -> Vec<OutpointRead>;
    /// Every stale mark and every park statement given, in ONE batch (the hook's write; the settle's; the
    /// drain's). Answers the rows each park statement changed, in order (the sweep's count).
    async fn write(
        &self,
        marks: &[(String, &'static str)],
        ops: &[ParkOp],
    ) -> Result<Vec<u64>, String>;
    /// The parked wake-ups due at `now`, oldest due first, at most `limit`.
    async fn due_parked(&self, now: u64, limit: u32) -> Result<Vec<Parked>, String>;
    async fn counters(&self, deltas: Vec<(String, u64)>);
    fn now_ms(&self) -> u64;
    /// 0 to 999: the low digits of a park's generation (two hooks of one millisecond do not share one).
    fn nonce(&self) -> u64;
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
        let binds =
            |txid: &str, vout: u32| vec![Bind::Text(txid.to_string()), Bind::Int(i64::from(vout))];
        // (1) THE REQUIRED READ, one batch: every outpoint's decoded row. D1 fails a batch whole, so on a fault each
        // outpoint is asked again alone and only the one whose own statement faults is `Failed` (H2).
        let decoded_sql = crate::results::decoded_pots_sql(1);
        let stmts = outpoints
            .iter()
            .map(|(txid, vout)| Stmt::new(decoded_sql.clone(), binds(txid, *vout)))
            .collect();
        let decoded: Vec<Result<Option<crate::results::CovenantParams>, String>> =
            match self.sql.run_batch(stmts).await {
                Ok(results) => results.iter().map(decoded_of).collect(),
                Err(e) => {
                    self.sql.log(&format!(
                        "[pot-changed] decoded-pots batch failed ({e}): each outpoint asked alone"
                    ));
                    let mut each = Vec::with_capacity(outpoints.len());
                    for (txid, vout) in outpoints {
                        let one = vec![Stmt::new(decoded_sql.clone(), binds(txid, *vout))];
                        each.push(match self.sql.run_batch(one).await {
                            Ok(r) => r.first().map(decoded_of).unwrap_or(Ok(None)),
                            Err(e) => Err(e),
                        });
                    }
                    each
                }
            };
        // (2) one batch: the decoded pots' seat markers, then `attribute_seats` per pot.
        let mut params_by_pot = std::collections::HashMap::new();
        for ((txid, vout), d) in outpoints.iter().zip(&decoded) {
            if let Ok(Some(p)) = d {
                params_by_pot.insert((txid.clone(), *vout), p.clone());
            }
        }
        let markers = self.seat_markers(&params_by_pot).await;
        let seats: Vec<Option<Vec<String>>> = outpoints
            .iter()
            .zip(&decoded)
            .map(|((txid, vout), d)| match d {
                Ok(Some(params)) => Some(seats_of(params, txid, *vout, &markers)),
                _ => None,
            })
            .collect();
        // (3) THE OPTIONAL MARKER ARMS, only for an outpoint that needs them (no decoded params, or decoded with no
        // attributed seat), as before L4: one batch PER ARM over those outpoints. An arm that faults names nobody
        // for this body, is counted and logged by name, and the other arms and the outpoint stand (H2).
        let needs: Vec<usize> = (0..outpoints.len())
            .filter(|&i| match (&decoded[i], &seats[i]) {
                (Ok(None), _) => true,
                (Ok(Some(_)), Some(v)) => v.is_empty(),
                _ => false,
            })
            .collect();
        let mut named: Vec<Vec<String>> = vec![Vec::new(); outpoints.len()];
        let mut arm_faults = 0u64;
        if !needs.is_empty() {
            for (arm, sql) in MARKER_ARMS {
                let stmts = needs
                    .iter()
                    .map(|&i| Stmt::new(sql, binds(&outpoints[i].0, outpoints[i].1)))
                    .collect();
                match self.sql.run_batch(stmts).await {
                    Ok(results) => {
                        for (&i, r) in needs.iter().zip(&results) {
                            identity_rows(&r.rows, &mut named[i]);
                        }
                    }
                    Err(e) => {
                        arm_faults += 1;
                        self.sql.log(&format!(
                            "[pot-changed] marker arm {arm} failed for {} outpoint(s): {e}",
                            needs.len()
                        ));
                    }
                }
            }
        }
        if arm_faults > 0 {
            self.sql
                .counters(vec![(COUNTER_MARKER_ARM_FAILED.to_string(), arm_faults)])
                .await;
        }
        decoded
            .into_iter()
            .zip(seats)
            .zip(named)
            .map(|((d, seats), marker_named)| match d {
                Err(e) => OutpointRead::Failed(e),
                Ok(None) => OutpointRead::NoParams { marker_named },
                Ok(Some(_)) => OutpointRead::Decoded {
                    seats: seats.unwrap_or_default(),
                    marker_named,
                },
            })
            .collect()
    }

    async fn write(
        &self,
        marks: &[(String, &'static str)],
        ops: &[ParkOp],
    ) -> Result<Vec<u64>, String> {
        let mut stmts: Vec<Stmt> = marks
            .iter()
            .map(|(id, _)| {
                Stmt::new(
                    crate::owed::OWED_STALE_FOR_IDENTITY_SQL,
                    vec![Bind::Text(id.clone())],
                )
            })
            .collect();
        stmts.extend(ops.iter().map(ParkOp::stmt));
        let out = self.sql.run_batch(stmts).await?;
        Ok(out[marks.len()..].iter().map(|o| o.changes).collect())
    }

    async fn due_parked(&self, now: u64, limit: u32) -> Result<Vec<Parked>, String> {
        let out = self
            .sql
            .run_batch(vec![Stmt::new(
                PARKED_DUE_SQL,
                vec![Bind::Int(now as i64), Bind::Int(i64::from(limit))],
            )])
            .await?;
        let mut parked = Vec::new();
        for r in out.first().map(|o| o.rows.as_slice()).unwrap_or(&[]) {
            let text = |k: &str| {
                r.get(k)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            };
            let num = |k: &str| r.get(k).and_then(Value::as_f64).unwrap_or(0.0) as u64;
            let outpoint = text("outpoint");
            let Some((txid, vout)) = outpoint.rsplit_once(':') else {
                continue;
            };
            let Ok(vout) = vout.parse() else { continue };
            let recipient = text("recipient");
            parked.push(Parked {
                wake: Wake {
                    txid: txid.to_string(),
                    vout,
                    recipient: (text("kind") == "seat").then_some(recipient),
                },
                at_ms: num("at"),
                attempts: num("attempts") as u32,
                generation: num("generation"),
            });
        }
        Ok(parked)
    }

    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.sql.counters(deltas).await
    }
    fn now_ms(&self) -> u64 {
        self.sql.now_ms()
    }
    fn nonce(&self) -> u64 {
        self.sql.nonce()
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
    /// The generation its parks carry.
    pub generation: u64,
}

impl AnnouncePlan {
    pub fn calls(&self) -> u64 {
        (self.outpoints.len() + self.seat_events.len()) as u64
    }
    /// Every wake-up of the plan: the rooms, then the seats.
    pub fn wakes(&self) -> Vec<Wake> {
        let rooms = self.outpoints.iter().map(|(txid, vout)| Wake {
            txid: txid.clone(),
            vout: *vout,
            recipient: None,
        });
        let seats = self.seat_events.iter().map(|(id, txid, vout)| Wake {
            txid: txid.clone(),
            vout: *vout,
            recipient: Some(id.clone()),
        });
        rooms.chain(seats).collect()
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
    /// The hook's write batch (the stale marks and the parks) faulted: nothing of it landed, and the answer says so.
    pub stale_fault: Option<String>,
    /// Wake-ups parked durably before the answer (H1).
    pub parked: usize,
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
            generation: at_ms * 1000 + store.nonce() % 1000,
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
    // H1: every wake-up of the plan is PARKED in the same write batch as the stale marks, BEFORE the answer, not
    // due until the detached announce has had its window; the announce's settle takes out what it delivered.
    let parks: Vec<ParkOp> = out
        .plan
        .wakes()
        .into_iter()
        .map(|wake| ParkOp::Park {
            wake,
            at_ms,
            attempts: 0,
            next_ms: at_ms + PARK_GRACE_MS,
            generation: out.plan.generation,
        })
        .collect();
    match store.write(&out.stale_marks, &parks).await {
        Ok(_) => out.parked = parks.len(),
        Err(e) => out.stale_fault = Some(e),
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
    store.write(&marks, &[]).await.map(|_| ())
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
    /// Wake-ups left parked for the drain (every failed one).
    pub parked: u64,
    /// The settle's write faulted.
    pub park_faults: u64,
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
            (COUNTER_ANNOUNCE_PARKED, self.parked),
            (COUNTER_ANNOUNCE_PARK_FAULTS, self.park_faults),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| (k.to_string(), n))
        .collect()
    }
}

/// Deliver `wakes` together (the calls of one body run together, not one after another), all raced against ONE
/// `wall`: each wake-up's answer, `None` for a call that had not answered at the wall (dropped).
pub async fn deliver<R: RelayPort, W: Future<Output = ()>>(
    relay: &R,
    wakes: &[Wake],
    at_ms: u64,
    wall: W,
) -> Vec<Option<Delivery>> {
    use futures_util::future::{join_all, select};
    let got: RefCell<Vec<Option<Delivery>>> = RefCell::new(vec![None; wakes.len()]);
    let calls = wakes.iter().enumerate().map(|(i, w)| {
        let got = &got;
        async move {
            let d = match &w.recipient {
                None => {
                    let body =
                        crate::internal_events::pot_changed_event_body(&w.txid, w.vout, at_ms);
                    relay
                        .broadcast(crate::internal_events::POTS_ROOM, body)
                        .await
                }
                Some(id) => {
                    let body = crate::internal_events::pot_event_body(
                        &w.txid,
                        w.vout,
                        carrier_entry(),
                        at_ms,
                    );
                    relay.push(id, body).await
                }
            };
            got.borrow_mut()[i] = Some(d);
        }
    });
    {
        let all = join_all(calls);
        futures_util::pin_mut!(all, wall);
        let _ = select(all, wall).await;
    }
    got.into_inner()
}

/// The detached announce of one body: every changed outpoint into the pots room (the deployed client matches
/// `potOutpoint` alone, so one body per outpoint) and one `pot` event into each attributed seat's box, under ONE
/// `wall`. A call that has not answered at the wall is dropped and counted failed.
pub async fn announce<R: RelayPort, W: Future<Output = ()>>(
    relay: &R,
    plan: &AnnouncePlan,
    wall: W,
) -> AnnounceTally {
    let got = deliver(relay, &plan.wakes(), plan.at_ms, wall).await;
    let mut t = AnnounceTally::default();
    for d in got {
        match d {
            Some(d) => t.note(d),
            None => {
                t.failed += 1;
                t.walled = true;
            }
        }
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

/// The detached announce of one body, then its SETTLE (H1): a delivered wake-up (or one the deploy cannot send)
/// leaves the park; one that failed or had not answered at the wall is re-parked for the drain (attempt 1, due after
/// [`PARK_RETRY_BASE_MS`]), so a park the answer's write could not make is made here. One write batch; its log line
/// and its counters.
pub async fn announce_and_report<S: HookStore, R: RelayPort, W: Future<Output = ()>>(
    store: &S,
    relay: &R,
    plan: &AnnouncePlan,
    wall: W,
) -> AnnounceTally {
    let t0 = store.now_ms();
    let wakes = plan.wakes();
    let got = deliver(relay, &wakes, plan.at_ms, wall).await;
    let now = store.now_ms();
    let mut t = AnnounceTally::default();
    let mut ops = Vec::with_capacity(wakes.len());
    for (wake, d) in wakes.into_iter().zip(got) {
        match d {
            Some(d @ (Delivery::Sent | Delivery::NotConfigured)) => {
                t.note(d);
                ops.push(ParkOp::Delivered {
                    wake,
                    generation: plan.generation,
                });
            }
            failed => {
                t.failed += 1;
                t.walled |= failed.is_none();
                t.parked += 1;
                ops.push(ParkOp::Park {
                    wake,
                    at_ms: plan.at_ms,
                    attempts: 1,
                    next_ms: retry_at(now, 1),
                    generation: plan.generation,
                });
            }
        }
    }
    if !ops.is_empty() {
        if let Err(e) = store.write(&[], &ops).await {
            t.park_faults += 1;
            store.log(&format!(
                "[pot-changed] announce settle failed ({} wake-up(s)): {e}",
                ops.len()
            ));
        }
    }
    store.log(&format!(
        "[pot-changed] announce: {} outpoint(s), {} seat event(s): sent={} skipped={} failed={} parked={}{} in {} ms",
        plan.outpoints.len(),
        plan.seat_events.len(),
        t.sent,
        t.skipped,
        t.failed,
        t.parked,
        if t.walled { " (WALL)" } else { "" },
        store.now_ms().saturating_sub(t0)
    ));
    let counters = t.counters();
    if !counters.is_empty() {
        store.counters(counters).await;
    }
    t
}

/// What one drain pass came to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DrainTally {
    pub due: u64,
    pub drained: u64,
    pub skipped: u64,
    pub retry_failed: u64,
    pub exhausted: u64,
    pub swept: u64,
    pub faults: u64,
}

impl DrainTally {
    pub fn counters(&self) -> Vec<(String, u64)> {
        [
            (COUNTER_ANNOUNCE_DRAINED, self.drained),
            (COUNTER_ANNOUNCE_SKIPPED, self.skipped),
            (COUNTER_ANNOUNCE_RETRY_FAILED, self.retry_failed),
            (COUNTER_ANNOUNCE_EXHAUSTED, self.exhausted),
            (COUNTER_ANNOUNCE_SWEPT, self.swept),
            (COUNTER_ANNOUNCE_PARK_FAULTS, self.faults),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| (k.to_string(), n))
        .collect()
    }
}

/// ONE DRAIN PASS (the scheduled tick's, and `POST /internal/pot-announce-drain`'s): at most
/// [`DRAIN_MAX_PER_PASS`] parked wake-ups due now, delivered together under `wall`; a delivered one leaves the park,
/// a failed one is retried later (doubling from [`PARK_RETRY_BASE_MS`] to [`PARK_RETRY_MAX_MS`]) and marked
/// exhausted at its [`PARK_MAX_ATTEMPTS`]th attempt; exhausted rows older than [`PARK_SWEEP_AFTER_MS`] are swept, at
/// most [`SWEEP_MAX_PER_PASS`]. Two D1 round trips: the due read, then one write batch (the settle and the sweep).
pub async fn drain_parked<S: HookStore, R: RelayPort, W: Future<Output = ()>>(
    store: &S,
    relay: &R,
    wall: W,
) -> DrainTally {
    let now = store.now_ms();
    let mut t = DrainTally::default();
    let due = match store.due_parked(now, DRAIN_MAX_PER_PASS).await {
        Ok(d) => d,
        Err(e) => {
            t.faults += 1;
            store.log(&format!("[pot-changed] drain read failed: {e}"));
            store.counters(t.counters()).await;
            return t;
        }
    };
    t.due = due.len() as u64;
    let mut ops = Vec::with_capacity(due.len() + 1);
    let got = if due.is_empty() {
        Vec::new()
    } else {
        deliver_each_at(relay, &due, wall).await
    };
    let now = store.now_ms();
    for (p, d) in due.into_iter().zip(got) {
        match d {
            Some(d @ (Delivery::Sent | Delivery::NotConfigured)) => {
                if d == Delivery::Sent {
                    t.drained += 1;
                } else {
                    t.skipped += 1;
                }
                ops.push(ParkOp::Delivered {
                    wake: p.wake,
                    generation: p.generation,
                });
            }
            _ => {
                let attempts = p.attempts + 1;
                let exhausted = attempts >= PARK_MAX_ATTEMPTS;
                if exhausted {
                    t.exhausted += 1;
                } else {
                    t.retry_failed += 1;
                }
                ops.push(ParkOp::Retry {
                    wake: p.wake,
                    generation: p.generation,
                    attempts,
                    next_ms: retry_at(now, attempts),
                    exhausted_at_ms: exhausted.then_some(now),
                });
            }
        }
    }
    ops.push(ParkOp::Sweep {
        before_ms: now.saturating_sub(PARK_SWEEP_AFTER_MS),
        limit: SWEEP_MAX_PER_PASS,
    });
    match store.write(&[], &ops).await {
        Ok(changes) => t.swept = changes.last().copied().unwrap_or(0),
        Err(e) => {
            t.faults += 1;
            store.log(&format!("[pot-changed] drain settle failed: {e}"));
        }
    }
    store.log(&format!(
        "[pot-changed] drain: due={} drained={} skipped={} retry_failed={} exhausted={} swept={} faults={}",
        t.due, t.drained, t.skipped, t.retry_failed, t.exhausted, t.swept, t.faults
    ));
    let counters = t.counters();
    if !counters.is_empty() {
        store.counters(counters).await;
    }
    t
}

/// The drain's delivery: each parked wake-up with the `at` it was announced at, all together under the one wall.
async fn deliver_each_at<R: RelayPort, W: Future<Output = ()>>(
    relay: &R,
    due: &[Parked],
    wall: W,
) -> Vec<Option<Delivery>> {
    use futures_util::future::{join_all, select};
    let got: RefCell<Vec<Option<Delivery>>> = RefCell::new(vec![None; due.len()]);
    let calls = due.iter().enumerate().map(|(i, p)| {
        let got = &got;
        async move {
            let mut one = deliver(
                relay,
                std::slice::from_ref(&p.wake),
                p.at_ms,
                std::future::pending::<()>(),
            )
            .await;
            got.borrow_mut()[i] = one.pop().flatten();
        }
    });
    {
        let all = join_all(calls);
        futures_util::pin_mut!(all, wall);
        let _ = select(all, wall).await;
    }
    got.into_inner()
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
        fn nonce(&self) -> u64 {
            self.stmts.get() as u64 % 1000
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
            let slow = self.hung.contains(&txid) && to == crate::internal_events::POTS_ROOM;
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
    /// NO relay call started and its D1 work in six round trips (H2: the required batch, the
    /// seat markers, one batch per marker arm, one write); the announce was handed to `wait_until`;
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
        assert_eq!(
            store.sql.batches.get(),
            6,
            "the answer's D1 work: the decoded rows, the seat markers, one batch per marker arm (the marker-only \
             outpoint needs them), one write"
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

    fn parked_rows(db: &SqliteSql) -> Vec<(String, String, String, i64, Option<i64>)> {
        let mut q = db
            .conn
            .prepare("SELECT outpoint, kind, recipient, attempts, exhausted_at_ms FROM pot_announce_parked ORDER BY outpoint, kind, recipient")
            .unwrap();
        q.query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    /// (H1, M2 b) NO WAKE-UP IS LOST. The production handler over the real store's SQL, the mixed body, the eighth
    /// room broadcast hung past the wall: every wake-up (8 rooms, 14 seats) is PARKED in D1 before the answer, in
    /// the hook's one write batch; the detached announce runs after the answer, and its settle leaves exactly the
    /// hung wake-up parked (attempt 1), counted. The drain is not early (the retry is not due), then delivers it
    /// one pass later and empties the park. A wake-up that keeps failing is marked exhausted at its 8th attempt
    /// and swept a day after; every step counted.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f_h1_the_wall_parks_the_unfinished_wake_up_and_the_drain_delivers_it() {
        let (db, ops) = mixed_fixture();
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(Relay {
            hung: vec![MARKER_POT.to_string()],
            ..Relay::default()
        });
        let rec = Recorded::default();
        let detach = |f: Detached| rec.0.borrow_mut().push(f);
        let at = store.sql.now.get();
        let out = serve_pot_changed(
            store.clone(),
            relay.clone(),
            tokio::time::sleep(Duration::from_millis(300)),
            &detach,
            &ops,
            at,
        )
        .await;
        assert_eq!(out.stale_fault, None);
        assert_eq!(relay.started.get(), 0, "nothing sent before the answer");
        let before = parked_rows(&store.sql);
        assert_eq!(
            (out.parked, before.len()),
            (22, 22),
            "the wake-ups are parked before the answer"
        );
        assert_eq!(
            before.iter().filter(|r| r.1 == "room").count(),
            8,
            "every parsed outpoint's room wake-up"
        );
        rec.run_all().await;
        let after = parked_rows(&store.sql);
        assert_eq!(
            after,
            vec![(
                format!("{MARKER_POT}:0"),
                "room".to_string(),
                String::new(),
                1,
                None
            )],
            "the wall leaves the hung wake-up parked, the delivered ones settled"
        );
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_PARKED), 1);
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_SENT), 21);

        let quick = Relay::default();
        let t = drain_parked(&*store, &quick, tokio::time::sleep(Duration::from_secs(5))).await;
        assert_eq!((t.due, t.drained), (0, 0), "not due before its retry");
        store.sql.now.set(store.sql.now.get() + PARK_RETRY_BASE_MS);
        let t = drain_parked(&*store, &quick, tokio::time::sleep(Duration::from_secs(5))).await;
        assert_eq!(
            (t.due, t.drained, t.faults),
            (1, 1, 0),
            "the drain delivers it one pass later"
        );
        assert!(quick
            .delivered
            .borrow()
            .iter()
            .any(|(to, txid, _)| to == crate::internal_events::POTS_ROOM && txid == MARKER_POT));
        assert!(
            parked_rows(&store.sql).is_empty(),
            "delivered: the park is empty"
        );
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_DRAINED), 1);

        // a wake-up that keeps failing: exhausted at its 8th attempt, swept a day later
        let wake = Wake {
            txid: MARKER_POT.to_string(),
            vout: 0,
            recipient: Some(marker_seat(0)),
        };
        let now = store.sql.now.get();
        store
            .write(
                &[],
                &[ParkOp::Park {
                    wake,
                    at_ms: now,
                    attempts: 1,
                    next_ms: now,
                    generation: 5,
                }],
            )
            .await
            .unwrap();
        let failing = SlowRelay::new(Duration::ZERO, Delivery::Failed);
        for _ in 0..PARK_MAX_ATTEMPTS - 1 {
            let t = drain_parked(
                &*store,
                &failing,
                tokio::time::sleep(Duration::from_secs(5)),
            )
            .await;
            assert_eq!(t.due, 1);
            store.sql.now.set(store.sql.now.get() + PARK_RETRY_MAX_MS);
        }
        let rows = parked_rows(&store.sql);
        assert_eq!((rows.len(), rows[0].3), (1, i64::from(PARK_MAX_ATTEMPTS)));
        assert!(rows[0].4.is_some(), "exhausted at its 8th attempt");
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_EXHAUSTED), 1);
        assert_eq!(
            store.sql.counter(COUNTER_ANNOUNCE_RETRY_FAILED),
            u64::from(PARK_MAX_ATTEMPTS) - 2
        );
        let t = drain_parked(
            &*store,
            &failing,
            tokio::time::sleep(Duration::from_secs(5)),
        )
        .await;
        assert_eq!(
            (t.due, t.swept),
            (0, 0),
            "an exhausted wake-up is not retried, nor swept early"
        );
        store.sql.now.set(store.sql.now.get() + PARK_SWEEP_AFTER_MS);
        let t = drain_parked(
            &*store,
            &failing,
            tokio::time::sleep(Duration::from_secs(5)),
        )
        .await;
        assert_eq!(t.swept, 1, "swept a day after");
        assert!(parked_rows(&store.sql).is_empty());
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_SWEPT), 1);
    }

    /// (H1) A LATER HOOK'S PARK IS NEVER UNDONE BY AN EARLIER ONE'S SETTLE: the settle and the drain touch only the
    /// generation they hold.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f_h1_a_settle_touches_only_its_own_generation() {
        let store = HookD1 {
            sql: SqliteSql::migrated(),
        };
        let wake = Wake {
            txid: MARKER_POT.to_string(),
            vout: 0,
            recipient: None,
        };
        let park = |generation: u64| ParkOp::Park {
            wake: wake.clone(),
            at_ms: 1,
            attempts: 0,
            next_ms: 1,
            generation,
        };
        store.write(&[], &[park(2000)]).await.unwrap();
        store.write(&[], &[park(1000)]).await.unwrap();
        store
            .write(
                &[],
                &[ParkOp::Delivered {
                    wake: wake.clone(),
                    generation: 1000,
                }],
            )
            .await
            .unwrap();
        let rows = store.due_parked(10, 24).await.unwrap();
        assert_eq!(
            rows.len(),
            1,
            "the older park neither replaced nor deleted the newer"
        );
        assert_eq!(rows[0].generation, 2000);
        assert_eq!(rows[0].wake, wake);
    }

    /// (H2, M2 c) AN OPTIONAL MARKER ARM'S FAULT LEAVES THE OTHER SEATS FILED. The review's case through the
    /// production handler and the real store's SQL: the mixed body with `potrefund_records` unavailable (renamed
    /// away under SQLite, so the shipped statement fails to prepare as on a D1 that faults it). The fourteen
    /// verified seats are filed, the five marker identities (named by the party arm, which answered) are marked
    /// stale in D1 and queued, and the faulted arm is counted and logged by name; nothing is `Failed`.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f_h2_an_optional_arm_fault_leaves_the_other_seats_filed() {
        let (db, ops) = mixed_fixture();
        db.conn
            .execute_batch("ALTER TABLE potrefund_records RENAME TO potrefund_records_away")
            .unwrap();
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(Relay::default());
        let rec = Recorded::default();
        let detach = |f: Detached| rec.0.borrow_mut().push(f);
        let out = serve_pot_changed(
            store.clone(),
            relay,
            tokio::time::sleep(Duration::from_secs(5)),
            &detach,
            &ops,
            7,
        )
        .await;
        let filed: Vec<&str> = out
            .filed
            .iter()
            .filter_map(|f| f["identity"].as_str())
            .collect();
        assert_eq!(filed.len(), 14, "filed={filed:?} skipped={:?}", out.skipped);
        assert_eq!(
            out.owed_identities.len(),
            19,
            "every identity queued for its recompute"
        );
        assert_eq!(
            out.stale_marks.len(),
            5,
            "the marker identities marked stale"
        );
        assert_eq!(out.stale_fault, None);
        let stale: i64 = store
            .sql
            .conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM owed_state WHERE stale = 1 AND identity IN ({})",
                    (0..5)
                        .map(|k| format!("'{}'", marker_seat(k)))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stale, 5, "the stale marks landed in D1");
        assert_eq!(
            store.sql.counter(COUNTER_MARKER_ARM_FAILED),
            1,
            "the faulted arm counted once"
        );
        assert!(
            store
                .sql
                .lines
                .borrow()
                .iter()
                .any(|l| l.contains("marker arm potrefund failed")),
            "the faulted arm logged by name: {:?}",
            store.sql.lines.borrow()
        );
        rec.run_all().await;
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
            generation: 7000,
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
                walled: false,
                parked: 0,
                park_faults: 0,
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
                walled: true,
                parked: 0,
                park_faults: 0,
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
                walled: false,
                parked: 0,
                park_faults: 0,
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
