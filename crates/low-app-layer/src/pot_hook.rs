//! E649 L4 (the E649 north-star plan, lane E649-HOOK, 2026-10-13): the write hooks `/internal/pot-changed` and
//! `/internal/hop-changed` are WRITES. Beta measured the pot hook at 15.3 s p50 and 30 s p90 (Observability,
//! 12:05:30 to 12:06:30Z): step 0 awaited one relay `/broadcast` per outpoint, whose answer waits on the relay's
//! sequential fan-out (11 to 15 s p50 under the fleet), then a full `/results` gather per attributed seat.
//!
//! The rule here: the answer path is D1 alone, in batches; no relay call and no read-model walk is awaited before
//! the answer. The worker's handler is [`serve_pot_changed`]: what the hook decides is [`apply_pot_changed`] /
//! [`apply_hop_changed`], over a [`HookStore`] port (the worker's is [`HookD1`] over `routes::D1Sql`; a queue
//! consumer is the next caller, L3's seam). The publish is VOLATILE scheduling, the filing DURABLE: every wake-up of
//! a body is PARKED in D1 in the hook's one write batch before the answer (the review's H1); [`announce_and_report`]
//! runs DETACHED (`ctx.wait_until`) under one wall ([`ANNOUNCE_WALL_MS`]), counted, and settles the park (a
//! delivered wake-up out, an undelivered one retried at its token); [`drain_parked`] (the scheduled tick's own
//! task, and `POST /internal/pot-announce-drain`) delivers what is left. A batch that faulted announces nothing (the
//! 503 alone, N2). THE DELIVERY BOUND (the delta review's N3, N4), conditional on the relay answering within the
//! walls and the scheduled task running: a wake-up is sent within [`ANNOUNCE_WALL_MS`] of the answer, or else it is
//! due [`PARK_RETRY_BASE_MS`] later and waits behind the B wake-ups due before it, delivered by the
//! ceil((B + 1) / 96)-th scheduled pass after it falls due ([`DRAIN_MAX_ROUNDS`] x [`DRAIN_MAX_PER_PASS`] a pass, one
//! pass every 15 minutes); B is at most [`PARK_MAX_ROWS`] - 1, so at most 22 passes (5.5 hours) on a full table. A
//! failed retry backs off (1 minute doubling to an hour) and is exhausted at its 8th attempt; a wake-up due for a day
//! is aged out. A wake-up is NEVER acknowledged unparked (the second delta review's N6): a batch that would pass the
//! ceiling first makes room (the oldest exhausted rows, then the oldest live rows that failed at least
//! [`PARK_DISPLACE_MIN_ATTEMPTS`] deliveries, each displaced one named LOST in the log and counted); past that the
//! WHOLE batch rolls back and the hook answers 503, nothing announced, the refusal counted. The owed recompute stays
//! where #469 and #648 put it: after the answer, coalesced and shed per identity, not parked.
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
/// N3, N6: wake-ups of a body the park REFUSED at the table's ceiling ([`PARK_MAX_ROWS`]) with no room to make: the
/// whole batch rolled back, the hook answered 503, nothing announced.
pub const COUNTER_ANNOUNCE_PARK_FULL: &str = "pot_changed_announce_park_full_total";
/// N3: live wake-ups aged out, due for longer than [`PARK_OVERDUE_MAX_MS`].
pub const COUNTER_ANNOUNCE_AGED: &str = "pot_changed_announce_aged_total";
/// N6: exhausted rows deleted early to make room for a new body's wake-ups (already given up: no wake-up is lost).
pub const COUNTER_ANNOUNCE_EVICTED_EXHAUSTED: &str = "pot_changed_announce_evicted_exhausted_total";
/// N6: LIVE wake-ups displaced to make room (each failed at least [`PARK_DISPLACE_MIN_ATTEMPTS`] deliveries), each
/// named LOST in the log; served on the overlay's `/health/invariants.counters` with every counter row.
pub const COUNTER_ANNOUNCE_DISPLACED: &str = "pot_changed_announce_displaced_total";

/// H1: the park of the pots-room wake-ups and the seats' events (overlay migration 229; its index 230 is the
/// overlay's alone, as the catch-up list is CREATE TABLE statements by contract). One row per (outpoint, kind,
/// recipient): `kind` is `room` (the pots-room broadcast, `recipient` empty) or `seat` (a `pot` event into that
/// identity's box). `generation` is the park's TOKEN (N1 of the delta review): taken from [`PARK_SEQ_CREATE`]'s one
/// counter in the park's own batch, so it is ordered by commit and never reused, whatever the clock; a later park of
/// a key replaces an earlier one's, and a settle or a drain touches only the token it holds.
pub const PARKED_CREATE: &str = "CREATE TABLE IF NOT EXISTS pot_announce_parked (outpoint TEXT NOT NULL, kind TEXT NOT NULL, recipient TEXT NOT NULL, at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at_ms INTEGER NOT NULL, generation INTEGER NOT NULL, exhausted_at_ms INTEGER, PRIMARY KEY (outpoint, kind, recipient))";
/// The drain's order (live rows by `next_attempt_at_ms`) and the sweep's range (exhausted rows by their stamp).
pub const PARKED_DUE_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_pot_announce_parked_due ON pot_announce_parked(exhausted_at_ms, next_attempt_at_ms)";
/// N1: the park tokens' counter (overlay migration 231; in the app's catch-up list too). One row, `id` 1: `n` is the
/// last token handed out, `room` the table's room under the ceiling after the last batch's parks (N6: negative, the
/// excess that batch made room for). Never deleted.
pub const PARK_SEQ_CREATE: &str = "CREATE TABLE IF NOT EXISTS pot_announce_seq (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL, room INTEGER NOT NULL)";
/// Park one wake-up (a NEW intent), a park statement of a write batch: its token is the counter's last plus its
/// position `?6` in the batch (1-based; `?7`, the clock, times 1,000 on a first counter row, above every token handed
/// out before at under 1,000 a millisecond and above the clock-stamped generations parked before the counter existed),
/// its attempts from 0, due at `?5`. [`PARK_SEQ_BUMP_SQL`] then moves the counter past the batch; D1 runs a batch as
/// one transaction, so no other batch takes a token between them (N1). A key already parked is replaced by the later
/// token (a row exhausted comes back). Every park of a batch is written (N6 of the second delta review): the ceiling
/// is held after them, for the batch as a whole ([`PARK_GUARD_SQL`], [`PARK_MAKE_ROOM_SQL`]), so a batch parks EVERY
/// wake-up or none. The row written answers its token (`RETURNING`).
pub const PARK_SQL: &str = "INSERT INTO pot_announce_parked (outpoint, kind, recipient, at, attempts, next_attempt_at_ms, generation, exhausted_at_ms) SELECT ?1, ?2, ?3, ?4, 0, ?5, COALESCE((SELECT n FROM pot_announce_seq WHERE id = 1), ?7 * 1000) + ?6, NULL WHERE true ON CONFLICT (outpoint, kind, recipient) DO UPDATE SET at = excluded.at, attempts = 0, next_attempt_at_ms = excluded.next_attempt_at_ms, generation = excluded.generation, exhausted_at_ms = NULL WHERE excluded.generation > pot_announce_parked.generation RETURNING generation";
/// N1, N6: the statement after a batch's `?1` parks: the counter moves past them (`n` from `n0` to `n0 + ?1`, a first
/// row at the clock `?2` times 1,000 plus `?1`, as [`PARK_SQL`] read it), and `room` is the table's room under the
/// ceiling `?3` NOW, the parks counted (one LIMITed scan, at most `?3 + ?1` rows: the table held at most `?3` before
/// the batch); negative, it is the excess the batch must make room for.
pub const PARK_SEQ_BUMP_SQL: &str = "INSERT INTO pot_announce_seq (id, n, room) VALUES (1, ?2 * 1000 + ?1, ?3 - (SELECT COUNT(*) FROM (SELECT 1 FROM pot_announce_parked LIMIT ?3 + ?1))) ON CONFLICT (id) DO UPDATE SET n = n + ?1, room = excluded.room";
/// N6: the rows that may make room (`?1` [`PARK_DISPLACE_MIN_ATTEMPTS`], `?2` the batch's parks): an EXHAUSTED row
/// (already given up), or a LIVE row that failed at least `?1` deliveries, never a row of this batch (its tokens are
/// the counter's last `?2`).
macro_rules! park_room_rows {
    () => {
        "FROM pot_announce_parked WHERE exhausted_at_ms IS NOT NULL OR (attempts >= ?1 AND generation <= (SELECT n FROM pot_announce_seq WHERE id = 1) - ?2)"
    };
}
/// N6: past the ceiling (`room` below 0) with fewer rows that may make room than the excess, this writes a counter row
/// that breaks the counter's `CHECK (id = 1)`: the statement faults and D1 rolls the WHOLE batch back (the stale
/// marks, every park, the counter): nothing of the body is written and the hook answers 503 with nothing announced.
/// The fault's words carry [`PARK_FULL_FAULT`]. Under the ceiling it reads the counter row alone. Over it, its LIMIT
/// is the excess (one body's 24) of ELIGIBLE rows found, not of rows visited (N10, the third delta review): the scan
/// is unindexed and stops at the excess-th eligible row, so it visits up to the whole table (at most 2,048 + the
/// batch's parks) when the eligible rows sit at the tail of the scan order or are too few (a refusal visits every
/// row). Measured: 12 rows with the eligible rows first, the whole table at the tail (`pot-changed-hook-full-room-tail`).
pub const PARK_GUARD_SQL: &str = concat!(
    "INSERT INTO pot_announce_seq (id, n, room) SELECT 2, 0, 0 WHERE (SELECT room FROM pot_announce_seq WHERE id = 1) < 0 AND (SELECT COUNT(*) FROM (SELECT 1 ",
    park_room_rows!(),
    " LIMIT max(0, -(SELECT room FROM pot_announce_seq WHERE id = 1)))) < -(SELECT room FROM pot_announce_seq WHERE id = 1)"
);
/// N6: what a fault of [`PARK_GUARD_SQL`] says (SQLite's and D1's words for the broken check).
pub const PARK_FULL_FAULT: &str = "CHECK constraint failed";
/// N6: the batch's last statement: the excess taken out, the oldest EXHAUSTED rows first, then the oldest LIVE rows
/// that may make room. So a batch displaces at most its own size. Each row taken is RETURNED, so the store names it.
/// Under the ceiling its limit is 0.
pub const PARK_MAKE_ROOM_SQL: &str = concat!(
    "DELETE FROM pot_announce_parked WHERE rowid IN (SELECT rowid ",
    park_room_rows!(),
    " ORDER BY exhausted_at_ms IS NULL, exhausted_at_ms, at LIMIT max(0, -(SELECT room FROM pot_announce_seq WHERE id = 1))) RETURNING outpoint, kind, recipient, attempts, exhausted_at_ms"
);
/// A delivered wake-up leaves the park (only the token that was delivered).
pub const PARKED_DELIVERED_SQL: &str = "DELETE FROM pot_announce_parked WHERE outpoint = ?1 AND kind = ?2 AND recipient = ?3 AND generation = ?4";
/// A wake-up that failed (the detached announce's, or the drain's): its next attempt, or its exhausted stamp, only at
/// the token it holds (a park since is a new intent with its own attempts).
pub const PARKED_RETRY_SQL: &str = "UPDATE pot_announce_parked SET attempts = ?5, next_attempt_at_ms = ?6, exhausted_at_ms = ?7 WHERE outpoint = ?1 AND kind = ?2 AND recipient = ?3 AND generation = ?4";
/// The drain's read: the live wake-ups due now, oldest due first, bounded.
pub const PARKED_DUE_SQL: &str = "SELECT outpoint, kind, recipient, at, attempts, generation FROM pot_announce_parked WHERE exhausted_at_ms IS NULL AND next_attempt_at_ms <= ?1 ORDER BY next_attempt_at_ms LIMIT ?2";
/// The sweep: exhausted wake-ups older than the bound, bounded per pass.
pub const PARKED_SWEEP_SQL: &str = "DELETE FROM pot_announce_parked WHERE rowid IN (SELECT rowid FROM pot_announce_parked WHERE exhausted_at_ms IS NOT NULL AND exhausted_at_ms < ?1 LIMIT ?2)";
/// N3: the AGE-OUT of live rows: a wake-up due for longer than [`PARK_OVERDUE_MAX_MS`] (the drain never reached it)
/// is stamped exhausted (`?2`), so the sweep takes it a day later; bounded per pass, by the due index.
pub const PARKED_AGE_SQL: &str = "UPDATE pot_announce_parked SET exhausted_at_ms = ?2 WHERE rowid IN (SELECT rowid FROM pot_announce_parked WHERE exhausted_at_ms IS NULL AND next_attempt_at_ms < ?1 LIMIT ?3)";

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
/// N3: the table's ceiling, as PR #32's `CHANGE_PARKED_MAX_ROWS`. N6: a batch past it makes room
/// ([`PARK_MAKE_ROOM_SQL`]) or is refused WHOLE ([`PARK_GUARD_SQL`], counted [`COUNTER_ANNOUNCE_PARK_FULL`], 503).
/// Read after the batch's parks, so a key already held never needs room.
pub const PARK_MAX_ROWS: u32 = 2048;
/// N6: a LIVE row may be displaced to make room only once it has failed this many deliveries (the detached announce
/// and two drain retries: at least 3 minutes of failing sends). The bound of the room a batch may make of live rows:
/// at most its own size (24), only of rows at or past this count. Past it the body is refused (503).
pub const PARK_DISPLACE_MIN_ATTEMPTS: u32 = 3;
/// N3: a live wake-up due for longer than this is aged out (exhausted, then swept).
pub const PARK_OVERDUE_MAX_MS: u64 = 86_400_000;
/// The drain's bound per ROUND: the wake-ups of one full body (8 rooms, 16 seats), delivered together.
pub const DRAIN_MAX_PER_PASS: u32 = 24;
/// N3: a pass runs further rounds while the last one was full (a backlog) and the wall has not fallen: at most
/// [`DRAIN_MAX_ROUNDS`] x [`DRAIN_MAX_PER_PASS`] = 96 wake-ups a scheduled pass, 384 an hour at `*/15`.
pub const DRAIN_MAX_ROUNDS: u32 = 4;
/// N4, N7: the drain TASK's one deadline (its boot, every round's due read, relay calls and settle, its counters and
/// the final courier flush; [`Deadline`]); it runs in its own scheduled task.
pub const DRAIN_WALL_MS: u64 = 60_000;
/// N7: the time a round's due read and relay calls leave before the deadline, so the settle of what they did (and
/// the age-out and sweep of the first round) is still issued while a relay hangs.
pub const DRAIN_SETTLE_RESERVE_MS: u64 = 5_000;
/// The sweep's bound per pass (and the age-out's).
pub const SWEEP_MAX_PER_PASS: u32 = 50;

/// bsv-low #668 (hop C): when the detached announce walls or the relay call fails, the wake-up is re-driven in
/// SECONDS within this same `wait_until` window, BEFORE it settles to the [`PARK_RETRY_BASE_MS`] fallback the
/// `*/15` drain picks up (the 150 s felt-latency this closes). At most this many extra delivery attempts.
pub const ANNOUNCE_REDRIVE_TRIES: u32 = 2;
/// The backoff before each re-drive attempt (a fresh subrequest typically hits a now-warm relay/hub DO).
pub const ANNOUNCE_REDRIVE_BACKOFF_MS: u64 = 1_000;
/// The per-attempt wall of a re-drive delivery (shorter than the first [`ANNOUNCE_WALL_MS`]).
pub const ANNOUNCE_REDRIVE_WALL_MS: u64 = 2_500;
/// The re-drive stops once this much of the task's own window has elapsed, so the settle write still fits inside
/// the platform's 30 s `wait_until` ([`PARK_GRACE_MS`] after the park) whatever the first wall cost.
pub const ANNOUNCE_REDRIVE_DEADLINE_MS: u64 = 25_000;

/// bsv-low #668 (step 0 / the observability bar): the pots-room push counters on `/health/invariants`. Distinct
/// from the pre-#668 `pot_changed_announce_*` counters; these measure the PUSH as a UX figure.
pub const COUNTER_PUSH_SENT: &str = "pots_push_sent_total";
/// Wake-ups whose FIRST delivery did not land (walled or a relay fault), so the seconds re-drive was tried.
pub const COUNTER_PUSH_WALLED: &str = "pots_push_walled_total";
/// Of the walled wake-ups, the ones the seconds re-drive recovered (delivered without the `*/15` drain).
pub const COUNTER_PUSH_REDRIVEN: &str = "pots_push_redriven_total";

/// bsv-low #668: the push-latency histogram's finite bucket bounds (ms), measured app-layer-side (the hook's
/// answer to the relay's 2xx). A sample lands in the first bucket whose bound it is at or under, else the overflow
/// bucket; the overlay's `/health/invariants.potsPush` derives p50/p95 from the bucket counters.
pub const PUSH_LATENCY_BUCKETS_MS: [u64; 7] = [250, 500, 1_000, 2_000, 5_000, 10_000, 20_000];

/// The `ops_counters` name for the bucket a `sample_ms` latency falls in (bsv-low #668).
pub fn push_latency_bucket_counter(sample_ms: u64) -> String {
    for b in PUSH_LATENCY_BUCKETS_MS {
        if sample_ms <= b {
            return format!("pots_push_latency_ms_le_{b}_total");
        }
    }
    "pots_push_latency_ms_le_inf_total".to_string()
}

/// bsv-low #668 (hop C): a seconds-cadence re-drive of the failed announce calls, within the detached task's own
/// `wait_until` window. `attempt_wall` and `backoff` are timer FACTORIES (the worker's `worker::Delay`, the pins'
/// ready/injected futures). `tries == 0` is the pre-#668 behaviour (no re-drive), the pins' default.
pub struct Redrive {
    pub tries: u32,
    pub attempt_wall: Box<dyn Fn() -> Detached>,
    pub backoff: Box<dyn Fn() -> Detached>,
}

impl Redrive {
    /// No re-drive: the pre-#668 settle-straight-to-park behaviour. Used by [`serve_pot_changed`] (the pins).
    pub fn none() -> Self {
        Redrive {
            tries: 0,
            attempt_wall: Box::new(|| Box::pin(async {})),
            backoff: Box::new(|| Box::pin(async {})),
        }
    }
}

/// A delivery that did not land and is worth a re-drive: walled (`None`) or a relay fault (`Failed`). A `Sent` or a
/// `NotConfigured` (no relay configured) is not re-driven.
fn is_retryable(d: &Option<Delivery>) -> bool {
    matches!(d, None | Some(Delivery::Failed))
}

/// When attempt `attempts` (1-based) may be retried, from `now`.
pub fn retry_at(now: u64, attempts: u32) -> u64 {
    let shift = attempts.saturating_sub(1).min(16);
    now + (PARK_RETRY_BASE_MS << shift).min(PARK_RETRY_MAX_MS)
}

/// N7 (the second delta review): ONE deadline for a whole task, an absolute end on a clock and a timer for the time
/// left. Every await of the task runs under [`Deadline::run`] (or [`Deadline::run_reserving`]): raced against the
/// time left, and NOT ISSUED once that time is spent. The worker's is [`Deadline::new`] over `Date::now` and
/// `worker::Delay`; the pins' over `Instant` and tokio's sleep.
pub struct Deadline {
    end_ms: u64,
    reserve_ms: u64,
    clock: Box<dyn Fn() -> u64>,
    timer: Box<dyn Fn(u64) -> Detached>,
}

impl Deadline {
    /// A deadline `budget_ms` from now; `reserve_ms` is what [`Deadline::run_reserving`] leaves before it.
    pub fn new(
        budget_ms: u64,
        reserve_ms: u64,
        clock: Box<dyn Fn() -> u64>,
        timer: Box<dyn Fn(u64) -> Detached>,
    ) -> Self {
        Deadline {
            end_ms: clock() + budget_ms,
            reserve_ms,
            clock,
            timer,
        }
    }
    /// The time left, in ms.
    pub fn left_ms(&self) -> u64 {
        self.end_ms.saturating_sub((self.clock)())
    }
    /// `f` raced against the deadline: `None` when the deadline fell first (`f` dropped) or had fallen before it.
    pub async fn run<F: Future>(&self, f: F) -> Option<F::Output> {
        self.run_leaving(0, f).await
    }
    /// `f` raced against the deadline less its reserve: what may not take the settle's time.
    pub async fn run_reserving<F: Future>(&self, f: F) -> Option<F::Output> {
        self.run_leaving(self.reserve_ms, f).await
    }
    async fn run_leaving<F: Future>(&self, reserve_ms: u64, f: F) -> Option<F::Output> {
        use futures_util::future::{select, Either};
        let left = self.left_ms().saturating_sub(reserve_ms);
        if left == 0 {
            return None;
        }
        let f = std::pin::pin!(f);
        match select(f, (self.timer)(left)).await {
            Either::Left((v, _)) => Some(v),
            Either::Right(_) => None,
        }
    }
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
    /// Park `wake` as a NEW intent (attempts 0, due at `next_ms`): its token is taken in the batch (N1).
    Park {
        wake: Wake,
        at_ms: u64,
        next_ms: u64,
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
    /// N3: stamp exhausted (at `now_ms`) the live rows due before `before_ms`.
    Age {
        before_ms: u64,
        now_ms: u64,
        limit: u32,
    },
}

impl ParkOp {
    /// The statement; a park's position `pos` in the batch (1-based) and the batch's clock `now` (N1).
    fn stmt(&self, pos: u64, now: u64) -> Stmt {
        let int = |n: u64| Bind::Int(n as i64);
        match self {
            ParkOp::Park {
                wake,
                at_ms,
                next_ms,
            } => {
                let mut b = wake.key_binds();
                b.extend([int(*at_ms), int(*next_ms), int(pos), int(now)]);
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
            ParkOp::Age {
                before_ms,
                now_ms,
                limit,
            } => Stmt::new(
                PARKED_AGE_SQL,
                vec![int(*before_ms), int(*now_ms), int(u64::from(*limit))],
            ),
        }
    }
}

/// What one park statement of a write came to: the rows it changed, and the token a park wrote (`None`: refused).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpOut {
    pub changes: u64,
    pub generation: Option<u64>,
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

/// The SQL under [`HookD1`]: ONE `run_batch` is one D1 round trip (the worker's `D1Sql` runs `counted_batch`), atomic
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
    /// Every outpoint's attribution, in the order of `outpoints`. [`HookD1`]'s round trips, conditional: the
    /// required decoded-rows batch (and, only when that batch faults, one more per outpoint); the seat markers'
    /// batch, only when a pot decoded; one batch per marker arm (three), only when an outpoint has no decoded params
    /// or no attributed seat. So 1 to 5 reads (5 + n on a required-read fault), then the caller's ONE write.
    async fn read_outpoints(&self, outpoints: &[(String, u32)]) -> Vec<OutpointRead>;
    /// Every stale mark and every park statement given, in ONE batch (the hook's write; the settle's; the
    /// drain's), the parks' tokens taken in it (N1). Answers each op's [`OpOut`], in order.
    async fn write(
        &self,
        marks: &[(String, &'static str)],
        ops: &[ParkOp],
    ) -> Result<Vec<OpOut>, String>;
    /// The parked wake-ups due at `now`, oldest due first, at most `limit`.
    async fn due_parked(&self, now: u64, limit: u32) -> Result<Vec<Parked>, String>;
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
    /// N6: the rows [`PARK_MAKE_ROOM_SQL`] took out of a batch that committed: exhausted ones counted, each LIVE one
    /// named LOST in the log and counted (its wake-up is not delivered).
    async fn room_made(&self, out: &StmtOut) {
        let (mut exhausted, mut displaced) = (0u64, 0u64);
        for r in &out.rows {
            if r.get("exhausted_at_ms").is_some_and(|v| !v.is_null()) {
                exhausted += 1;
                continue;
            }
            displaced += 1;
            let text = |k: &str| r.get(k).and_then(Value::as_str).unwrap_or_default();
            self.sql.log(&format!(
                "[pot-changed] LOST {} {} {} attempts={}: displaced at the park's ceiling ({PARK_MAX_ROWS} rows)",
                text("outpoint"),
                text("kind"),
                text("recipient"),
                r.get("attempts").and_then(Value::as_f64).unwrap_or(0.0) as u64
            ));
        }
        let deltas: Vec<(String, u64)> = [
            (COUNTER_ANNOUNCE_EVICTED_EXHAUSTED, exhausted),
            (COUNTER_ANNOUNCE_DISPLACED, displaced),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| (k.to_string(), n))
        .collect();
        if !deltas.is_empty() {
            self.sql.counters(deltas).await;
        }
    }

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
    ) -> Result<Vec<OpOut>, String> {
        let mut stmts: Vec<Stmt> = marks
            .iter()
            .map(|(id, _)| {
                Stmt::new(
                    crate::owed::OWED_STALE_FOR_IDENTITY_SQL,
                    vec![Bind::Text(id.clone())],
                )
            })
            .collect();
        // N1: the parks' tokens, the counter's last plus each park's position; the bump after them
        let parks = ops
            .iter()
            .filter(|o| matches!(o, ParkOp::Park { .. }))
            .count() as u64;
        let now = self.sql.now_ms();
        let mut pos = 0u64;
        for op in ops {
            if matches!(op, ParkOp::Park { .. }) {
                pos += 1;
            }
            stmts.push(op.stmt(pos, now));
        }
        // N6: the counter and the room, the guard that rolls the batch back, then the room made
        if parks > 0 {
            let room = vec![
                Bind::Int(i64::from(PARK_DISPLACE_MIN_ATTEMPTS)),
                Bind::Int(parks as i64),
            ];
            stmts.push(Stmt::new(
                PARK_SEQ_BUMP_SQL,
                vec![
                    Bind::Int(parks as i64),
                    Bind::Int(now as i64),
                    Bind::Int(i64::from(PARK_MAX_ROWS)),
                ],
            ));
            stmts.push(Stmt::new(PARK_GUARD_SQL, room.clone()));
            stmts.push(Stmt::new(PARK_MAKE_ROOM_SQL, room));
        }
        let out = self.sql.run_batch(stmts).await?;
        let at = marks.len();
        if parks > 0 {
            self.room_made(&out[at + ops.len() + 2]).await;
        }
        Ok(out[at..at + ops.len()]
            .iter()
            .zip(ops)
            .map(|(o, op)| OpOut {
                changes: o.changes,
                generation: match op {
                    ParkOp::Park { .. } => o
                        .rows
                        .first()
                        .and_then(|r| r.get("generation"))
                        .and_then(Value::as_f64)
                        .map(|g| g as u64),
                    _ => None,
                },
            })
            .collect())
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
    /// One `pot` event into `recipient`'s box, under the relay message id `message_id` ([`seat_message_id`]).
    async fn push(&self, recipient: &str, message_id: &str, body: Value) -> Delivery;
}

/// N2: the relay message id of one seat's `pot` event: the sha256 hex (64, the relay's stored-id shape) of the
/// outpoint, the recipient (lowercase) and the park's TOKEN (`g<generation>`; since N6 every send has one: a
/// call with none, kept for the type, takes `a<at_ms>`, the event's own stamp). One intent's every send (the detached announce, each
/// drain retry) carries one id, and the relay (`rust-message-box` 0.4.6, `split_push_body`: a flat `/push` body's
/// `messageId` is kept; `messages.message_id` is UNIQUE and stored `INSERT OR IGNORE`) files it once and answers a
/// repeat `400 ERR_DUPLICATE_MESSAGE`, which [`crate::internal_events::push_answer`] reads as delivered.
pub fn seat_message_id(txid: &str, vout: u32, recipient: &str, token: &str) -> String {
    let key = format!(
        "low_events/pot|{}:{vout}|{}|{token}",
        txid.to_ascii_lowercase(),
        recipient.to_ascii_lowercase()
    );
    hex::encode(bsv_rs::primitives::hash::sha256(key.as_bytes()))
}

/// N8 (the second delta review): the INTENT id of one pots-room broadcast, the sha256 hex of the outpoint and the
/// park's TOKEN (as [`seat_message_id`]): every send of one intent (the detached announce, each drain retry, a
/// retry after a lost answer or a failed delete) carries one id, and a later park of the outpoint is a new intent with
/// a new id. The relay cannot honour it: `rust-message-box` 0.4.6's `/broadcast` reads `{box_name, body}` only and
/// mints `bcast-<ms>` per fan-out (`fan_out_broadcast`), so a repeat is filed again. It rides the BODY
/// (`intentId`), for the client's own dedupe; the relay's `/broadcast` taking a caller's id is the ask filed in the
/// PR.
pub fn room_intent_id(txid: &str, vout: u32, token: &str) -> String {
    let key = format!(
        "{}|{}:{vout}|{token}",
        crate::internal_events::POTS_ROOM,
        txid.to_ascii_lowercase()
    );
    hex::encode(bsv_rs::primitives::hash::sha256(key.as_bytes()))
}

/// What the detached work of one body is: the outpoints to announce in the pots room, and the seats to tell.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnnouncePlan {
    pub outpoints: Vec<(String, u32)>,
    /// (identity, txid, vout): one `pot` event into the seat's `low_events` box.
    pub seat_events: Vec<(String, String, u32)>,
    /// bsv-low #668 (step 0): the hook's answer time, carried onto the push as `hookedAt`.
    pub at_ms: u64,
    /// bsv-low #668 (step 0, hop A): the overlay's SHIP time, if the body carried one; carried onto the push as
    /// `admittedAt`. `None` when the producer did not stamp it (the park-drain fallback, an old overlay).
    pub admitted_at_ms: Option<u64>,
    /// Per wake-up of [`AnnouncePlan::wakes`], the token its park WROTE (N1: read back by `RETURNING`); `None` is
    /// never planned since N6 (a refused body announces nothing; kept for the type).
    pub generations: Vec<Option<u64>>,
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
    /// Every wake-up with its token, stamped with the relay-call time (`broadcast_at_ms`, bsv-low #668 step 0).
    pub fn sends(&self, broadcast_at_ms: u64) -> Vec<Call> {
        self.wakes()
            .into_iter()
            .enumerate()
            .map(|(i, wake)| Call {
                wake,
                at_ms: self.at_ms,
                admitted_at_ms: self.admitted_at_ms,
                broadcast_at_ms,
                generation: self.generations.get(i).copied().flatten(),
            })
            .collect()
    }
}

/// One relay call to make: a wake-up, the `at` its body carries, and its park's token (`None`: not parked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    pub wake: Wake,
    /// The hook's answer time (`hookedAt` on the push body, bsv-low #668 step 0).
    pub at_ms: u64,
    /// The overlay's ship time (`admittedAt` on the push body), if the body carried one.
    pub admitted_at_ms: Option<u64>,
    /// The relay-call time (`broadcastAt` on the push body).
    pub broadcast_at_ms: u64,
    pub generation: Option<u64>,
}

/// What [`apply_pot_changed`] decided: the answer's lists, the identities whose owed rows the detached recompute
/// re-derives, and the detached announce.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PotChangedOutcome {
    pub filed: Vec<Value>,
    pub skipped: Vec<Value>,
    pub owed_identities: Vec<String>,
    pub stale_marks: Vec<(String, &'static str)>,
    /// The hook's write batch (the stale marks and the parks) faulted: nothing of it landed, NOTHING is announced
    /// (N2), and the answer says so (503, retryable).
    pub stale_fault: Option<String>,
    /// Wake-ups parked durably before the answer: the rows the park WROTE (N1, `RETURNING`).
    pub parked: usize,
    /// N6: wake-ups of a body the park refused WHOLE at its ceiling (no room to make): counted, and the body is
    /// answered 503 (`stale_fault` is set) with nothing announced.
    pub park_full: usize,
    pub plan: AnnouncePlan,
}

fn push_unique(v: &mut Vec<String>, id: &str) {
    if !v.iter().any(|x| x == id) {
        v.push(id.to_string());
    }
}

/// The pot hook's whole decision for one parsed body: the store's reads, then ONE write batch (the stale marks and
/// every wake-up parked), and the plan of what runs after the answer. No relay call, no read-model walk, no owed
/// recompute.
///
/// Per outpoint, the branches of the handler before L4: a read fault is skipped; with no decoded params, or with
/// decoded params and no attributed seat, the marker-named identities are marked stale and queued for the
/// recompute (fleet loop 11 and 15); an attributed pot queues a `pot` event for each seat and its recompute.
pub async fn apply_pot_changed<S: HookStore>(
    store: &S,
    outpoints: &[(String, u32)],
    at_ms: u64,
    admitted_at_ms: Option<u64>,
) -> PotChangedOutcome {
    let mut out = PotChangedOutcome {
        plan: AnnouncePlan {
            outpoints: outpoints.to_vec(),
            seat_events: Vec::new(),
            at_ms,
            admitted_at_ms,
            generations: Vec::new(),
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
    // due until the detached announce has had its window; the announce's settle takes out what it delivered. N1:
    // each park's token is the one the batch WROTE; N6: a batch the ceiling refuses is rolled back whole.
    let parks: Vec<ParkOp> = out
        .plan
        .wakes()
        .into_iter()
        .map(|wake| ParkOp::Park {
            wake,
            at_ms,
            next_ms: at_ms + PARK_GRACE_MS,
        })
        .collect();
    match store.write(&out.stale_marks, &parks).await {
        Ok(outs) => {
            out.plan.generations = outs.iter().map(|o| o.generation).collect();
            out.parked = outs.iter().filter(|o| o.generation.is_some()).count();
        }
        Err(e) if !parks.is_empty() && e.contains(PARK_FULL_FAULT) => {
            out.park_full = parks.len();
            store.log(&format!(
                "[pot-changed] the park is FULL ({PARK_MAX_ROWS} rows, no room to make): the body's {} wake-up(s) refused, answered 503, nothing announced",
                out.park_full
            ));
            store
                .counters(vec![(
                    COUNTER_ANNOUNCE_PARK_FULL.to_string(),
                    out.park_full as u64,
                )])
                .await;
            out.stale_fault = Some(format!("the park is full: {e}"));
        }
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
    /// Wake-ups left parked for the drain (every failed one that was parked).
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

/// One relay call: a room's broadcast carrying its [`room_intent_id`] (N8), or a seat's `pot` event under its
/// [`seat_message_id`].
async fn send_one<R: RelayPort>(relay: &R, s: &Call) -> Delivery {
    let w = &s.wake;
    let token = match s.generation {
        Some(g) => format!("g{g}"),
        None => format!("a{}", s.at_ms),
    };
    match &w.recipient {
        None => {
            let mut body = crate::internal_events::pot_changed_event_body(&w.txid, w.vout, s.at_ms);
            body["intentId"] = json!(room_intent_id(&w.txid, w.vout, &token));
            stamp_hops(&mut body, s);
            relay
                .broadcast(crate::internal_events::POTS_ROOM, body)
                .await
        }
        Some(id) => {
            let mut body =
                crate::internal_events::pot_event_body(&w.txid, w.vout, carrier_entry(), s.at_ms);
            stamp_hops(&mut body, s);
            relay
                .push(id, &seat_message_id(&w.txid, w.vout, id, &token), body)
                .await
        }
    }
}

/// bsv-low #668 (step 0): stamp the push body with the latency hops — `admittedAt` (the overlay's ship time, when
/// the body carried one), `hookedAt` (the hook's answer time) and `broadcastAt` (the relay-call time). Additive:
/// the client reads `potOutpoint`/`kind`/`at` and ignores these (`app/src/lib/lowEvents.ts`, `useServedWord.ts`).
fn stamp_hops(body: &mut Value, s: &Call) {
    if let Some(a) = s.admitted_at_ms {
        body["admittedAt"] = json!(a);
    }
    body["hookedAt"] = json!(s.at_ms);
    body["broadcastAt"] = json!(s.broadcast_at_ms);
}

/// Deliver `sends` together (the calls of one body run together, not one after another), all raced against ONE
/// `wall`: each call's answer, `None` for a call that had not answered at the wall (dropped).
pub async fn deliver<R: RelayPort, W: Future<Output = ()>>(
    relay: &R,
    sends: &[Call],
    wall: W,
) -> Vec<Option<Delivery>> {
    use futures_util::future::{join_all, select};
    let got: RefCell<Vec<Option<Delivery>>> = RefCell::new(vec![None; sends.len()]);
    let calls = sends.iter().enumerate().map(|(i, s)| {
        let got = &got;
        async move {
            let d = send_one(relay, s).await;
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
    let got = deliver(relay, &plan.sends(plan.at_ms), wall).await;
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
    async fn push(&self, recipient: &str, message_id: &str, body: Value) -> Delivery {
        crate::internal_events::first_party_push_with_id(
            &self.env,
            recipient,
            Some(message_id),
            body,
        )
        .await
    }
}

/// One unit of work handed past the answer (the worker's is `ctx.wait_until`).
pub type Detached = std::pin::Pin<Box<dyn Future<Output = ()>>>;

/// The detached announce of one body, then its SETTLE (H1): a delivered wake-up (or one the deploy cannot send)
/// leaves the park at the token its park wrote; one that failed or had not answered at the wall is retried by the
/// drain (attempt 1, due after [`PARK_RETRY_BASE_MS`]) at that token only (N1: a later hook's park of the key is a new
/// intent, untouched). A call with no token (none since N6) settles nothing. One write batch; its log line
/// and its counters.
pub async fn announce_and_report<S: HookStore, R: RelayPort, W: Future<Output = ()>>(
    store: &S,
    relay: &R,
    plan: &AnnouncePlan,
    wall: W,
    redrive: &Redrive,
) -> AnnounceTally {
    let t0 = store.now_ms();
    // bsv-low #668 (step 0): the relay-call time stamps `broadcastAt` on every push body.
    let sends = plan.sends(t0);
    let mut got = deliver(relay, &sends, wall).await;
    // bsv-low #668 (hop C): the wake-ups whose FIRST delivery did not land are the re-drive candidates (walled or
    // a relay fault), and only those that were PARKED (a generation) — a `NotConfigured` skip is not a failure.
    let walled_first: Vec<usize> = (0..sends.len())
        .filter(|&i| sends[i].generation.is_some() && is_retryable(&got[i]))
        .collect();
    let mut redriven = 0u64;
    if redrive.tries > 0 && !walled_first.is_empty() {
        let mut pending = walled_first.clone();
        for _ in 0..redrive.tries {
            if pending.is_empty()
                || store.now_ms().saturating_sub(t0) >= ANNOUNCE_REDRIVE_DEADLINE_MS
            {
                break;
            }
            (redrive.backoff)().await;
            let retry_sends: Vec<Call> = pending.iter().map(|&i| sends[i].clone()).collect();
            let retry_got = deliver(relay, &retry_sends, (redrive.attempt_wall)()).await;
            let mut still = Vec::new();
            for (k, &i) in pending.iter().enumerate() {
                match retry_got[k] {
                    Some(d @ (Delivery::Sent | Delivery::NotConfigured)) => {
                        got[i] = Some(d);
                        if d == Delivery::Sent {
                            redriven += 1;
                        }
                    }
                    other => {
                        got[i] = other;
                        still.push(i);
                    }
                }
            }
            pending = still;
        }
    }
    let now = store.now_ms();
    let mut t = AnnounceTally::default();
    let mut ops = Vec::with_capacity(sends.len());
    for (s, d) in sends.iter().zip(&got) {
        match d {
            Some(d @ (Delivery::Sent | Delivery::NotConfigured)) => {
                t.note(*d);
                if let Some(generation) = s.generation {
                    ops.push(ParkOp::Delivered {
                        wake: s.wake.clone(),
                        generation,
                    });
                }
            }
            failed => {
                t.failed += 1;
                t.walled |= failed.is_none();
                if let Some(generation) = s.generation {
                    t.parked += 1;
                    ops.push(ParkOp::Retry {
                        wake: s.wake.clone(),
                        generation,
                        attempts: 1,
                        next_ms: retry_at(now, 1),
                        exhausted_at_ms: None,
                    });
                }
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
        "[pot-changed] announce: {} outpoint(s), {} seat event(s): sent={} skipped={} failed={} parked={}{} redriven={} in {} ms",
        plan.outpoints.len(),
        plan.seat_events.len(),
        t.sent,
        t.skipped,
        t.failed,
        t.parked,
        if t.walled { " (WALL)" } else { "" },
        redriven,
        now.saturating_sub(t0)
    ));
    let mut counters = t.counters();
    // bsv-low #668 (step 0 / the observability bar): the pots-room PUSH counters + the latency histogram, measured
    // app-layer-side (the hook's answer `plan.at_ms` to the relay's 2xx `now`). A body that delivered at least one
    // wake-up records one latency sample; the overlay's `/health/invariants.potsPush` derives p50/p95.
    if t.sent > 0 {
        counters.push((COUNTER_PUSH_SENT.to_string(), t.sent));
        let sample_ms = now.saturating_sub(plan.at_ms);
        counters.push((push_latency_bucket_counter(sample_ms), 1));
    }
    if !walled_first.is_empty() {
        counters.push((COUNTER_PUSH_WALLED.to_string(), walled_first.len() as u64));
    }
    if redriven > 0 {
        counters.push((COUNTER_PUSH_REDRIVEN.to_string(), redriven));
    }
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
    /// N3: the rounds the pass ran, and the live rows it aged out.
    pub rounds: u64,
    pub aged: u64,
}

impl DrainTally {
    pub fn counters(&self) -> Vec<(String, u64)> {
        [
            (COUNTER_ANNOUNCE_DRAINED, self.drained),
            (COUNTER_ANNOUNCE_SKIPPED, self.skipped),
            (COUNTER_ANNOUNCE_RETRY_FAILED, self.retry_failed),
            (COUNTER_ANNOUNCE_EXHAUSTED, self.exhausted),
            (COUNTER_ANNOUNCE_SWEPT, self.swept),
            (COUNTER_ANNOUNCE_AGED, self.aged),
            (COUNTER_ANNOUNCE_PARK_FAULTS, self.faults),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| (k.to_string(), n))
        .collect()
    }
}

/// ONE DRAIN PASS (the scheduled tick's own task, and `POST /internal/pot-announce-drain`'s): ROUNDS of at most
/// [`DRAIN_MAX_PER_PASS`] parked wake-ups due now, each round's delivered together, a further round only while the
/// last was full (a backlog), delivered at least one and the deadline has not fallen, at most [`DRAIN_MAX_ROUNDS`]
/// (N3). A delivered one leaves the park, a failed one is retried later (doubling from [`PARK_RETRY_BASE_MS`] to
/// [`PARK_RETRY_MAX_MS`]) and marked exhausted at its [`PARK_MAX_ATTEMPTS`]th attempt; the first round's write also
/// ages out live rows due for longer than [`PARK_OVERDUE_MAX_MS`] and sweeps exhausted rows older than
/// [`PARK_SWEEP_AFTER_MS`], each at most [`SWEEP_MAX_PER_PASS`]. Two D1 round trips a round: the due read, then one
/// write batch. N7: EVERY await (the due read, the relay calls, the settle, the counters) runs under the task's one
/// `deadline`; the due read and the relay calls leave its reserve ([`DRAIN_SETTLE_RESERVE_MS`]) for the settle. A
/// relay call that has not answered when they stop is settled as a failed attempt; a round whose settle is not
/// issued, or is dropped, leaves its rows as they were (still parked at their tokens, retried by a later pass).
pub async fn drain_parked<S: HookStore, R: RelayPort>(
    store: &S,
    relay: &R,
    deadline: &Deadline,
) -> DrainTally {
    let mut t = DrainTally::default();
    let mut walled = false;
    for round in 0..DRAIN_MAX_ROUNDS {
        let now = store.now_ms();
        let due = match deadline
            .run_reserving(store.due_parked(now, DRAIN_MAX_PER_PASS))
            .await
        {
            Some(Ok(d)) => d,
            Some(Err(e)) => {
                t.faults += 1;
                store.log(&format!("[pot-changed] drain read failed: {e}"));
                break;
            }
            None => {
                walled = true;
                break;
            }
        };
        t.rounds += 1;
        let full = due.len() as u32 == DRAIN_MAX_PER_PASS;
        t.due += due.len() as u64;
        let mut ops = Vec::with_capacity(due.len() + 2);
        let got = if due.is_empty() {
            Vec::new()
        } else {
            let sends: Vec<Call> = due
                .iter()
                .map(|p| Call {
                    wake: p.wake.clone(),
                    at_ms: p.at_ms,
                    // The park does not keep the overlay's ship time; the drain stamps its own send time as
                    // broadcastAt and omits admittedAt (the fallback path, not the warm latency measurement).
                    admitted_at_ms: None,
                    broadcast_at_ms: now,
                    generation: Some(p.generation),
                })
                .collect();
            let cell: RefCell<Vec<Option<Delivery>>> = RefCell::new(vec![None; sends.len()]);
            let calls = sends.iter().enumerate().map(|(i, s)| {
                let cell = &cell;
                async move {
                    let d = send_one(relay, s).await;
                    cell.borrow_mut()[i] = Some(d);
                }
            });
            if deadline
                .run_reserving(futures_util::future::join_all(calls))
                .await
                .is_none()
            {
                walled = true;
            }
            cell.into_inner()
        };
        let now = store.now_ms();
        let settled_before = t.drained + t.skipped;
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
        if round == 0 {
            ops.push(ParkOp::Age {
                before_ms: now.saturating_sub(PARK_OVERDUE_MAX_MS),
                now_ms: now,
                limit: SWEEP_MAX_PER_PASS,
            });
            ops.push(ParkOp::Sweep {
                before_ms: now.saturating_sub(PARK_SWEEP_AFTER_MS),
                limit: SWEEP_MAX_PER_PASS,
            });
        }
        if !ops.is_empty() {
            match deadline.run(store.write(&[], &ops)).await {
                Some(Ok(outs)) => {
                    if round == 0 {
                        let n = outs.len();
                        t.aged = outs[n - 2].changes;
                        t.swept = outs[n - 1].changes;
                    }
                }
                Some(Err(e)) => {
                    t.faults += 1;
                    store.log(&format!("[pot-changed] drain settle failed: {e}"));
                    break;
                }
                None => {
                    walled = true;
                    store.log(&format!(
                        "[pot-changed] drain settle not landed at the deadline: {} op(s) left as they were",
                        ops.len()
                    ));
                    break;
                }
            }
        }
        // a further round only for a backlog the relay is taking: a round that delivered nothing ends the pass
        if !full || walled || t.drained + t.skipped == settled_before {
            break;
        }
    }
    store.log(&format!(
        "[pot-changed] drain: rounds={} due={} drained={} skipped={} retry_failed={} exhausted={} aged={} swept={} faults={}{}",
        t.rounds, t.due, t.drained, t.skipped, t.retry_failed, t.exhausted, t.aged, t.swept, t.faults,
        if walled { " (WALL)" } else { "" }
    ));
    let counters = t.counters();
    if !counters.is_empty() && deadline.run(store.counters(counters)).await.is_none() {
        store.log("[pot-changed] drain counters not flushed at the deadline");
    }
    t
}

/// THE POT HOOK, after its bearer and its parse: the decision over the store ([`apply_pot_changed`]), then, ONLY when
/// its write batch committed, the announce handed to `detach` (never awaited here), then the outcome the answer is
/// built from. N2: a batch that faulted announces NOTHING (the answer is 503 and the producer's retry announces once).
/// The worker's `routes::internal_pot_changed` calls it with `HookD1` over D1, the `RELAY` binding and
/// `ctx.wait_until`; the pins call it with the same store over SQLite and a recording `detach`.
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
    serve_pot_changed_at(
        store,
        relay,
        wall,
        detach,
        outpoints,
        at_ms,
        None,
        Redrive::none(),
    )
    .await
}

/// bsv-low #668 (step 0 + hop C): as [`serve_pot_changed`], with the overlay's ship time (`admittedAt`) carried
/// onto the push (the felt's first-wake latency is decomposable) and a seconds-cadence `redrive` of a walled
/// announce. The worker's handler passes both; the pins use the [`serve_pot_changed`] wrapper (`None` /
/// [`Redrive::none`]).
#[allow(clippy::too_many_arguments)] // the seam carries the hook's inputs, the ship-time stamp and the re-drive
pub async fn serve_pot_changed_at<S, R, W>(
    store: std::rc::Rc<S>,
    relay: std::rc::Rc<R>,
    wall: W,
    detach: &dyn Fn(Detached),
    outpoints: &[(String, u32)],
    at_ms: u64,
    admitted_at_ms: Option<u64>,
    redrive: Redrive,
) -> PotChangedOutcome
where
    S: HookStore + 'static,
    R: RelayPort + 'static,
    W: Future<Output = ()> + 'static,
{
    let outcome = apply_pot_changed(&*store, outpoints, at_ms, admitted_at_ms).await;
    if outcome.stale_fault.is_none() {
        let plan = outcome.plan.clone();
        detach(Box::pin(async move {
            announce_and_report(&*store, &*relay, &plan, wall, &redrive).await;
        }));
    }
    outcome
}

/// N4: the scheduled tick's units of work, each its OWN `wait_until` task, the drain handed FIRST: the tracker's
/// boot, board era, backfill and tick await D1 and CHAINTRACKS, so a pending answer there no longer holds the drain
/// behind it. bsv-low #778: the chain-event intake is the second task, out of reach of the tracker's awaits too. The
/// worker's `scheduled` calls this with `ctx.wait_until`.
pub fn schedule_tick(
    spawn: &dyn Fn(Detached),
    drain: Detached,
    intake: Detached,
    tracker: Detached,
) {
    spawn(drain);
    spawn(intake);
    spawn(tracker);
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
    pub(crate) const MARKER_POT: &str =
        "f0000000000000000000000000000000000000000000000000000000000000ff";
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
        /// Every call: (to, the seat's message id, the body).
        pub bodies: RefCell<Vec<(String, Option<String>, Value)>>,
        /// Recipients whose next push fails (once each).
        pub fail_once: RefCell<Vec<String>>,
    }
    impl Relay {
        async fn call(&self, to: String, message_id: Option<&str>, body: Value) -> Delivery {
            self.started.set(self.started.get() + 1);
            self.bodies.borrow_mut().push((
                to.clone(),
                message_id.map(str::to_string),
                body.clone(),
            ));
            let failing = {
                let mut f = self.fail_once.borrow_mut();
                let hit = f.iter().position(|r| *r == to);
                hit.map(|i| f.remove(i)).is_some()
            };
            if failing {
                return Delivery::Failed;
            }
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
            self.call(room.to_string(), None, body).await
        }
        async fn push(&self, recipient: &str, message_id: &str, body: Value) -> Delivery {
            self.call(recipient.to_string(), Some(message_id), body)
                .await
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
                "crate::pot_hook::serve_pot_changed_at(store, relay, wall, &detach, &outpoints, t0, admitted_at_ms, redrive,)"
            )),
            "the worker's handler is serve_pot_changed_at (bsv-low #668: carries admittedAt + the re-drive)"
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

    /// bsv-low #668 (step 0): the pots-room push and a seat event carry the latency hops — `admittedAt` (the
    /// overlay's ship time, when present), `hookedAt` (the hook's answer `at_ms`), `broadcastAt` (the relay-call
    /// time `t0`). RED on a body without them. Additive: the client reads `potOutpoint`/`kind`/`at` and ignores
    /// these (`app/src/lib/lowEvents.ts`, `useServedWord.ts`).
    #[tokio::test(flavor = "current_thread")]
    async fn push_body_carries_the_hop_stamps() {
        let store = Rc::new(HookD1 {
            sql: SqliteSql::migrated(),
        });
        store.sql.now.set(333);
        let relay = Rc::new(Relay::default());
        let plan = AnnouncePlan {
            outpoints: vec![op(1)],
            seat_events: vec![(A.into(), op(1).0, op(1).1)],
            at_ms: 222,
            admitted_at_ms: Some(111),
            generations: vec![Some(1), Some(2)],
        };
        announce_and_report(
            &*store,
            &*relay,
            &plan,
            tokio::time::sleep(Duration::from_millis(200)),
            &Redrive::none(),
        )
        .await;
        // Scoped so the borrow is dropped before the next await (clippy `await_holding_refcell_ref`).
        {
            let bodies = relay.bodies.borrow();
            assert_eq!(bodies.len(), 2, "one room broadcast and one seat event");
            for (_to, _mid, body) in bodies.iter() {
                assert_eq!(
                    body["admittedAt"].as_u64(),
                    Some(111),
                    "hop A: the overlay ship time"
                );
                assert_eq!(
                    body["hookedAt"].as_u64(),
                    Some(222),
                    "hop C: the hook's answer"
                );
                assert_eq!(body["broadcastAt"].as_u64(), Some(333), "the relay call");
            }
        }
        // Absent an overlay stamp, `admittedAt` is OMITTED; the other two stay.
        let relay2 = Rc::new(Relay::default());
        let plan2 = AnnouncePlan {
            admitted_at_ms: None,
            ..plan.clone()
        };
        announce_and_report(
            &*store,
            &*relay2,
            &plan2,
            tokio::time::sleep(Duration::from_millis(200)),
            &Redrive::none(),
        )
        .await;
        let b2 = relay2.bodies.borrow();
        assert!(
            b2[0].2.get("admittedAt").is_none(),
            "no overlay stamp, no admittedAt"
        );
        assert_eq!(b2[0].2["hookedAt"].as_u64(), Some(222));
        assert_eq!(b2[0].2["broadcastAt"].as_u64(), Some(333));
    }

    /// bsv-low #668 (hop C): a walled/failed announce is RE-DRIVEN in seconds within the same `wait_until` task,
    /// recovering the wake-up instead of parking it for the `*/15` drain. RED without the re-drive
    /// ([`Redrive::none`] parks it at +60 s); GREEN with two tries over a relay that fails once then lands.
    #[tokio::test(flavor = "current_thread")]
    async fn a_walled_announce_is_redriven_not_parked() {
        let plan = AnnouncePlan {
            outpoints: vec![op(1)],
            seat_events: Vec::new(),
            at_ms: 100,
            admitted_at_ms: Some(50),
            generations: vec![Some(7)],
        };
        let fail_pots = || {
            Rc::new(Relay {
                fail_once: RefCell::new(vec![crate::internal_events::POTS_ROOM.to_string()]),
                ..Relay::default()
            })
        };
        // WITHOUT the re-drive: the first delivery fails and the wake-up parks (the pre-#668 fallback).
        let store0 = Rc::new(HookD1 {
            sql: SqliteSql::migrated(),
        });
        let relay0 = fail_pots();
        let t0 = announce_and_report(
            &*store0,
            &*relay0,
            &plan,
            tokio::time::sleep(Duration::from_millis(200)),
            &Redrive::none(),
        )
        .await;
        assert_eq!(
            (t0.sent, t0.parked, t0.failed),
            (0, 1, 1),
            "no re-drive: the failed wake-up parks for the */15 drain"
        );
        // WITH the re-drive: the second attempt lands and the wake-up is delivered, not parked.
        let store = Rc::new(HookD1 {
            sql: SqliteSql::migrated(),
        });
        let relay = fail_pots();
        let redrive = Redrive {
            tries: 2,
            attempt_wall: Box::new(|| {
                Box::pin(tokio::time::sleep(Duration::from_millis(200))) as Detached
            }),
            backoff: Box::new(|| {
                Box::pin(tokio::time::sleep(Duration::from_millis(30))) as Detached
            }),
        };
        let t = announce_and_report(
            &*store,
            &*relay,
            &plan,
            tokio::time::sleep(Duration::from_millis(200)),
            &redrive,
        )
        .await;
        assert_eq!(
            (t.sent, t.parked, t.failed),
            (1, 0, 0),
            "the seconds re-drive recovered the wake-up before the park"
        );
        assert_eq!(
            relay.started.get(),
            2,
            "one failed call, then one re-drive call"
        );
        assert_eq!(
            store.sql.counter(COUNTER_PUSH_REDRIVEN),
            1,
            "the recovery is counted"
        );
        assert_eq!(
            store.sql.counter(COUNTER_PUSH_WALLED),
            1,
            "the first-try miss is counted"
        );
        assert_eq!(store.sql.counter(COUNTER_PUSH_SENT), 1);
    }

    /// bsv-low #668 (the observability bar): a delivered push records one latency sample into the histogram,
    /// measured app-layer-side (the hook's answer `at_ms` to the relay's 2xx `now`); the bucket counter moves and
    /// the bucket function's boundaries hold. The overlay's `/health/invariants.potsPush` derives p50/p95 from it.
    #[tokio::test(flavor = "current_thread")]
    async fn the_push_latency_histogram_moves() {
        let store = Rc::new(HookD1 {
            sql: SqliteSql::migrated(),
        });
        store.sql.now.set(1_400); // broadcastAt/now; hookedAt 1000 → a 400 ms app-layer push latency
        let relay = Rc::new(Relay::default());
        let plan = AnnouncePlan {
            outpoints: vec![op(1)],
            seat_events: Vec::new(),
            at_ms: 1_000,
            admitted_at_ms: Some(900),
            generations: vec![Some(1)],
        };
        announce_and_report(
            &*store,
            &*relay,
            &plan,
            tokio::time::sleep(Duration::from_millis(200)),
            &Redrive::none(),
        )
        .await;
        assert_eq!(
            store.sql.counter("pots_push_latency_ms_le_500_total"),
            1,
            "a 400 ms push lands in the le_500 bucket"
        );
        assert_eq!(store.sql.counter(COUNTER_PUSH_SENT), 1);
        assert_eq!(
            push_latency_bucket_counter(0),
            "pots_push_latency_ms_le_250_total"
        );
        assert_eq!(
            push_latency_bucket_counter(250),
            "pots_push_latency_ms_le_250_total"
        );
        assert_eq!(
            push_latency_bucket_counter(251),
            "pots_push_latency_ms_le_500_total"
        );
        assert_eq!(
            push_latency_bucket_counter(999_999),
            "pots_push_latency_ms_le_inf_total"
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
        let t = drain_parked(&*store, &quick, &test_deadline(5_000)).await;
        assert_eq!((t.due, t.drained), (0, 0), "not due before its retry");
        store.sql.now.set(store.sql.now.get() + PARK_RETRY_BASE_MS);
        let t = drain_parked(&*store, &quick, &test_deadline(5_000)).await;
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
                    next_ms: now,
                }],
            )
            .await
            .unwrap();
        let failing = SlowRelay::new(Duration::ZERO, Delivery::Failed);
        for _ in 0..PARK_MAX_ATTEMPTS {
            let t = drain_parked(&*store, &failing, &test_deadline(5_000)).await;
            assert_eq!(t.due, 1);
            store.sql.now.set(store.sql.now.get() + PARK_RETRY_MAX_MS);
        }
        let rows = parked_rows(&store.sql);
        assert_eq!((rows.len(), rows[0].3), (1, i64::from(PARK_MAX_ATTEMPTS)));
        assert!(rows[0].4.is_some(), "exhausted at its 8th attempt");
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_EXHAUSTED), 1);
        assert_eq!(
            store.sql.counter(COUNTER_ANNOUNCE_RETRY_FAILED),
            u64::from(PARK_MAX_ATTEMPTS) - 1
        );
        let t = drain_parked(&*store, &failing, &test_deadline(5_000)).await;
        assert_eq!(
            (t.due, t.swept),
            (0, 0),
            "an exhausted wake-up is not retried, nor swept early"
        );
        store.sql.now.set(store.sql.now.get() + PARK_SWEEP_AFTER_MS);
        let t = drain_parked(&*store, &failing, &test_deadline(5_000)).await;
        assert_eq!(t.swept, 1, "swept a day after");
        assert!(parked_rows(&store.sql).is_empty());
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_SWEPT), 1);
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
        async fn push(&self, _recipient: &str, _message_id: &str, _body: Value) -> Delivery {
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
            admitted_at_ms: None,
            generations: Vec::new(),
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

    /// (L1) THE COMMENTS SAY WHAT THE HOOK DOES: the handler's doc no longer promises an assembled `/results` entry
    /// filed before the answer, it names the empty carrier, the volatile scheduling and the durable park; the store's
    /// doc states its conditional round trips, not "one round trip per method".
    #[test]
    fn e649f_l1_the_hooks_comments_say_the_carrier_the_park_and_the_round_trips() {
        let routes = include_str!("routes.rs");
        let at = routes
            .find("pub(crate) async fn internal_pot_changed(")
            .expect("the handler");
        let doc = &routes[routes[..at].rfind("/// W2-P4").expect("the handler's doc")..at];
        for gone in ["assemble each", "served `/results` entry (the same gather"] {
            assert!(
                !doc.contains(gone),
                "the handler's doc still promises an assembled entry: {gone}"
            );
        }
        for said in [
            "EMPTY `entry`",
            "SCHEDULED, not made",
            "PARKED durably",
            "not delivered",
        ] {
            assert!(doc.contains(said), "the handler's doc says {said}");
        }
        let hook = include_str!("pot_hook.rs");
        assert!(
            !hook.contains(&["Each method is ONE round", " trip"].concat()),
            "a stale round-trip claim"
        );
        assert!(hook.contains(
            "So 1 to 5 reads (5 + n on a required-read fault), then the caller's ONE write."
        ));
        assert!(hook.contains("The publish is VOLATILE scheduling, the filing DURABLE"));
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

    fn parked_count(db: &SqliteSql) -> i64 {
        db.conn
            .query_row("SELECT COUNT(*) FROM pot_announce_parked", [], |r| r.get(0))
            .unwrap()
    }
    fn generations(db: &SqliteSql) -> Vec<i64> {
        let mut q = db
            .conn
            .prepare("SELECT generation FROM pot_announce_parked ORDER BY generation")
            .unwrap();
        q.query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// N decoded pots, two verified (latched) seats each, every seat holding an owed-state row: 8 of them are the
    /// body of 8 rooms and 16 seats, the review's 24 calls.
    fn decoded_fixture(n: u32) -> (SqliteSql, Vec<(String, u32)>) {
        let db = SqliteSql::migrated();
        for i in 0..n {
            let (txid, _) = pot(i);
            let (a, b) = (seat(2, i), seat(3, i));
            db.conn.execute_batch(&format!(
                "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind, paramsDecoded, pubA, pubB, pubTower, payPkhA, payPkhB, rakePkh, stakeA, stakeB, feeSats, recoveryHeight, potSats) \
                 VALUES ('{txid}', 0, 0, 1, 'covenant', 1, '{a}', '{b}', '{A}', '{}', '{}', '{}', 1, 1, 1, 958846, 2)",
                "11".repeat(20), "22".repeat(20), "33".repeat(20)
            )).unwrap();
            for (k, (me, them)) in [(&a, &b), (&b, &a)].into_iter().enumerate() {
                db.conn.execute_batch(&format!(
                    "INSERT INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, txid, outputIndex, createdAt, seatSettlePubkey, seatSigHex, sigValid) \
                     VALUES ('{me}', '{them}', 'g{i}', '{txid}', 0, 958846, '3045ab', 'd{k}{i:062x}', 0, 1, '{me}', '00', 1); \
                     INSERT INTO owed_state (identity, computedAtMs, tip, rows, stale, truncated) VALUES ('{me}', 1, 1, 0, 0, 0)"
                )).unwrap();
            }
        }
        (db, (0..n).map(pot).collect())
    }

    /// (N1, the delta review) THE PARK'S TOKEN IS ORDERED AND NEVER REUSED. The review's shapes through the production
    /// handler over the shipped SQL: two hooks of the SAME body at the SAME millisecond (their old generation was the
    /// stamp times 1,000 plus a nonce; with one nonce the older settle deleted all 22 later parks, `left: 0 right:
    /// 22`; with an older nonce above the later one the later park was refused while `parked` said 22). Now each park
    /// takes its token from D1's counter in its own batch: the later hook's 22 tokens are all above the earlier's,
    /// its `parked` is the rows it WROTE (`RETURNING`), the earlier hook's settle (run after) deletes none of them,
    /// and the later hook's detached work cancelled leaves its 22 wake-ups for the drain, which delivers them at the
    /// later tokens. A settle's retry at an old token touches nothing either.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f2_n1_a_later_park_of_the_same_millisecond_survives_the_older_settle() {
        let (db, ops) = mixed_fixture();
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(Relay::default());
        let (a, b) = (Recorded::default(), Recorded::default());
        let at = store.sql.now.get();
        let wall = || tokio::time::sleep(Duration::from_secs(5));
        let oa = serve_pot_changed(
            store.clone(),
            relay.clone(),
            wall(),
            &|f| a.0.borrow_mut().push(f),
            &ops,
            at,
        )
        .await;
        let ga = generations(&store.sql);
        let ob = serve_pot_changed(
            store.clone(),
            relay.clone(),
            wall(),
            &|f| b.0.borrow_mut().push(f),
            &ops,
            at,
        )
        .await;
        let gb = generations(&store.sql);
        assert_eq!(
            (oa.parked, ob.parked),
            (22, 22),
            "each reports the rows it wrote"
        );
        assert_eq!(gb.len(), 22);
        assert!(
            gb.iter().min() > ga.iter().max(),
            "every later token is above every earlier one: {ga:?} then {gb:?}"
        );
        let mut seen = gb.clone();
        seen.dedup();
        assert_eq!(seen.len(), 22, "no token is shared");
        assert_eq!(
            ob.plan
                .generations
                .iter()
                .flatten()
                .map(|g| *g as i64)
                .collect::<Vec<_>>()
                .len(),
            22,
            "the plan holds the tokens the batch returned"
        );
        a.run_all().await;
        b.0.borrow_mut().clear();
        assert_eq!(
            parked_count(&store.sql),
            ob.parked as i64,
            "the later hook's parks survive the older settle"
        );
        assert_eq!(generations(&store.sql), gb, "untouched at the later tokens");
        // a retry at an old token touches nothing
        store
            .write(
                &[],
                &[ParkOp::Retry {
                    wake: ob.plan.wakes()[0].clone(),
                    generation: ga[0] as u64,
                    attempts: 3,
                    next_ms: 0,
                    exhausted_at_ms: Some(1),
                }],
            )
            .await
            .unwrap();
        assert_eq!(generations(&store.sql), gb);
        store.sql.now.set(at + PARK_GRACE_MS);
        let quick = Relay::default();
        let t = drain_parked(&*store, &quick, &test_deadline(5_000)).await;
        assert_eq!(
            (t.due, t.drained),
            (22, 22),
            "the drain delivers the later intents"
        );
        assert_eq!(parked_count(&store.sql), 0);
    }

    fn seat_sends(relay: &Relay) -> Vec<(String, Option<String>, Value)> {
        relay
            .bodies
            .borrow()
            .iter()
            .filter(|(to, _, _)| to != crate::internal_events::POTS_ROOM)
            .cloned()
            .collect()
    }

    /// (N2, the delta review) A FAILED WRITE BATCH ANNOUNCES NOTHING, AND ONE INTENT HAS ONE RELAY ID. The review's
    /// reproduction through the production handler: the first park batch rejected (a trigger refuses the last room
    /// park after the earlier statements ran), its relay calls let succeed, the fault removed, the same eight
    /// outpoints sent again. On `24bbda1` the first hook still scheduled its announce: 28 seat sends and 16 room
    /// sends (`left: 28 right: 14`). Now the 503 hook hands nothing to `wait_until`, and the resend's announce is the
    /// only one: 14 seat sends, 8 room sends. Each seat send carries `messageId` = `seat_message_id(.., "g<token>")`,
    /// 64 hex; a seat send that fails is retried by the drain under the SAME id, so a relay that did file the first
    /// one answers the repeat `ERR_DUPLICATE_MESSAGE`, which reads as delivered.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f2_n2_a_failed_batch_announces_nothing_and_the_resend_once() {
        let (db, ops) = mixed_fixture();
        db.conn
            .execute_batch(&format!(
                "CREATE TRIGGER refuse_park BEFORE INSERT ON pot_announce_parked WHEN NEW.outpoint = '{MARKER_POT}:0' AND NEW.kind = 'room' BEGIN SELECT RAISE(ABORT, 'refused'); END"
            ))
            .unwrap();
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(Relay::default());
        let rec = Recorded::default();
        let wall = || tokio::time::sleep(Duration::from_secs(5));
        let o1 = serve_pot_changed(
            store.clone(),
            relay.clone(),
            wall(),
            &|f| rec.0.borrow_mut().push(f),
            &ops,
            7,
        )
        .await;
        assert!(o1.stale_fault.is_some(), "the batch faulted");
        assert_eq!(
            (o1.parked, parked_count(&store.sql)),
            (0, 0),
            "nothing of it landed"
        );
        assert_eq!(
            rec.0.borrow().len(),
            0,
            "a failed batch hands no announce to wait_until"
        );
        rec.run_all().await;
        store
            .sql
            .conn
            .execute_batch("DROP TRIGGER refuse_park")
            .unwrap();
        let o2 = serve_pot_changed(
            store.clone(),
            relay.clone(),
            wall(),
            &|f| rec.0.borrow_mut().push(f),
            &ops,
            8,
        )
        .await;
        assert_eq!((o2.stale_fault.clone(), o2.parked), (None, 22));
        rec.run_all().await;
        let (all, room) = {
            let d = relay.delivered.borrow();
            let room = d
                .iter()
                .filter(|(to, _, _)| to == crate::internal_events::POTS_ROOM)
                .count();
            (d.len(), room)
        };
        assert_eq!(all - room, 14, "seat sends");
        assert_eq!(room, 8, "room sends");
        let wakes = o2.plan.wakes();
        for (to, id, body) in seat_sends(&relay) {
            let i = wakes
                .iter()
                .position(|w| {
                    w.recipient.as_deref() == Some(to.as_str())
                        && body["potOutpoint"]["txid"] == w.txid
                })
                .expect("a planned seat");
            let g = o2.plan.generations[i].expect("parked");
            let id = id.expect("a seat send carries its messageId");
            assert_eq!(
                id,
                seat_message_id(&wakes[i].txid, wakes[i].vout, &to, &format!("g{g}"))
            );
            assert!(
                id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()),
                "{id}"
            );
        }
        // the drain's retry of one intent carries the id of its first send
        let (db, ops) = decoded_fixture(1);
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(Relay::default());
        relay.fail_once.borrow_mut().push(seat(2, 0));
        let rec = Recorded::default();
        let at = store.sql.now.get();
        serve_pot_changed(
            store.clone(),
            relay.clone(),
            wall(),
            &|f| rec.0.borrow_mut().push(f),
            &ops,
            at,
        )
        .await;
        rec.run_all().await;
        store.sql.now.set(at + PARK_GRACE_MS + PARK_RETRY_BASE_MS);
        let t = drain_parked(&*store, &*relay, &test_deadline(5_000)).await;
        assert_eq!((t.due, t.drained), (1, 1));
        let ids: Vec<Option<String>> = seat_sends(&relay)
            .into_iter()
            .filter(|(to, _, _)| *to == seat(2, 0))
            .map(|(_, id, _)| id)
            .collect();
        assert_eq!(ids.len(), 2, "the failed send and the drain's retry");
        assert_eq!(ids[0], ids[1], "one intent, one relay id");
        // the relay's answer to a repeated id is delivered; any other refusal is not
        use crate::internal_events::push_answer;
        assert_eq!(push_answer(201, ""), Delivery::Sent);
        assert_eq!(
            push_answer(
                400,
                r#"{"status":"error","code":"ERR_DUPLICATE_MESSAGE","description":"Duplicate message."}"#
            ),
            Delivery::Sent
        );
        assert_eq!(
            push_answer(400, r#"{"status":"error","code":"ERR_INVALID_MESSAGEID"}"#),
            Delivery::Failed
        );
        assert_eq!(push_answer(503, "ERR_DUPLICATE_MESSAGE"), Delivery::Failed);
        // the worker's push sends the id flat, where the relay's `split_push_body` keeps it
        let ie = squash(&code_only(include_str!("internal_events.rs")));
        assert!(
            ie.contains(&squash("payload[\"messageId\"] = json!(id);")),
            "the payload carries messageId"
        );
        assert!(ie.contains(&squash("if push_answer(status, &body) == Delivery::Sent {")));
        let src = squash(&code_only(include_str!("pot_hook.rs")));
        let serve = &src[src.find("pubasyncfnserve_pot_changed<").unwrap()..];
        assert!(
            serve.contains(&squash("if outcome.stale_fault.is_none() {")),
            "the announce is scheduled only on a committed batch"
        );
    }

    /// (N3, the delta review) THE PARK IS BOUNDED AND ITS DRAIN FOLLOWS THE BACKLOG. The review's reproduction: 400
    /// distinct capped bodies whose detached work is cancelled left 3,200 parks on `24bbda1` (`left: 3200 right: <=
    /// 2048`), and a pass drained 24. Now the table holds 2,048: the 144 bodies past it are refused WHOLE (N6 of the
    /// second delta review: 503, nothing announced), their 1,152 wake-ups counted and logged. A pass runs rounds while the backlog fills them: 96 delivered, not 24. A live row
    /// due for longer than a day is aged out (exhausted, then swept), at most 50 a pass.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f2_n3_the_park_is_bounded_and_the_drain_follows_the_backlog() {
        let store = Rc::new(HookD1 {
            sql: SqliteSql::migrated(),
        });
        let relay = Rc::new(Relay::default());
        let at = store.sql.now.get();
        let mut last = PotChangedOutcome::default();
        for b in 0..400u32 {
            let ops: Vec<(String, u32)> = (0..8u32)
                .map(|i| (format!("a{b:031x}{i:032x}"), 0))
                .collect();
            let rec = Recorded::default();
            last = serve_pot_changed(
                store.clone(),
                relay.clone(),
                std::future::pending::<()>(),
                &|f| rec.0.borrow_mut().push(f),
                &ops,
                at,
            )
            .await;
            if b == 399 {
                assert_eq!(
                    rec.0.borrow().len(),
                    0,
                    "N6: a refused body announces nothing"
                );
            }
        }
        let n = parked_count(&store.sql);
        assert_eq!(
            n,
            i64::from(PARK_MAX_ROWS),
            "the parked table is bounded: left: {n} right: <= 2048"
        );
        assert_eq!(
            store.sql.counter(COUNTER_ANNOUNCE_PARK_FULL),
            3200 - u64::from(PARK_MAX_ROWS)
        );
        assert_eq!(
            (last.parked, last.park_full),
            (0, 8),
            "the last body answers what it parked"
        );
        assert!(
            last.stale_fault.is_some(),
            "N6: a refused body is answered 503"
        );
        assert!(store
            .sql
            .lines
            .borrow()
            .iter()
            .any(|l| l.contains("the park is FULL")));
        // a key already parked is re-parked at the ceiling (a new intent is never refused for an existing key)
        let first: Vec<(String, u32)> = (0..8u32)
            .map(|i| (format!("a{:031x}{i:032x}", 0), 0))
            .collect();
        let rec = Recorded::default();
        let again = serve_pot_changed(
            store.clone(),
            relay.clone(),
            std::future::pending::<()>(),
            &|f| rec.0.borrow_mut().push(f),
            &first,
            at,
        )
        .await;
        assert_eq!((again.parked, again.park_full), (8, 0));
        // the drain follows the backlog: four rounds of 24
        store.sql.now.set(at + PARK_GRACE_MS);
        let quick = Relay::default();
        let t = drain_parked(&*store, &quick, &test_deadline(5_000)).await;
        assert_eq!(
            (t.rounds, t.due, t.drained),
            (u64::from(DRAIN_MAX_ROUNDS), 96, 96)
        );
        assert_eq!(parked_count(&store.sql), i64::from(PARK_MAX_ROWS) - 96);
        // the age-out: rows due for more than a day leave the drain, 50 a pass, swept a day after
        store
            .sql
            .now
            .set(at + PARK_GRACE_MS + PARK_OVERDUE_MAX_MS + 1);
        let down = SlowRelay::new(Duration::ZERO, Delivery::Failed);
        let t = drain_parked(&*store, &down, &test_deadline(5_000)).await;
        assert_eq!(t.aged, u64::from(SWEEP_MAX_PER_PASS));
        assert_eq!(
            store.sql.counter(COUNTER_ANNOUNCE_AGED),
            u64::from(SWEEP_MAX_PER_PASS)
        );
        let aged: i64 = store
            .sql
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pot_announce_parked WHERE exhausted_at_ms IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(aged, i64::from(SWEEP_MAX_PER_PASS));
        assert_eq!(
            t.rounds, 1,
            "a round that only failed is not followed by another while the rest backs off"
        );
        const {
            assert!(DRAIN_MAX_ROUNDS * DRAIN_MAX_PER_PASS == 96);
        }
    }

    /// (N4, the delta review) A HUNG TRACKER CALL LEAVES THE DRAIN RUNNING. On `24bbda1` the drain ran after the
    /// tracker's backfill and tick in ONE scheduled task, and the backfill awaits CHAINTRACKS with no timeout. Now the
    /// scheduled tick hands two tasks to `wait_until`, the drain FIRST ([`schedule_tick`]): with the tracker's future
    /// never answering, the drain's pass over the shipped SQL delivers the due wake-up. Source: the worker's
    /// `scheduled` calls `schedule_tick` with `ctx.wait_until`, its drain task holds the drain and not the tracker.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f2_n4_a_hung_tracker_call_leaves_the_drain_running() {
        let lib = squash(&code_only(include_str!("lib.rs")));
        let sched = &lib[lib.find(&squash("async fn scheduled(")).expect("scheduled")..];
        let sched = &sched[..sched
            .find(&squash("#[event(fetch)]"))
            .unwrap_or(sched.len())];
        assert!(
            sched.contains(&squash(
                "pot_hook::schedule_tick(&|f| ctx.wait_until(f), drain, intake, tracker);"
            )),
            "the drain is its own task, scheduled before the intake and the tracker work"
        );
        let drain_at = sched
            .find(&squash("let drain: pot_hook::Detached"))
            .expect("the drain task");
        let tracker_at = sched
            .find(&squash("let tracker: pot_hook::Detached"))
            .expect("the tracker task");
        let drain_task = &sched[drain_at..tracker_at];
        assert!(drain_task.contains("drain_pot_announce(") && !drain_task.contains("backfill("));
        assert!(
            sched[tracker_at..].contains("backfill(")
                && !sched[tracker_at..].contains("drain_pot_announce(")
        );
        assert!(
            include_str!("routes.rs").contains("crate::pot_hook::DRAIN_WALL_MS,"),
            "the drain's own wall"
        );

        let store = Rc::new(HookD1 {
            sql: SqliteSql::migrated(),
        });
        let now = store.sql.now.get();
        let wake = Wake {
            txid: MARKER_POT.to_string(),
            vout: 0,
            recipient: None,
        };
        store
            .write(
                &[],
                &[ParkOp::Park {
                    wake,
                    at_ms: now,
                    next_ms: now,
                }],
            )
            .await
            .unwrap();
        let relay = Rc::new(Relay::default());
        let (s2, r2) = (store.clone(), relay.clone());
        let drain: Detached = Box::pin(async move {
            drain_parked(&*s2, &*r2, &test_deadline(5_000)).await;
        });
        let tracker: Detached = Box::pin(std::future::pending::<()>());
        let intake: Detached = Box::pin(std::future::pending::<()>());
        let rec = Recorded::default();
        schedule_tick(&|f| rec.0.borrow_mut().push(f), drain, intake, tracker);
        assert_eq!(rec.0.borrow().len(), 3, "three tasks");
        let _ = tokio::time::timeout(Duration::from_secs(2), rec.run_all()).await;
        assert_eq!(
            relay.delivered.borrow().len(),
            1,
            "the drain delivered under a hung tracker"
        );
        assert_eq!(parked_count(&store.sql), 0);
    }

    /// (N5, the delta review) THE EMITTED BODIES, BYTE FOR BYTE, THROUGH THE PRODUCTION SEAM. The pins `24bbda1`
    /// amended no longer asserted the emitted event: a `carrier_entry` answering `json!(null)` passed all seven, and
    /// the client (`app/src/lib/lowEvents.ts` `parseLowEvent`) refuses an event whose `entry` is not an object. Here
    /// the production handler's detached announce over the mixed body: every seat's `pot` event is exactly
    /// `{"v":1,"kind":"pot","potOutpoint":{..},"at":..,"entry":{}}` and every room's the outpoint alone, each as the
    /// bytes the relay is handed.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f2_n5_a_the_emitted_event_bodies_byte_for_byte() {
        let (db, ops) = mixed_fixture();
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(Relay::default());
        let rec = Recorded::default();
        let out = serve_pot_changed(
            store.clone(),
            relay.clone(),
            tokio::time::sleep(Duration::from_secs(5)),
            &|f| rec.0.borrow_mut().push(f),
            &ops,
            7,
        )
        .await;
        rec.run_all().await;
        let bodies = relay.bodies.borrow();
        assert_eq!(bodies.len(), 22);
        let mut seats = 0;
        for (to, _, body) in bodies.iter() {
            let txid = body["potOutpoint"]["txid"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if to == crate::internal_events::POTS_ROOM {
                let mut want = crate::internal_events::pot_changed_event_body(&txid, 0, 7);
                want["intentId"] = body["intentId"].clone();
                // bsv-low #668 (step 0): additive hop stamps — hookedAt = at = 7, broadcastAt = the store clock
                // (t0, 1_000_000 in this fixture); no admittedAt (the wrapper passes None).
                want["hookedAt"] = json!(7);
                want["broadcastAt"] = json!(1_000_000);
                assert_eq!(
                    body, &want,
                    "the room's body, its intent id and the #668 hop stamps beside"
                );
                continue;
            }
            seats += 1;
            assert!(
                body["entry"].is_object(),
                "the client requires an object entry: {body}"
            );
            assert_eq!(
                body.to_string(),
                format!(
                    r#"{{"at":7,"broadcastAt":1000000,"entry":{{}},"hookedAt":7,"kind":"pot","potOutpoint":{{"txid":"{txid}","vout":0}},"v":1}}"#
                ),
                "the seat's pot event, byte for byte (bsv-low #668: the additive hop stamps)"
            );
            assert!(out
                .filed
                .iter()
                .any(|f| f["identity"] == to.as_str() && f["txid"] == txid.as_str()));
        }
        assert_eq!(seats, 14);
    }

    /// (N5) EVERY BRANCH OF THE DECISION, THROUGH `apply_pot_changed` (the seam the worker's handler runs): a read that
    /// failed, no params with and without marker-named seats, decoded with no attributed seat with and without
    /// markers, and an attributed pot; each branch's `skipped` words, its stale marks, its queued identities and its
    /// parks (a room per outpoint, a seat per attributed identity). And the real store's failed branch: with
    /// `pot_records` unavailable every outpoint is skipped as a read failure and only its room is parked.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f2_n5_b_every_branch_of_the_decision() {
        struct Scripted {
            inner: HookD1<SqliteSql>,
            reads: Vec<OutpointRead>,
        }
        impl HookStore for Scripted {
            async fn read_outpoints(&self, _o: &[(String, u32)]) -> Vec<OutpointRead> {
                self.reads.clone()
            }
            async fn write(
                &self,
                marks: &[(String, &'static str)],
                ops: &[ParkOp],
            ) -> Result<Vec<OpOut>, String> {
                self.inner.write(marks, ops).await
            }
            async fn due_parked(&self, now: u64, limit: u32) -> Result<Vec<Parked>, String> {
                self.inner.due_parked(now, limit).await
            }
            async fn counters(&self, d: Vec<(String, u64)>) {
                self.inner.counters(d).await
            }
            fn now_ms(&self) -> u64 {
                self.inner.now_ms()
            }
            fn log(&self, line: &str) {
                self.inner.log(line)
            }
        }
        let (m, n) = (marker_seat(0), marker_seat(1));
        let store = Scripted {
            inner: HookD1 {
                sql: SqliteSql::migrated(),
            },
            reads: vec![
                OutpointRead::Failed("boom".into()),
                OutpointRead::NoParams {
                    marker_named: vec![m.clone(), n.clone()],
                },
                OutpointRead::NoParams {
                    marker_named: vec![],
                },
                OutpointRead::Decoded {
                    seats: vec![],
                    marker_named: vec![m.clone()],
                },
                OutpointRead::Decoded {
                    seats: vec![],
                    marker_named: vec![],
                },
                OutpointRead::Decoded {
                    seats: vec![seat(2, 0), seat(3, 0)],
                    marker_named: vec![],
                },
            ],
        };
        let ops: Vec<(String, u32)> = (0..6).map(op).collect();
        let out = apply_pot_changed(&store, &ops, 7, None).await;
        let why: Vec<String> = out
            .skipped
            .iter()
            .map(|s| s["why"].as_str().unwrap().to_string())
            .collect();
        let o = |i: usize| format!("{}:{}", ops[i].0, ops[i].1);
        assert_eq!(
            why,
            vec![
                format!("[pot-changed] decoded-pots read failed for {}: boom", o(0)),
                format!("[pot-changed] {} has no decoded params yet, nothing to file (2 seat(s) attributed by marker)", o(1)),
                format!("[pot-changed] {} has no decoded params yet, nothing to file (0 seat(s) attributed by marker)", o(2)),
                format!("[pot-changed] {} has no attributed seats yet, nothing to file (1 seat(s) marked stale by marker)", o(3)),
                format!("[pot-changed] {} has no attributed seats yet, nothing to file (0 seat(s) marked stale by marker)", o(4)),
            ]
        );
        assert_eq!(
            out.stale_marks,
            vec![
                (m.clone(), "pot-changed (by marker)"),
                (n.clone(), "pot-changed (by marker)"),
                (m.clone(), "pot-changed (unattributed, by marker)"),
            ]
        );
        assert_eq!(
            out.owed_identities,
            vec![m.clone(), n.clone(), seat(2, 0), seat(3, 0)]
        );
        assert_eq!(
            out.filed,
            vec![
                json!({ "txid": ops[5].0, "vout": ops[5].1, "identity": seat(2, 0) }),
                json!({ "txid": ops[5].0, "vout": ops[5].1, "identity": seat(3, 0) }),
            ]
        );
        assert_eq!(
            (out.parked, out.park_full, out.stale_fault),
            (8, 0, None),
            "six rooms and two seats"
        );

        // the real store, the required read unavailable: every outpoint skipped as a read failure, rooms parked
        let (db, ops) = mixed_fixture();
        db.conn
            .execute_batch("ALTER TABLE pot_records RENAME TO pot_records_away")
            .unwrap();
        let store = HookD1 { sql: db };
        let out = apply_pot_changed(&store, &ops, 7, None).await;
        assert_eq!((out.filed.len(), out.skipped.len(), out.parked), (0, 8, 8));
        assert!(out.skipped.iter().all(|s| s["why"]
            .as_str()
            .unwrap()
            .contains("decoded-pots read failed")));
        assert!(store
            .sql
            .lines
            .borrow()
            .iter()
            .any(|l| l.contains("each outpoint asked alone")));
    }

    /// (N5) THE HOP BATCH: eight identities marked stale in ONE batch of eight statements, each landed.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f2_n5_c_the_hop_hook_marks_eight_identities_in_one_batch() {
        let (db, _) = decoded_fixture(4);
        let store = HookD1 { sql: db };
        let ids: Vec<String> = (0..4).flat_map(|i| [seat(2, i), seat(3, i)]).collect();
        apply_hop_changed(&store, &ids).await.unwrap();
        assert_eq!(
            (store.sql.batches.get(), store.sql.stmts.get()),
            (1, 8),
            "one batch, a statement per identity"
        );
        let stale: i64 = store
            .sql
            .conn
            .query_row("SELECT COUNT(*) FROM owed_state WHERE stale = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(stale, 8);
        apply_hop_changed(&store, &[]).await.unwrap();
        assert_eq!(store.sql.batches.get(), 1, "no identity, no batch");
    }

    /// (N5) ALL 24 CALLS SLOW: a full body (8 decoded pots, 16 attributed seats) with EVERY relay call past the
    /// wall. The answer is in hand with no call started and all 24 wake-ups parked; the detached announce runs all 24
    /// together into the one wall, counts 24 failed, and leaves all 24 for the drain at attempt 1.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f2_n5_d_all_24_calls_slow_are_parked_for_the_drain() {
        let (db, ops) = decoded_fixture(8);
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(SlowRelay::new(Duration::from_secs(40), Delivery::Sent));
        let rec = Recorded::default();
        let t0 = Instant::now();
        let out = serve_pot_changed(
            store.clone(),
            relay,
            tokio::time::sleep(Duration::from_millis(300)),
            &|f| rec.0.borrow_mut().push(f),
            &ops,
            store.sql.now.get(),
        )
        .await;
        assert!(t0.elapsed() < Duration::from_secs(1));
        assert_eq!(
            (out.filed.len(), out.parked, out.plan.calls()),
            (16, 24, 24)
        );
        let t1 = Instant::now();
        rec.run_all().await;
        assert!(
            t1.elapsed() < Duration::from_secs(2),
            "the 24 run together into one wall"
        );
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_FAILED), 24);
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_PARKED), 24);
        let rows = parked_rows(&store.sql);
        assert_eq!(rows.len(), 24);
        assert!(
            rows.iter().all(|r| r.3 == 1 && r.4.is_none()),
            "each at attempt 1, live"
        );
    }

    /// The table filled to `rows` directly (older live rows, attempts 0, due at once; keys `9..`, apart from every
    /// fixture's), for the ceiling's pins.
    fn fill_parked(db: &SqliteSql, rows: u32) {
        db.conn
            .execute_batch(&format!(
                "WITH RECURSIVE k(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM k WHERE i + 1 < {rows}) \
                 INSERT INTO pot_announce_parked (outpoint, kind, recipient, at, attempts, next_attempt_at_ms, generation, exhausted_at_ms) \
                 SELECT printf('9%063x:0', i), 'room', '', i, 0, i, i + 1, NULL FROM k"
            ))
            .unwrap();
    }

    /// (N6, the second delta review) A WAKE-UP IS NEVER ACKNOWLEDGED UNPARKED. The review's reproduction on
    /// `c830527`: 400 capped bodies with cancelled announces leave 2,048 rows; the mixed body then answered 200 with
    /// `parkFull: 22` and volatile sends; its announce cancelled, 22 drain passes over the 2,048 older rows never
    /// delivered one of its wake-ups (RED: `left: 0 right: 22`). Now that body is refused WHOLE: 503, nothing written
    /// (its stale marks rolled back too), nothing announced, counted; the producer's re-send after the drain has made
    /// room parks all 22, and the drain delivers them though the re-sent announce is cancelled too.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f3_n6_a_the_review_sequence_never_loses_the_mixed_body_silently() {
        let (db, mixed) = mixed_fixture();
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(Relay::default());
        let at = store.sql.now.get();
        for b in 0..400u32 {
            let ops: Vec<(String, u32)> = (0..8u32)
                .map(|i| (format!("a{b:031x}{i:032x}"), 0))
                .collect();
            let rec = Recorded::default();
            serve_pot_changed(
                store.clone(),
                relay.clone(),
                std::future::pending::<()>(),
                &|f| rec.0.borrow_mut().push(f),
                &ops,
                at,
            )
            .await;
        }
        assert_eq!(parked_count(&store.sql), i64::from(PARK_MAX_ROWS));
        let stale = |db: &SqliteSql| -> i64 {
            db.conn
                .query_row("SELECT COUNT(*) FROM owed_state WHERE stale = 1", [], |r| {
                    r.get(0)
                })
                .unwrap()
        };
        let rec = Recorded::default();
        let o = serve_pot_changed(
            store.clone(),
            relay.clone(),
            std::future::pending::<()>(),
            &|f| rec.0.borrow_mut().push(f),
            &mixed,
            at,
        )
        .await;
        assert_eq!(
            (o.parked, o.park_full, rec.0.borrow().len()),
            (0, 22, 0),
            "refused whole, nothing announced"
        );
        assert!(
            o.stale_fault
                .as_deref()
                .is_some_and(|e| e.contains("park is full")),
            "answered 503: {:?}",
            o.stale_fault
        );
        assert_eq!(stale(&store.sql), 0, "the batch rolled back whole");
        assert_eq!(parked_count(&store.sql), i64::from(PARK_MAX_ROWS));
        assert_eq!(
            store.sql.counter(COUNTER_ANNOUNCE_PARK_FULL),
            1152 + 22,
            "every refused wake-up counted"
        );
        // the producer re-sends a retryable answer; its announce is cancelled again; the drain delivers
        let quick = Relay::default();
        let mut resent = None;
        for pass in 0..30u64 {
            store.sql.now.set(at + PARK_GRACE_MS + pass * 900_000);
            drain_parked(&*store, &quick, &test_deadline(5_000)).await;
            if resent.is_none() && parked_count(&store.sql) <= i64::from(PARK_MAX_ROWS) - 22 {
                let rec = Recorded::default();
                let again = serve_pot_changed(
                    store.clone(),
                    relay.clone(),
                    std::future::pending::<()>(),
                    &|f| rec.0.borrow_mut().push(f),
                    &mixed,
                    store.sql.now.get() - PARK_GRACE_MS,
                )
                .await;
                assert_eq!((again.parked, again.park_full), (22, 0));
                resent = Some(again);
            }
        }
        let txids: Vec<String> = mixed.iter().map(|(t, _)| t.clone()).collect();
        let got = quick
            .delivered
            .borrow()
            .iter()
            .filter(|(_, t, _)| txids.contains(t))
            .count();
        let lost = store
            .sql
            .lines
            .borrow()
            .iter()
            .filter(|l| l.contains("LOST"))
            .count();
        assert_eq!(
            got + lost,
            22,
            "every wake-up of the mixed body delivered or named lost"
        );
        assert_eq!(got, 22);
    }

    /// (N6) THE ROOM A BATCH MAY MAKE, and its bound. At 2,040 rows the mixed body's 22 wake-ups passed the ceiling by
    /// 14; on `c830527` its eight rooms took the slots and the fourteen seats were refused, so with every send failing
    /// no seat retry row existed (the review's `left: 0 right: 14`). Now: (1) with no row to displace, the body is
    /// refused whole; (2) the oldest EXHAUSTED rows make the room first (counted, nothing named lost); (3) then the
    /// oldest LIVE rows that failed at least [`PARK_DISPLACE_MIN_ATTEMPTS`] deliveries, each named LOST in the log and
    /// counted; a live row under that count is never displaced; and every failed seat send of a parked body leaves its
    /// retry row.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f3_n6_b_room_is_made_of_exhausted_then_failing_live_rows_and_past_that_refused() {
        let (db, mixed) = mixed_fixture();
        fill_parked(&db, PARK_MAX_ROWS - 8);
        let store = Rc::new(HookD1 { sql: db });
        let down = Rc::new(SlowRelay::new(Duration::ZERO, Delivery::Failed));
        let at = store.sql.now.get();
        let serve = |ops: Vec<(String, u32)>| {
            let (store, down) = (store.clone(), down.clone());
            async move {
                let rec = Recorded::default();
                let o = serve_pot_changed(
                    store,
                    down,
                    std::future::pending::<()>(),
                    &|f| rec.0.borrow_mut().push(f),
                    &ops,
                    at,
                )
                .await;
                rec.run_all().await;
                o
            }
        };
        // (1) attempts under the bound: nothing to displace, refused whole
        store
            .sql
            .conn
            .execute_batch(&format!(
                "UPDATE pot_announce_parked SET attempts = {}",
                PARK_DISPLACE_MIN_ATTEMPTS - 1
            ))
            .unwrap();
        let o = serve(mixed.clone()).await;
        assert_eq!((o.parked, o.park_full), (0, 22));
        assert_eq!(parked_count(&store.sql), i64::from(PARK_MAX_ROWS) - 8);
        // (2) twenty exhausted rows: the fourteen oldest make the room, nothing named lost
        store
            .sql
            .conn
            .execute_batch(
                "UPDATE pot_announce_parked SET exhausted_at_ms = at WHERE rowid IN (SELECT rowid FROM pot_announce_parked ORDER BY at DESC LIMIT 20)",
            )
            .unwrap();
        let o = serve(mixed.clone()).await;
        assert_eq!((o.parked, o.park_full), (22, 0));
        assert!(o.stale_fault.is_none());
        assert_eq!(parked_count(&store.sql), i64::from(PARK_MAX_ROWS));
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_EVICTED_EXHAUSTED), 14);
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_DISPLACED), 0);
        let seat_retries: i64 = store
            .sql
            .conn
            .query_row(
                "SELECT COUNT(*) FROM pot_announce_parked WHERE kind = 'seat' AND attempts = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            seat_retries, 14,
            "every failed seat send keeps its retry row"
        );
        // (3) a new 8-room body: six exhausted rows left, then two live rows at the bound, oldest first, named LOST
        store
            .sql
            .conn
            .execute_batch(&format!(
                "UPDATE pot_announce_parked SET attempts = {} WHERE outpoint IN ('{}:0', '{}:0')",
                PARK_DISPLACE_MIN_ATTEMPTS,
                format_args!("9{:063x}", 0),
                format_args!("9{:063x}", 1)
            ))
            .unwrap();
        let fresh: Vec<(String, u32)> = (0..8u32).map(|i| (format!("b{i:063x}"), 0)).collect();
        let o = serve(fresh).await;
        assert_eq!((o.parked, o.park_full), (8, 0));
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_EVICTED_EXHAUSTED), 20);
        assert_eq!(store.sql.counter(COUNTER_ANNOUNCE_DISPLACED), 2);
        let lost: Vec<String> = store
            .sql
            .lines
            .borrow()
            .iter()
            .filter(|l| l.contains("LOST"))
            .cloned()
            .collect();
        assert_eq!(lost.len(), 2, "{lost:?}");
        assert!(lost[0].contains(&format!("9{:063x}:0 room", 0)), "{lost:?}");
        assert!(lost[1].contains(&format!("9{:063x}:0 room", 1)), "{lost:?}");
        // (4) full again of rows under the bound (the batch's own rows included): the next body is refused whole
        let more: Vec<(String, u32)> = (0..8u32).map(|i| (format!("c{i:063x}"), 0)).collect();
        let o = serve(more).await;
        assert_eq!((o.parked, o.park_full), (0, 8));
        assert_eq!(parked_count(&store.sql), i64::from(PARK_MAX_ROWS));
    }

    /// A deadline `ms` from now over `Instant` and tokio's sleep, no reserve (the pins' fake relay is quick).
    fn test_deadline(ms: u64) -> Deadline {
        test_deadline_reserving(ms, 0)
    }
    fn test_deadline_reserving(ms: u64, reserve_ms: u64) -> Deadline {
        let t0 = Instant::now();
        Deadline::new(
            ms,
            reserve_ms,
            Box::new(move || t0.elapsed().as_millis() as u64),
            Box::new(|ms| Box::pin(tokio::time::sleep(Duration::from_millis(ms)))),
        )
    }

    /// [`test_deadline_reserving`] on tokio's clock, for a pin run under `start_paused` (bsv-low #767): the deadline's
    /// clock and timer are the runtime's virtual time, which moves only when every task waits, so a pass's elapsed time
    /// is what its timers say and nothing the host's scheduler adds.
    fn paused_deadline_reserving(ms: u64, reserve_ms: u64) -> Deadline {
        let t0 = tokio::time::Instant::now();
        Deadline::new(
            ms,
            reserve_ms,
            Box::new(move || t0.elapsed().as_millis() as u64),
            Box::new(|ms| Box::pin(tokio::time::sleep(Duration::from_millis(ms)))),
        )
    }

    /// The shipped store with SLOW (or never answering) D1 round trips: the due read and the write batch each sleep
    /// their delay before the real SQL runs (a dropped call runs none).
    struct SlowStore<'a> {
        inner: &'a HookD1<SqliteSql>,
        read: Option<Duration>,
        write: Option<Duration>,
    }
    /// A host that stalls on every D1 round trip (a loaded scheduler: bsv-low #767 measured 63 ms against a 60 ms bar
    /// at a load of 13). The stall blocks the thread, as a descheduled isolate does; the pin's clock is tokio's paused
    /// one, which a blocked thread does not move, so the pin's verdict is the deadline's arithmetic, never the host's.
    const HOST_STALL: Duration = Duration::from_millis(25);
    async fn slow(d: Option<Duration>) {
        std::thread::sleep(HOST_STALL);
        match d {
            Some(d) => tokio::time::sleep(d).await,
            None => std::future::pending::<()>().await,
        }
    }
    impl HookStore for SlowStore<'_> {
        async fn read_outpoints(&self, o: &[(String, u32)]) -> Vec<OutpointRead> {
            self.inner.read_outpoints(o).await
        }
        async fn write(
            &self,
            marks: &[(String, &'static str)],
            ops: &[ParkOp],
        ) -> Result<Vec<OpOut>, String> {
            slow(self.write).await;
            self.inner.write(marks, ops).await
        }
        async fn due_parked(&self, now: u64, limit: u32) -> Result<Vec<Parked>, String> {
            slow(self.read).await;
            self.inner.due_parked(now, limit).await
        }
        async fn counters(&self, deltas: Vec<(String, u64)>) {
            self.inner.counters(deltas).await
        }
        fn now_ms(&self) -> u64 {
            self.inner.now_ms()
        }
        fn log(&self, line: &str) {
            self.inner.log(line)
        }
    }

    /// (N9, the third delta review) AN EXPIRED DRAIN LEAVES THE COURIER DELTAS QUEUED. On `b792b85` the drain task
    /// took the pending deltas (`take_pending_if_due`, which empties the map) BEFORE the deadline's race, and an
    /// expired deadline returned without polling the flush: the deltas were dropped (RED: the next take was `[]`).
    /// Now the take runs inside the raced future (`courier::take_and_flush`): a deadline that has fallen polls
    /// nothing, the delta is still there for the next take, and a live one takes and flushes it once.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f4_n9_an_expired_drain_leaves_the_courier_deltas_queued() {
        let lib = squash(&code_only(include_str!("lib.rs")));
        let sched = &lib[lib.find(&squash("async fn scheduled(")).expect("scheduled")..];
        let task = &sched[sched
            .find(&squash("let drain: pot_hook::Detached"))
            .expect("the drain task")
            ..sched
                .find(&squash("let tracker: pot_hook::Detached"))
                .expect("the tracker task")];
        assert!(
            !task.contains(&squash("take_pending_if_due")),
            "the drain task takes the courier deltas inside the deadline's race, never before it"
        );
        assert!(
            task.contains(&squash(
                "deadline.run(courier::take_and_flush(now, |pending| { courier::flush_ack(Some(db), pending) }))"
            )),
            "the drain task races the take and the flush as one future"
        );

        let name = "e649f4_n9_probe_total";
        crate::courier::note_presence(name);
        let flushed = std::cell::RefCell::new(Vec::<(String, u64)>::new());
        let expired = test_deadline(0);
        let out = expired
            .run(crate::courier::take_and_flush(0.0, |p| {
                flushed.borrow_mut().extend(p);
                async { true }
            }))
            .await;
        assert!(out.is_none(), "the deadline had fallen");
        assert!(
            flushed.borrow().is_empty(),
            "an expired deadline polls no flush"
        );
        let live = test_deadline(1_000);
        let out = live
            .run(crate::courier::take_and_flush(0.0, |p| {
                flushed.borrow_mut().extend(p);
                async { true }
            }))
            .await;
        assert!(out.is_some());
        let got = flushed.borrow().clone();
        assert!(
            got.iter().any(|(n, d)| n == name && *d == 1),
            "the delta an expired drain left is taken and flushed by the next: {got:?}"
        );
        assert!(
            crate::courier::take_pending_if_due(1.0e12).is_empty(),
            "flushed once, not twice"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn e658_expiry_during_the_courier_flush_leaves_the_batch_for_the_next_take() {
        let name = "e658_cancelled_flush_total";
        crate::courier::note_presence_n(name, 7);
        let entered = std::cell::Cell::new(false);
        let out = paused_deadline_reserving(40, 0)
            .run(crate::courier::take_and_flush(0.0, |pending| {
                assert!(pending.iter().any(|(n, count)| n == name && *count == 7));
                entered.set(true);
                // New increments during the await must survive alongside the returned batch.
                crate::courier::note_presence_n(name, 3);
                std::future::pending()
            }))
            .await;
        assert!(entered.get(), "the deadline must expire after the take");
        assert!(out.is_none());
        let next = crate::courier::take_pending_if_due(1.0e12);
        assert_eq!(
            next.iter()
                .find(|(n, _)| n == name)
                .map(|(_, count)| *count),
            Some(10),
            "cancellation must return the taken batch without replacing newer increments"
        );
    }

    /// (N7, the second delta review) THE DRAIN TASK HAS ONE DEADLINE. On `c830527` the shared timer raced the relay
    /// futures only: an EMPTY pass with an 80 ms due read and an 80 ms maintenance write returned after 174 ms under
    /// a 40 ms wall (RED: "took 174 ms under 40 ms"), and a pending store read never reached the race. Now every await
    /// of the pass runs under [`Deadline`]: (a) that empty pass returns at the deadline; (b) a never-answering due
    /// read too; (c) 24 due rows delivered by a quick relay whose SETTLE is slow return at the deadline with the rows
    /// RETAINED as they were (attempts 0, still parked at their tokens, retried by a later pass); (d) a relay that
    /// never answers is stopped at the reserve and its rows are settled as failed attempts before the deadline. The
    /// worker's drain task runs its boot and its courier flush under the same deadline. The pass runs on tokio's PAUSED
    /// clock with every D1 round trip stalling the thread [`HOST_STALL`] (bsv-low #767: on the wall clock a loaded host
    /// missed the bar by scheduling alone, 63 ms against 60): each bound is the deadline's own arithmetic.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn e649f3_n7_the_drain_task_is_bounded_by_one_deadline() {
        let lib = squash(&code_only(include_str!("lib.rs")));
        let sched = &lib[lib.find(&squash("async fn scheduled(")).expect("scheduled")..];
        let task = &sched[sched
            .find(&squash("let drain: pot_hook::Detached"))
            .expect("the drain task")
            ..sched
                .find(&squash("let tracker: pot_hook::Detached"))
                .expect("the tracker task")];
        for step in [
            "let deadline = routes::worker_deadline(pot_hook::DRAIN_WALL_MS, pot_hook::DRAIN_SETTLE_RESERVE_MS);",
            ".run(d1_ledger::boot(schema::ensure_latch_columns(&env_drain)))",
            "routes::drain_pot_announce(&env_drain, &deadline)",
            "deadline.run(courier::take_and_flush(now, |pending| { courier::flush_ack(Some(db), pending) }))",
        ] {
            assert!(task.contains(&squash(step)), "the drain task: {step}");
        }
        let src = squash(&code_only(include_str!("pot_hook.rs")));
        let pass = &src[src
            .find(&squash("pub async fn drain_parked<"))
            .expect("the pass")
            ..src
                .find(&squash("pub async fn serve_pot_changed<"))
                .expect("the hook")];
        for step in [
            ".run_reserving(store.due_parked(now, DRAIN_MAX_PER_PASS))",
            ".run_reserving(futures_util::future::join_all(calls))",
            "deadline.run(store.write(&[], &ops))",
            "deadline.run(store.counters(counters))",
        ] {
            assert!(pass.contains(&squash(step)), "the pass: {step}");
        }
        assert_eq!(
            pass.matches(".await").count(),
            5,
            "the pass awaits nothing but its four deadline runs and the relay call inside the raced join"
        );

        let store = HookD1 {
            sql: SqliteSql::migrated(),
        };
        let ms = Duration::from_millis;
        // (a) the review's empty pass
        let s = SlowStore {
            inner: &store,
            read: Some(ms(80)),
            write: Some(ms(80)),
        };
        let t0 = tokio::time::Instant::now();
        drain_parked(&s, &Relay::default(), &paused_deadline_reserving(40, 10)).await;
        let took = t0.elapsed().as_millis();
        assert!(took <= 40, "the empty pass took {took} ms under 40 ms");
        // (b) a never-answering due read
        let s = SlowStore {
            inner: &store,
            read: None,
            write: None,
        };
        let t0 = tokio::time::Instant::now();
        let t = drain_parked(&s, &Relay::default(), &paused_deadline_reserving(40, 10)).await;
        let took = t0.elapsed().as_millis();
        assert!(took <= 40, "a pending read took {took} ms under 40 ms");
        assert_eq!(t.rounds, 0);
        // (c) 24 due rows, a quick relay, a slow settle: the rows retained as they were
        let now = store.sql.now.get();
        let parks: Vec<ParkOp> = (0..24u32)
            .map(|i| ParkOp::Park {
                wake: Wake {
                    txid: format!("d{i:063x}"),
                    vout: 0,
                    recipient: None,
                },
                at_ms: now,
                next_ms: now,
            })
            .collect();
        store.write(&[], &parks).await.unwrap();
        let before = generations(&store.sql);
        let s = SlowStore {
            inner: &store,
            read: Some(Duration::ZERO),
            write: Some(ms(80)),
        };
        let t0 = tokio::time::Instant::now();
        let quick = Relay::default();
        drain_parked(&s, &quick, &paused_deadline_reserving(40, 10)).await;
        let took = t0.elapsed().as_millis();
        assert!(took <= 40, "the slow settle took {took} ms under 40 ms");
        assert_eq!(quick.delivered.borrow().len(), 24, "the relay took them");
        assert_eq!(generations(&store.sql), before, "retained at their tokens");
        let attempts: i64 = store
            .sql
            .conn
            .query_row("SELECT SUM(attempts) FROM pot_announce_parked", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(attempts, 0, "retained as they were");
        assert!(store
            .sql
            .lines
            .borrow()
            .iter()
            .any(|l| l.contains("drain settle not landed at the deadline")));
        // (d) a relay that never answers: stopped at the reserve, the settle issued before the deadline
        let s = SlowStore {
            inner: &store,
            read: Some(Duration::ZERO),
            write: Some(Duration::ZERO),
        };
        let hung = SlowRelay::new(Duration::from_secs(3600), Delivery::Sent);
        let t0 = tokio::time::Instant::now();
        let t = drain_parked(&s, &hung, &paused_deadline_reserving(60, 30)).await;
        let took = t0.elapsed().as_millis();
        assert!(took <= 60, "the hung relay took {took} ms under 60 ms");
        assert_eq!((t.due, t.retry_failed), (24, 24));
        let attempts: i64 = store
            .sql
            .conn
            .query_row("SELECT SUM(attempts) FROM pot_announce_parked", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(attempts, 24, "each settled as a failed attempt");
    }

    /// (N8, the second delta review) A ROOM INTENT CARRIES ONE ID ACROSS ITS RETRIES. On `c830527` a room broadcast
    /// carried no intent id: a relay that took the first body and answered a failure saw the same body again on the
    /// drain's retry with nothing to tell them apart (RED: `[None, None]`). Now every send of one room intent carries
    /// `intentId`, the [`room_intent_id`] of its park token: the failed send and the drain's retry carry the same 64-hex
    /// id; a later park of the outpoint (a new intent) carries another; the seat ids are unchanged.
    #[tokio::test(flavor = "current_thread")]
    async fn e649f3_n8_a_room_intent_carries_one_id_across_its_retries() {
        let (db, ops) = decoded_fixture(1);
        let store = Rc::new(HookD1 { sql: db });
        let relay = Rc::new(Relay::default());
        relay
            .fail_once
            .borrow_mut()
            .push(crate::internal_events::POTS_ROOM.to_string());
        let at = store.sql.now.get();
        let serve = |at: u64| {
            let (store, relay, ops) = (store.clone(), relay.clone(), ops.clone());
            async move {
                let rec = Recorded::default();
                let o = serve_pot_changed(
                    store,
                    relay,
                    std::future::pending::<()>(),
                    &|f| rec.0.borrow_mut().push(f),
                    &ops,
                    at,
                )
                .await;
                rec.run_all().await;
                o
            }
        };
        let first = serve(at).await;
        store.sql.now.set(at + PARK_GRACE_MS + PARK_RETRY_BASE_MS);
        let t = drain_parked(&*store, &*relay, &test_deadline(5_000)).await;
        assert_eq!((t.due, t.drained), (1, 1));
        let rooms = || -> Vec<Value> {
            relay
                .bodies
                .borrow()
                .iter()
                .filter(|(to, _, _)| to == crate::internal_events::POTS_ROOM)
                .map(|(_, _, b)| b.clone())
                .collect()
        };
        let ids: Vec<Option<String>> = rooms()
            .iter()
            .map(|b| b["intentId"].as_str().map(str::to_string))
            .collect();
        assert_eq!(ids.len(), 2, "the failed room send and its retry");
        assert!(
            ids[0].is_some() && ids[0] == ids[1],
            "one room intent, one id across its retries: {ids:?}"
        );
        let g = first.plan.generations[0].expect("the room parked");
        let (txid, vout) = &ops[0];
        let want = room_intent_id(txid, *vout, &format!("g{g}"));
        assert_eq!(ids[0].as_deref(), Some(want.as_str()));
        assert!(want.len() == 64 && want.bytes().all(|b| b.is_ascii_hexdigit()));
        // a later park of the outpoint is a new intent with a new id
        serve(store.sql.now.get()).await;
        let all = rooms();
        assert_eq!(all.len(), 3);
        assert_ne!(
            all[2]["intentId"], all[0]["intentId"],
            "a new intent, a new id"
        );
        assert_eq!(all[2]["kind"], "pot-changed");
    }
}
