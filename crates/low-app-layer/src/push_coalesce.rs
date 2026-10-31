//! bsv-low #935 (lane FIX-935-PUSH-COALESCE, L52's S2): the app layer's first-party pushes to a seat, coalesced per
//! identity and bounded under the relay's send budget.
//!
//! L52 counted 2,005 `429 ERR_SEND_BUDGET` refusals of the app layer's `/push` calls in ten minutes (16:15Z to
//! 16:25Z): every committed owed judgement pushed its own `owed-changed`, and the relay refused what passed its
//! window, so the seats it capped lost the wake-up. The relay's rule (relay 0.8.2, `src/send_budget.rs`, charged in
//! `src/routes/send_message.rs` step 1b, which `/push` reaches through `handle_send_message`): ONE identity makes at
//! most `SENDS` (100) sends in a tumbling window of 60 s, and the identity charged is the SENDER, here the app
//! layer's own key (`internal_events::sender_pubkey_hex`), whatever the recipient. So the 100 is shared by every
//! seat the app layer pushes to; a bound per seat alone cannot keep it. Two buckets, then:
//!
//! - per identity ([`PUSH_BUDGET_PER_MIN_VAR`], default [`PUSH_BUDGET_PER_MIN_DEFAULT`] 40): one hot seat takes at
//!   most 40 of the sender's 100 in any 60 s, so a storm on one seat leaves 60 for every other seat;
//! - per isolate, over every identity ([`PUSH_SENDER_BUDGET_PER_MIN_VAR`], default 80): the sender's own minute,
//!   20 under the relay's 100. Per ISOLATE, stated: two isolates pushing at once can still pass 100 together; the
//!   fleet-wide answer is the relay half (a first-party producer's pushes not charged to a send window), filed with
//!   the message box captain through the keeper.
//!
//! Each bucket holds at most a quarter of its minute and refills the rest evenly, so any 60 s spends at most its
//! budget (capacity + refill a minute = the budget).
//!
//! THE WINDOW ([`PUSH_COALESCE_MS_VAR`], default 2,000 ms). A trigger kind (one the client answers by re-reading
//! served state: [`coalesce_key`]) is coalesced per (identity, key): a push to a quiet identity goes at once (the
//! leading edge) and opens a window; a push inside the window replaces the pending state of its key (the newest
//! wins, so it carries the newest `computedAtMs` / `at`) and the newest submitter delivers it at the window's end.
//! A state the buckets cannot pay for at the window's end is HELD to the next window, never dropped: the newest
//! state of every key always goes, at most [`PUSH_HOLD_WINDOWS_MAX`] windows late (then it is sent regardless of the
//! buckets, under the relay's own line; counted held). bsv-low #1017: a NEW kind arriving behind a held state sends the
//! seat's set at once, and the gate serves its per-seat hold on `/health.push.gate`. No Durable Object: the state lives in the isolate, because the push is a wake-up the client
//! answers by re-reading `/owed` or `/results` (a lost window costs latency, never a fact), and an actor call per
//! push would put a subrequest and an actor's single thread in front of every judgement for the sake of a trigger.
//!
//! A relay answer `429 ERR_SEND_BUDGET`, read by its code ([`answer_of`]), is counted apart
//! (`push_refused_budget_total`) and the state is tried again at the next window, at most
//! [`PUSH_BUDGET_RETRY_WINDOWS`] times, then counted `push_given_up_total` and logged with the identity's prefix and
//! the kind. Any other refusal is today's: failed, logged by the caller's path (the pot hook's park re-drives it).
//! Stated: the relay's own window opens at the next minute (`retryAt`), so three retries 2 s apart inside one spent
//! relay minute give up; the pot event's park and drain still re-drive it, the owed list's other triggers (the tip,
//! the socket's return) re-read it.
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// The window, in milliseconds (the var's default).
pub const PUSH_COALESCE_MS_DEFAULT: u64 = 2_000;
/// The window's var: a decimal integer of milliseconds, 0 to [`PUSH_COALESCE_MS_MAX`]; 0 sends every push at once
/// (the buckets still hold what they cannot pay for).
pub const PUSH_COALESCE_MS_VAR: &str = "PUSH_COALESCE_MS";
pub const PUSH_COALESCE_MS_MAX: u64 = 10_000;
/// The sends one identity is pushed in any 60 s (the var's default): 40 of the relay's 100.
pub const PUSH_BUDGET_PER_MIN_DEFAULT: u64 = 40;
pub const PUSH_BUDGET_PER_MIN_VAR: &str = "PUSH_BUDGET_PER_MIN";
/// The sends this isolate makes in any 60 s over every identity (the var's default): 20 under the relay's 100.
pub const PUSH_SENDER_BUDGET_PER_MIN_DEFAULT: u64 = 80;
pub const PUSH_SENDER_BUDGET_PER_MIN_VAR: &str = "PUSH_SENDER_BUDGET_PER_MIN";
/// The relay's send window (relay 0.8.2 `send_budget::SENDS`): both budgets are clamped under it.
pub const RELAY_SENDS_PER_MIN: u64 = 100;
/// A refused `ERR_SEND_BUDGET` state is tried again at the next window this many times, then given up.
pub const PUSH_BUDGET_RETRY_WINDOWS: u32 = 3;
/// bsv-low #1017 (L56 S3, the captain's ruling): THE HOLD BAR. A held state waits at most this many windows (4 s at
/// the default window) for the buckets, then is sent regardless of them, so a seat's first-party event is never held
/// behind the fleet's. It was 10 (20 s, L56 read 18 to 26 s). The one line it does not cross is the RELAY's own minute
/// ([`relay_line`]): a state forced past the buckets goes only while the trailing 60 s holds fewer sends than that
/// line, else it is held one more window (a relay refusal would cost it three retries and a give-up).
pub const PUSH_HOLD_WINDOWS_MAX: u32 = 2;
/// The absolute bound: past this many windows a state is sent regardless of the relay's line too (never held for
/// good; inside the platform's 30 s `wait_until` at the default window). Reached only while the relay's minute is
/// full, which `/health.push.gate` shows as a hold past its bar.
pub const PUSH_HOLD_WINDOWS_RELAY_FULL: u32 = 10;
/// bsv-low #1017: the sends kept under the relay's [`RELAY_SENDS_PER_MIN`] in any trailing 60 s by a state sent past
/// the buckets (the relay's window is tumbling; a trailing count under its line keeps every tumbling one under it).
pub const RELAY_HEADROOM: u64 = 5;
/// bsv-low #1017: the per-seat hold bar `/health.push.gate` reads its p95 against (the fleet invariants row's).
pub const PUSH_HOLD_BAR_MS: u64 = 5_000;
/// The trailing span the hold figures and the relay line are read over.
pub const PUSH_TRAIL_MS: u64 = 60_000;

/// The sends any trailing 60 s may carry through this coalescer: the relay's 100 less [`RELAY_HEADROOM`].
pub const fn relay_line() -> u64 {
    RELAY_SENDS_PER_MIN - RELAY_HEADROOM
}

pub const COUNTER_COALESCED: &str = "push_coalesced_total";
pub const COUNTER_HELD: &str = "push_held_total";
pub const COUNTER_REFUSED_BUDGET: &str = "push_refused_budget_total";
pub const COUNTER_GIVEN_UP: &str = "push_given_up_total";
pub const COUNTER_WINDOWS: &str = "push_windows_total";

/// The durable names (seeded by the overlay, `applayer_counters::PUSH`, pinned equal there).
pub fn counter_names() -> Vec<String> {
    [
        COUNTER_COALESCED,
        COUNTER_HELD,
        COUNTER_REFUSED_BUDGET,
        COUNTER_GIVEN_UP,
        COUNTER_WINDOWS,
    ]
    .iter()
    .map(|n| n.to_string())
    .collect()
}

/// The knobs in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Knobs {
    pub window_ms: u64,
    pub per_identity_per_min: u64,
    pub sender_per_min: u64,
}

impl Default for Knobs {
    fn default() -> Self {
        Self {
            window_ms: PUSH_COALESCE_MS_DEFAULT,
            per_identity_per_min: PUSH_BUDGET_PER_MIN_DEFAULT,
            sender_per_min: PUSH_SENDER_BUDGET_PER_MIN_DEFAULT,
        }
    }
}

impl Knobs {
    /// PURE: the knobs from the three vars as set (`None` unset). Anything but decimal digits is the default; the
    /// window is clamped to [`PUSH_COALESCE_MS_MAX`], each budget to 1 ..= 99 (under the relay's 100).
    pub fn parse(window: Option<&str>, per_identity: Option<&str>, sender: Option<&str>) -> Self {
        let num = |v: Option<&str>| v.and_then(|s| s.trim().parse::<u64>().ok());
        let budget =
            |v: Option<&str>, d: u64| num(v).unwrap_or(d).clamp(1, RELAY_SENDS_PER_MIN - 1);
        Self {
            window_ms: num(window)
                .unwrap_or(PUSH_COALESCE_MS_DEFAULT)
                .min(PUSH_COALESCE_MS_MAX),
            per_identity_per_min: budget(per_identity, PUSH_BUDGET_PER_MIN_DEFAULT),
            sender_per_min: budget(sender, PUSH_SENDER_BUDGET_PER_MIN_DEFAULT),
        }
    }

    pub fn from_env(env: &worker::Env) -> Self {
        let var = |n: &str| env.var(n).ok().map(|v| v.to_string());
        Self::parse(
            var(PUSH_COALESCE_MS_VAR).as_deref(),
            var(PUSH_BUDGET_PER_MIN_VAR).as_deref(),
            var(PUSH_SENDER_BUDGET_PER_MIN_VAR).as_deref(),
        )
    }
}

/// PURE: the coalescing key of a body, `None` for a kind that is not a trigger (it passes through, uncoalesced).
/// The triggers, each read by the client as "re-read served state" (bsv-low `app/src/lib/lowEvents.ts`):
/// `owed-changed` (one key per identity: `OwedGames.tsx` and `useOwedBadge.ts` re-read `/owed`,
/// `useIdentityEvents.ts` refreshes names) and `pot` (one key per pot outpoint, the client filters by outpoint:
/// `engine/orchestrator.ts` re-reads the refund-credit and case legs, the entry is a carrier, never credited).
pub fn coalesce_key(body: &Value) -> Option<String> {
    match body.get("kind").and_then(Value::as_str)? {
        "owed-changed" => Some("owed-changed".to_string()),
        "pot" => {
            let po = body.get("potOutpoint")?;
            let txid = po.get("txid")?.as_str()?.to_ascii_lowercase();
            let vout = po.get("vout")?.as_u64()?;
            Some(format!("pot|{txid}:{vout}"))
        }
        _ => None,
    }
}

/// What one relay `/push` call came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Sent,
    /// `429 ERR_SEND_BUDGET`, read by name.
    BudgetRefused,
    Failed,
    NotConfigured,
}

/// PURE: a relay `/push` answer. 2xx and `400 ERR_DUPLICATE_MESSAGE` delivered (E649 L4 N2); `429` whose `code` is
/// `ERR_SEND_BUDGET` the budget's refusal; anything else failed.
pub fn answer_of(status: u16, body: &str) -> Answer {
    if crate::internal_events::push_answer(status, body) == crate::pot_hook::Delivery::Sent {
        return Answer::Sent;
    }
    let code = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("code").and_then(Value::as_str).map(str::to_string));
    if status == 429 && code.as_deref() == Some("ERR_SEND_BUDGET") {
        Answer::BudgetRefused
    } else {
        Answer::Failed
    }
}

impl Answer {
    pub fn delivery(self) -> crate::pot_hook::Delivery {
        use crate::pot_hook::Delivery;
        match self {
            Answer::Sent => Delivery::Sent,
            Answer::NotConfigured => Delivery::NotConfigured,
            Answer::BudgetRefused | Answer::Failed => Delivery::Failed,
        }
    }
}

/// The relay side and the clock.
#[allow(async_fn_in_trait)]
pub trait PushPort {
    fn now_ms(&self) -> u64;
    async fn sleep_ms(&self, ms: u64);
    async fn send(&self, recipient: &str, message_id: Option<&str>, body: Value) -> Answer;
    fn log(&self, line: &str);
}

/// A token bucket whose any 60 s spends at most `per_min`: a quarter held, the rest refilled evenly.
#[derive(Debug, Clone)]
struct Bucket {
    tokens: f64,
    at_ms: u64,
}

impl Bucket {
    fn capacity(per_min: u64) -> f64 {
        ((per_min / 4).max(1)) as f64
    }
    fn rate_per_ms(per_min: u64) -> f64 {
        (per_min as f64 - Self::capacity(per_min)).max(0.0) / 60_000.0
    }
    fn full(per_min: u64, now: u64) -> Self {
        Self {
            tokens: Self::capacity(per_min),
            at_ms: now,
        }
    }
    fn refill(&mut self, per_min: u64, now: u64) {
        let dt = now.saturating_sub(self.at_ms) as f64;
        self.tokens = (self.tokens + dt * Self::rate_per_ms(per_min)).min(Self::capacity(per_min));
        self.at_ms = now.max(self.at_ms);
    }
}

#[derive(Debug, Clone)]
struct Pending {
    seq: u64,
    /// bsv-low #1002: the state's place in the sender bucket's queue, taken when its key first waited and kept when a
    /// newer state of the key replaces it (coalescing never sends a seat to the back).
    ticket: u64,
    /// bsv-low #1017: when the oldest push this state carries arrived (kept when a newer state replaces it): its hold
    /// is measured from here.
    since: u64,
    /// bsv-low #1017: the buckets refused this key's state at least one window end (it is HELD, not coalescing).
    held: bool,
    body: Value,
    message_id: Option<String>,
}

#[derive(Debug, Default)]
struct Seat {
    pending: BTreeMap<String, Pending>,
    /// The end of the open window (a push before it coalesces).
    window_end: u64,
    bucket: Option<Bucket>,
}

/// One isolate's coalescer.
#[derive(Debug, Default)]
pub struct Coalescer {
    seats: BTreeMap<String, Seat>,
    sender: Option<Bucket>,
    seq: u64,
    ticket: u64,
    /// bsv-low #1017: every relay call this coalescer made in the trailing 60 s (the relay line's count).
    sent: std::collections::VecDeque<u64>,
    /// bsv-low #1017: each trigger state's hold (its send less its `since`; a leading edge 0) in the trailing 60 s,
    /// `(sent at, hold ms)`.
    holds: std::collections::VecDeque<(u64, u64)>,
    /// This coalescer's own counts (the isolate's are the statics; the pins read these).
    pub tally: Tally,
}

/// The hold figures over a trailing minute, as `/health.push` serves them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HoldFigures {
    pub samples: u64,
    pub hold_ms_p50: u64,
    pub hold_ms_p95: u64,
    pub hold_ms_max: u64,
}

impl Coalescer {
    fn prune(&mut self, now: u64) {
        let from = now.saturating_sub(PUSH_TRAIL_MS);
        while self.sent.front().is_some_and(|t| *t <= from) {
            self.sent.pop_front();
        }
        while self.holds.front().is_some_and(|(t, _)| *t <= from) {
            self.holds.pop_front();
        }
    }
    /// The relay's line has room for one more send now.
    fn relay_room(&mut self, now: u64) -> bool {
        self.prune(now);
        (self.sent.len() as u64) < relay_line()
    }
    fn note_send(&mut self, now: u64, hold: Option<u64>) {
        self.prune(now);
        self.sent.push_back(now);
        if let Some(h) = hold {
            self.holds.push_back((now, h));
        }
    }
    /// PURE over the trailing minute at `now`: the per-seat hold's p50, p95 and max (nearest rank).
    pub fn hold_figures(&mut self, now: u64) -> HoldFigures {
        self.prune(now);
        let mut h: Vec<u64> = self.holds.iter().map(|(_, h)| *h).collect();
        h.sort_unstable();
        let rank = |q: f64| -> u64 {
            if h.is_empty() {
                return 0;
            }
            let i = ((q * h.len() as f64).ceil() as usize).clamp(1, h.len()) - 1;
            h[i]
        };
        HoldFigures {
            samples: h.len() as u64,
            hold_ms_p50: rank(0.50),
            hold_ms_p95: rank(0.95),
            hold_ms_max: h.last().copied().unwrap_or(0),
        }
    }
}

/// The five counts, as `/health/invariants.push` serves them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Tally {
    pub coalesced: u64,
    pub held: u64,
    pub refused_budget: u64,
    pub given_up: u64,
    pub windows: u64,
}

#[derive(Clone, Copy)]
enum Which {
    Coalesced,
    Held,
    RefusedBudget,
    GivenUp,
    Window,
}

/// Count one: this coalescer's tally, the isolate's, and the durable delta (the courier census's flush).
fn note(cell: &RefCell<Coalescer>, which: Which) {
    let mut c = cell.borrow_mut();
    let (slot, stat, name) = match which {
        Which::Coalesced => (&mut c.tally.coalesced, &COALESCED, COUNTER_COALESCED),
        Which::Held => (&mut c.tally.held, &HELD, COUNTER_HELD),
        Which::RefusedBudget => (
            &mut c.tally.refused_budget,
            &REFUSED_BUDGET,
            COUNTER_REFUSED_BUDGET,
        ),
        Which::GivenUp => (&mut c.tally.given_up, &GIVEN_UP, COUNTER_GIVEN_UP),
        Which::Window => (&mut c.tally.windows, &WINDOWS, COUNTER_WINDOWS),
    };
    *slot += 1;
    stat.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence(name);
}

thread_local! {
    /// The isolate's coalescer, leaked once so a push's future can hold it across its awaits.
    static ISOLATE: &'static RefCell<Coalescer> = Box::leak(Box::new(RefCell::new(Coalescer::default())));
}

static COALESCED: AtomicU64 = AtomicU64::new(0);
static HELD: AtomicU64 = AtomicU64::new(0);
static REFUSED_BUDGET: AtomicU64 = AtomicU64::new(0);
static GIVEN_UP: AtomicU64 = AtomicU64::new(0);
static WINDOWS: AtomicU64 = AtomicU64::new(0);

/// The isolate's tally (the durable totals are the overlay's `/health/invariants.push`).
pub fn health_json() -> Value {
    json!({
        "countersScope": "isolate",
        "coalesced": COALESCED.load(Ordering::Relaxed),
        "held": HELD.load(Ordering::Relaxed),
        "refusedBudget": REFUSED_BUDGET.load(Ordering::Relaxed),
        "givenUp": GIVEN_UP.load(Ordering::Relaxed),
        "windows": WINDOWS.load(Ordering::Relaxed),
        // bsv-low #1017: this isolate's per-seat hold over the trailing minute (the gate's is `push.gate`).
        "hold": ISOLATE.with(|c| c.borrow_mut().hold_figures(now_ms_isolate())),
    })
}

fn now_ms_isolate() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        worker::Date::now().as_millis()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// bsv-low #1017 (FIX-1017-FOLD: the land tier's `health-dirty-snapshot`): the gate's hold costs `/health` NO D1
/// read. The figures live where the pushes are, in the gate object's own isolate coalescer; the gate answers them on
/// [`PUSH_GATE_HOLD_PATH`] ([`gate_hold_answer`]) and a fetch isolate's `/health` asks it at most once every
/// [`GATE_HOLD_MEMO_MS`] ([`gate_hold_served`], its [`HoldMemo`]), bounded at [`GATE_HOLD_READ_WALL_MS`]; a read that
/// faults serves the last answer with its age, or `null` with none. Stated: the figures are the gate isolate's, so a
/// gate the platform evicted answers no samples until it pushes again (`overBar` `null`).
pub const PUSH_GATE_HOLD_PATH: &str = "/push-gate/hold";
/// A fetch isolate asks the gate for its hold at most this often.
pub const GATE_HOLD_MEMO_MS: u64 = 10_000;
/// The gate's answer is awaited at most this long by `/health`.
pub const GATE_HOLD_READ_WALL_MS: u64 = 500;

/// PURE: the snapshot's cursor for the figures at `now`.
pub fn gate_hold_cursor(f: &HoldFigures, now: u64) -> String {
    json!({
        "asOfMs": now,
        "samples": f.samples,
        "holdMsP50": f.hold_ms_p50,
        "holdMsP95": f.hold_ms_p95,
        "holdMsMax": f.hold_ms_max,
    })
    .to_string()
}

/// PURE: `/health.push.gate` from the gate's answer (`None`: no gate answered, ever, in this isolate): the hold's p50,
/// p95 and max over the minute before `asOfMs`, its samples, the bar and whether the p95 is over it (`null` with no
/// sample), the snapshot's age at `now`.
pub fn gate_hold_view(cursor: Option<&str>, now: u64) -> Value {
    let Some(v) = cursor.and_then(|c| serde_json::from_str::<Value>(c).ok()) else {
        return Value::Null;
    };
    let n = |k: &str| v[k].as_u64().or_else(|| v[k].as_f64().map(|f| f as u64));
    let samples = n("samples").unwrap_or(0);
    let p95 = n("holdMsP95");
    let as_of = n("asOfMs");
    json!({
        "holdMsP50": n("holdMsP50"),
        "holdMsP95": p95,
        "holdMsMax": n("holdMsMax"),
        "samples": samples,
        "trailMs": PUSH_TRAIL_MS,
        "barMs": PUSH_HOLD_BAR_MS,
        "overBar": if samples == 0 { Value::Null } else { json!(p95.unwrap_or(0) > PUSH_HOLD_BAR_MS) },
        "holdWindowsMax": PUSH_HOLD_WINDOWS_MAX,
        "relayLine": relay_line(),
        "asOfMs": as_of,
        "ageMs": as_of.map(|a| now.saturating_sub(a)),
    })
}

/// The gate's answer at `now`: its figures over the trailing minute, as [`gate_hold_cursor`] writes them.
pub fn gate_hold_answer(cell: &RefCell<Coalescer>, now: u64) -> String {
    let f = cell.borrow_mut().hold_figures(now);
    gate_hold_cursor(&f, now)
}

/// A fetch isolate's memo of the gate's last answer.
#[derive(Debug, Default)]
pub struct HoldMemo {
    /// The answer and when this isolate read it.
    pub answer: Option<(String, u64)>,
    /// When this isolate last asked the gate (answered or not).
    pub asked_at: Option<u64>,
}

/// Where `/health` reads the gate's hold: the gate itself (an object fetch), never D1.
#[allow(async_fn_in_trait)]
pub trait HoldSource {
    /// The gate hosted in this isolate: its answer at once, no fetch.
    fn hosted(&self, now: u64) -> Option<String>;
    /// The gate's answer over its object, `None` on a fault, no binding or past the wall.
    async fn ask(&self) -> Option<String>;
}

/// `/health.push.gate`: the hosted gate's figures, else the memo while it is younger than [`GATE_HOLD_MEMO_MS`],
/// else one ask of the gate (a fault keeps the last answer, served with its age).
pub async fn gate_hold_served<S: HoldSource>(memo: &RefCell<HoldMemo>, src: &S, now: u64) -> Value {
    if let Some(answer) = src.hosted(now) {
        return gate_hold_view(Some(&answer), now);
    }
    let due = memo
        .borrow()
        .asked_at
        .is_none_or(|at| now >= at.saturating_add(GATE_HOLD_MEMO_MS));
    if due {
        memo.borrow_mut().asked_at = Some(now);
        if let Some(answer) = src.ask().await {
            memo.borrow_mut().answer = Some((answer, now));
        }
    }
    let m = memo.borrow();
    gate_hold_view(m.answer.as_ref().map(|(a, _)| a.as_str()), now)
}

thread_local! {
    /// The isolate's memo, leaked once so `/health`'s future can hold it across the ask.
    static GATE_HOLD_MEMO: &'static RefCell<HoldMemo> = Box::leak(Box::new(RefCell::new(HoldMemo::default())));
}

/// The worker's [`HoldSource`]: the isolate's own cell when it hosts the gate, else the `BOARD_VIEW` object named
/// [`PUSH_GATE_ACTOR`] at [`PUSH_GATE_HOLD_PATH`].
pub struct WorkerHoldSource<'a> {
    pub env: &'a worker::Env,
}

impl HoldSource for WorkerHoldSource<'_> {
    fn hosted(&self, now: u64) -> Option<String> {
        GATE_HOSTED
            .with(|h| h.get())
            .then(|| ISOLATE.with(|c| gate_hold_answer(c, now)))
    }
    async fn ask(&self) -> Option<String> {
        let ns = self.env.durable_object("BOARD_VIEW").ok()?;
        let stub = ns
            .id_from_name(PUSH_GATE_ACTOR)
            .and_then(|id| id.get_stub())
            .ok()?;
        let read = async move {
            let mut resp = stub
                .fetch_with_str(&format!("https://board-view{PUSH_GATE_HOLD_PATH}"))
                .await
                .ok()?;
            if resp.status_code() != 200 {
                return None;
            }
            resp.text().await.ok()
        };
        let wall = worker::Delay::from(core::time::Duration::from_millis(GATE_HOLD_READ_WALL_MS));
        futures_util::pin_mut!(read, wall);
        match futures_util::future::select(read, wall).await {
            futures_util::future::Either::Left((answer, _)) => answer,
            futures_util::future::Either::Right(_) => None,
        }
    }
}

/// `/health.push.gate` in the worker (no D1 read: [`gate_hold_served`]).
pub async fn gate_hold_health(env: &worker::Env) -> Value {
    let src = WorkerHoldSource { env };
    let now = worker::Date::now().as_millis();
    // The memo is the isolate's; its borrow is never held across the ask.
    let memo: &'static RefCell<HoldMemo> = GATE_HOLD_MEMO.with(|m| *m);
    gate_hold_served(memo, &src, now).await
}

/// The gate's `/push-gate/hold` request: its figures from the isolate's coalescer.
pub fn serve_gate_hold() -> worker::Result<worker::Response> {
    let answer = ISOLATE.with(|c| gate_hold_answer(c, now_ms_isolate()));
    let mut resp = worker::Response::ok(answer)?;
    resp.headers_mut().set("content-type", "application/json")?;
    Ok(resp)
}

/// bsv-low #1002: the states queued AHEAD of `ticket` (`None`: a state not yet queued, behind every queued one) that
/// the sender bucket must still cover: older, still waited on (their seat's window was set within the last window
/// and a second: a submitter whose future was dropped leaves its entry, which stops counting), and payable by their
/// own seat's bucket (a seat over its own minute holds no one else back). The caller's own entry is never counted.
fn ahead(
    c: &Coalescer,
    identity: &str,
    key: Option<&str>,
    ticket: Option<u64>,
    knobs: &Knobs,
    now: u64,
) -> usize {
    let live_after = now.saturating_sub(knobs.window_ms + 1_000);
    c.seats
        .iter()
        .filter(|(_, s)| s.window_end >= live_after)
        .filter(|(id, s)| {
            s.bucket.as_ref().is_none_or(|b| {
                let mut b = b.clone();
                b.refill(knobs.per_identity_per_min, now);
                b.tokens >= 1.0
            }) || id.as_str() == identity
        })
        .flat_map(|(id, s)| s.pending.iter().map(move |(k, p)| (id, k, p)))
        .filter(|(id, k, p)| {
            !(id.as_str() == identity && Some(k.as_str()) == key)
                && ticket.is_none_or(|t| p.ticket < t)
        })
        .count()
}

/// Take one token of both buckets, or none (the state waits). bsv-low #1002: the sender bucket is served FIRST COME,
/// FIRST SERVED across seats: a state pays only when the bucket also covers every live state queued ahead of it
/// ([`ahead`]). Until L55 the token went to whichever waiter woke first after a refill (or to a quiet seat's leading
/// edge, which jumped every held state), so under a saturated sender minute one seat of a pot took a send every
/// window while the other lost each race to its hold bound (L55 pair 10: p22 six `/push` calls 22:50:46Z to
/// 22:50:58Z, p21 its first at 22:51:15.9Z).
fn take(
    c: &mut Coalescer,
    identity: &str,
    key: Option<&str>,
    ticket: Option<u64>,
    knobs: &Knobs,
    now: u64,
) -> bool {
    // bsv-low #1017: nothing paid goes past the relay's line either.
    if !c.relay_room(now) {
        return false;
    }
    let queued = ahead(c, identity, key, ticket, knobs, now) as f64;
    let sender = c
        .sender
        .get_or_insert_with(|| Bucket::full(knobs.sender_per_min, now));
    sender.refill(knobs.sender_per_min, now);
    let seat = c.seats.entry(identity.to_string()).or_default();
    let mine = seat
        .bucket
        .get_or_insert_with(|| Bucket::full(knobs.per_identity_per_min, now));
    mine.refill(knobs.per_identity_per_min, now);
    if sender.tokens >= 1.0 + queued && mine.tokens >= 1.0 {
        sender.tokens -= 1.0;
        mine.tokens -= 1.0;
        true
    } else {
        false
    }
}

/// bsv-low #1017: the state of `key` has waited [`PUSH_HOLD_WINDOWS_MAX`] windows since the oldest push it carries
/// (4 s at the default window): its hold ends.
fn held_past_bar(c: &Coalescer, identity: &str, key: &str, knobs: &Knobs, now: u64) -> bool {
    c.seats
        .get(identity)
        .and_then(|s| s.pending.get(key))
        .is_some_and(|p| {
            now.saturating_sub(p.since) >= u64::from(PUSH_HOLD_WINDOWS_MAX) * knobs.window_ms.max(1)
        })
}

/// Past the hold bound a state is sent regardless: it still spends a token of both buckets (they may go below
/// zero), so what follows waits for it.
fn force(c: &mut Coalescer, identity: &str) -> bool {
    if let Some(b) = c.sender.as_mut() {
        b.tokens -= 1.0;
    }
    if let Some(b) = c.seats.get_mut(identity).and_then(|s| s.bucket.as_mut()) {
        b.tokens -= 1.0;
    }
    true
}

/// Forget a seat that holds nothing and whose window and bucket have run out (the map stays the size of the hot set).
fn tidy(c: &mut Coalescer, identity: &str, knobs: &Knobs, now: u64) {
    let idle = c.seats.get(identity).is_some_and(|s| {
        s.pending.is_empty()
            && s.window_end <= now
            && s.bucket.as_ref().is_none_or(|b| {
                let mut b = b.clone();
                b.refill(knobs.per_identity_per_min, now);
                b.tokens >= Bucket::capacity(knobs.per_identity_per_min)
            })
    });
    if idle {
        c.seats.remove(identity);
    }
}

fn prefix(identity: &str) -> &str {
    &identity[..12.min(identity.len())]
}

/// ONE push to a seat through the coalescer `cell` under `knobs`. Answers what the push came to: `Sent` for a state
/// a newer state of its key carried (coalesced), else the relay's answer for the call that carried it.
pub async fn submit<P: PushPort>(
    cell: &RefCell<Coalescer>,
    port: &P,
    knobs: &Knobs,
    recipient: &str,
    message_id: Option<&str>,
    body: Value,
) -> Answer {
    let identity = recipient.to_ascii_lowercase();
    let Some(key) = coalesce_key(&body) else {
        // Not a trigger: passes through, uncoalesced and unheld (counted on the relay's line).
        cell.borrow_mut().note_send(port.now_ms(), None);
        let a = port.send(&identity, message_id, body).await;
        if a == Answer::BudgetRefused {
            note(cell, Which::RefusedBudget);
        }
        return a;
    };
    let kind = body
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("-")
        .to_string();
    let now = port.now_ms();
    // The leading edge: a quiet identity (no open window, nothing pending of this key) with a token in both buckets
    // goes at once, and opens the window.
    let lead = {
        let mut c = cell.borrow_mut();
        let quiet = c
            .seats
            .get(&identity)
            .is_none_or(|s| s.window_end <= now && !s.pending.contains_key(&key));
        if quiet && take(&mut c, &identity, None, None, knobs, now) {
            c.seats.entry(identity.clone()).or_default().window_end = now + knobs.window_ms;
            c.note_send(now, Some(0));
            true
        } else {
            false
        }
    };
    let mut refusals = 0u32;
    if lead {
        match port.send(&identity, message_id, body.clone()).await {
            Answer::BudgetRefused => {
                note(cell, Which::RefusedBudget);
                refusals = 1;
            }
            a => return a,
        }
    }
    // Pending: the newest state of the key wins.
    let (seq, ticket, replaced, new_kind) = {
        let mut c = cell.borrow_mut();
        c.seq += 1;
        let seq = c.seq;
        c.ticket += 1;
        let fresh = c.ticket;
        let now = port.now_ms();
        let seat = c.seats.entry(identity.clone()).or_default();
        let ticket = seat.pending.get(&key).map_or(fresh, |p| p.ticket);
        let since = seat.pending.get(&key).map_or(now, |p| p.since);
        let held = seat.pending.get(&key).is_some_and(|p| p.held);
        // bsv-low #1017: a NEW kind behind a HELD state (a `pot` behind a held `owed-changed`, another pot's event)
        // ends the seat's hold: this submitter sends the seat's set below, at once. A state still in its first
        // window is coalescing, not held: a new kind beside it waits for that window's end with it.
        let new_kind = !seat.pending.contains_key(&key) && seat.pending.values().any(|p| p.held);
        let replaced = seat
            .pending
            .insert(
                key.clone(),
                Pending {
                    seq,
                    ticket,
                    since,
                    held,
                    body,
                    message_id: message_id.map(str::to_string),
                },
            )
            .is_some();
        // bsv-low #1002: a state that REPLACES a waiting one keeps the window its waiter is on (an ended one is
        // taken at once). Until L55 a push landing at or after the window's end opened a new one, the waiter woke
        // superseded, and a seat pushed about once a window (an owed actor's event every 2 s) was postponed for as
        // long as its pushes kept coming.
        if !replaced && seat.window_end <= now {
            seat.window_end = now + knobs.window_ms.max(1);
        }
        (seq, ticket, replaced, new_kind)
    };
    if replaced {
        note(cell, Which::Coalesced);
    }
    if new_kind {
        if let Some(a) = release_seat(cell, port, knobs, &identity, &key, seq).await {
            return a;
        }
    }
    let mut held_windows = 0u32;
    loop {
        // bsv-low #1017: the waiter wakes at its seat's window end, or at its state's hold bar if sooner (the bar is
        // measured from the oldest push the state carries, not on the window grid).
        let end = cell
            .borrow()
            .seats
            .get(&identity)
            .map(|s| {
                let bar = s
                    .pending
                    .get(&key)
                    .filter(|p| p.seq == seq)
                    .map_or(u64::MAX, |p| {
                        p.since + u64::from(PUSH_HOLD_WINDOWS_MAX) * knobs.window_ms.max(1)
                    });
                s.window_end.min(bar)
            })
            .unwrap_or(0);
        let now = port.now_ms();
        if end > now {
            port.sleep_ms(end - now).await;
        }
        let now = port.now_ms();
        let step = {
            let mut c = cell.borrow_mut();
            let mine = c
                .seats
                .get(&identity)
                .and_then(|s| s.pending.get(&key))
                .is_some_and(|p| p.seq == seq);
            if !mine {
                // Superseded: a newer state of the key took it over (and its submitter delivers it).
                Step::Carried
            } else if take(&mut c, &identity, Some(&key), Some(ticket), knobs, now)
                || (held_past_bar(&c, &identity, &key, knobs, now)
                    && (c.relay_room(now) || held_windows >= PUSH_HOLD_WINDOWS_RELAY_FULL)
                    && force(&mut c, &identity))
            {
                let seat = c.seats.entry(identity.clone()).or_default();
                seat.window_end = now + knobs.window_ms;
                match seat.pending.remove(&key) {
                    Some(p) => {
                        c.note_send(now, Some(now.saturating_sub(p.since)));
                        Step::Send(p)
                    }
                    None => Step::Carried,
                }
            } else {
                // HELD to the next window: never dropped.
                let seat = c.seats.entry(identity.clone()).or_default();
                seat.window_end = now + knobs.window_ms.max(1);
                if let Some(p) = seat.pending.get_mut(&key) {
                    p.held = true;
                }
                Step::Held
            }
        };
        let p = match step {
            Step::Carried => {
                tidy(&mut cell.borrow_mut(), &identity, knobs, port.now_ms());
                return Answer::Sent;
            }
            Step::Held => {
                held_windows += 1;
                note(cell, Which::Held);
                continue;
            }
            Step::Send(p) => p,
        };
        note(cell, Which::Window);
        let a = port
            .send(&identity, p.message_id.as_deref(), p.body.clone())
            .await;
        if a != Answer::BudgetRefused {
            tidy(&mut cell.borrow_mut(), &identity, knobs, port.now_ms());
            return a;
        }
        note(cell, Which::RefusedBudget);
        refusals += 1;
        if refusals > PUSH_BUDGET_RETRY_WINDOWS {
            note(cell, Which::GivenUp);
            port.log(&format!(
                "[push] GIVEN UP {}... kind={kind} after {refusals} ERR_SEND_BUDGET refusals",
                prefix(&identity)
            ));
            return Answer::BudgetRefused;
        }
        // Tried again at the next window, unless a newer state of the key arrived meanwhile (it goes instead).
        let mut c = cell.borrow_mut();
        let seat = c.seats.entry(identity.clone()).or_default();
        if seat.pending.contains_key(&key) {
            return Answer::Sent;
        }
        seat.pending.insert(key.clone(), Pending { seq, ..p });
        seat.window_end = port.now_ms() + knobs.window_ms.max(1);
    }
}

/// bsv-low #1017: a NEW kind arrived at a seat holding states: the seat's whole pending set goes now, past the
/// buckets (each still spends a token, so what follows waits for it), while the relay's line has room; a state the
/// line cannot carry stays pending for its own waiter. The submitter of the new kind answers with its own state's
/// relay answer (`None`: its state stayed pending or was refused, it waits as any). A refused state goes back to its
/// waiter's retries. The released states' waiters find their state
/// gone and answer delivered (carried), as a coalesced state does.
async fn release_seat<P: PushPort>(
    cell: &RefCell<Coalescer>,
    port: &P,
    knobs: &Knobs,
    identity: &str,
    key: &str,
    seq: u64,
) -> Option<Answer> {
    let now = port.now_ms();
    let batch: Vec<(String, Pending)> = {
        let mut c = cell.borrow_mut();
        let keys: Vec<String> = c
            .seats
            .get(identity)
            .map(|s| s.pending.keys().cloned().collect())
            .unwrap_or_default();
        let mut batch = Vec::new();
        for k in keys {
            if !c.relay_room(now) {
                break;
            }
            force(&mut c, identity);
            let seat = c.seats.entry(identity.to_string()).or_default();
            if let Some(p) = seat.pending.remove(&k) {
                seat.window_end = now + knobs.window_ms;
                c.note_send(now, Some(now.saturating_sub(p.since)));
                batch.push((k, p));
            }
        }
        batch
    };
    let mut mine = None;
    for (k, p) in batch {
        note(cell, Which::Window);
        let a = port
            .send(identity, p.message_id.as_deref(), p.body.clone())
            .await;
        if a == Answer::BudgetRefused {
            // Back to its own waiter (its retries), unless a newer state of its key arrived meanwhile.
            note(cell, Which::RefusedBudget);
            let mut c = cell.borrow_mut();
            let seat = c.seats.entry(identity.to_string()).or_default();
            seat.pending.entry(k).or_insert(p);
            continue;
        }
        if k == key && p.seq == seq {
            mine = Some(a);
        }
    }
    tidy(&mut cell.borrow_mut(), identity, knobs, port.now_ms());
    mine
}

enum Step {
    Carried,
    Held,
    Send(Pending),
}

/// The worker's port: `internal_events::first_party_push_answer` over the `RELAY` binding, `worker::Delay`.
pub struct WorkerPush<'a> {
    pub env: &'a worker::Env,
}

impl PushPort for WorkerPush<'_> {
    fn now_ms(&self) -> u64 {
        worker::Date::now().as_millis()
    }
    async fn sleep_ms(&self, ms: u64) {
        worker::Delay::from(core::time::Duration::from_millis(ms)).await;
    }
    async fn send(&self, recipient: &str, message_id: Option<&str>, body: Value) -> Answer {
        crate::internal_events::first_party_push_answer(self.env, recipient, message_id, body).await
    }
    fn log(&self, line: &str) {
        worker::console_log!("{line}");
    }
}

/// bsv-low #989 (lane FIX-989, L54's S2): the ONE place a first-party push to a seat is submitted to a coalescer.
///
/// L54 read the relay's refusals at 06:55Z: `541 sends, and an identity makes at most 100 in the window`, the count
/// rising across recipients (the SENDER's minute, the app layer's one key), refused in 27 of 40 sampled lines inside
/// an `OwedActor` (18 distinct objects) and the rest in the Worker's fetch isolates. PR 194's coalescer was on that
/// path, but it lives per ISOLATE: every owed actor (one Durable Object per seat, each in an isolate of its own) and
/// every fetch isolate kept its own seat and sender buckets, so each passed its own 40 and 80 and the fleet sent 541.
/// Its stated limit ("two isolates pushing at the same moment can still pass 100") was the whole fleet once the
/// actors pushed. So a push is now FORWARDED to one gate, the `BOARD_VIEW` object named [`PUSH_GATE_ACTOR`], whose
/// isolate's coalescer is the fleet's: one seat bucket a seat and one sender bucket, whatever the producer (the
/// actor's `owed-changed`, the D1 judge's beside it in Phase B, the pot hook's `pot`), so the actor's and the D1
/// judge's pushes of one judgement coalesce under one key. A push in the gate's own isolate (the gate, or another
/// `BoardView` object sharing it: the same cell) is submitted there. With no `BOARD_VIEW` binding the isolate's
/// coalescer stands, as before #989. A gate that faults answers `Failed` for its pushes and never sends around it
/// (the pot hook's park re-drives its event; an `owed-changed` costs the seat its next event's re-read).
pub const PUSH_GATE_ACTOR: &str = "push-gate:v1";
/// The gate's route on its object.
pub const PUSH_GATE_PATH: &str = "/push-gate";
/// The pushes one forward carries (a flush of the isolate's queue is cut into bodies of this many).
pub const PUSH_GATE_BODY_MAX: usize = 256;

/// One push as it rides to the gate.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatePush {
    pub recipient: String,
    #[serde(default)]
    pub message_id: Option<String>,
    pub body: Value,
}

/// What a forward to the gate came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Forwarded {
    /// One answer a push, in order.
    Answers(Vec<Answer>),
    /// No gate is bound: the isolate's coalescer.
    Unbound,
    /// The gate did not answer (or answered something else): every push `Failed`, never sent around it.
    Fault(String),
}

/// Where this isolate's pushes go.
#[allow(async_fn_in_trait)]
pub trait SeatGate {
    /// The gate lives in this isolate: its coalescer is this isolate's cell.
    fn hosted_here(&self) -> bool;
    async fn forward(&self, pushes: Vec<GatePush>) -> Forwarded;
}

impl Answer {
    pub fn word(self) -> &'static str {
        match self {
            Answer::Sent => "sent",
            Answer::BudgetRefused => "budget",
            Answer::Failed => "failed",
            Answer::NotConfigured => "not-configured",
        }
    }
    pub fn of_word(w: &str) -> Option<Self> {
        Some(match w {
            "sent" => Answer::Sent,
            "budget" => Answer::BudgetRefused,
            "failed" => Answer::Failed,
            "not-configured" => Answer::NotConfigured,
            _ => return None,
        })
    }
}

/// The pushes submitted to `cell`, together, one answer each in order.
pub async fn submit_all<P: PushPort>(
    cell: &RefCell<Coalescer>,
    port: &P,
    knobs: &Knobs,
    pushes: Vec<GatePush>,
) -> Vec<Answer> {
    futures_util::future::join_all(pushes.into_iter().map(|p| async move {
        submit(
            cell,
            port,
            knobs,
            &p.recipient,
            p.message_id.as_deref(),
            p.body,
        )
        .await
    }))
    .await
}

/// THE route of a seat's pushes (bsv-low #989): to the gate when it is elsewhere, to `cell` when it is here or
/// unbound. One answer a push, in order.
pub async fn deliver<P: PushPort, G: SeatGate>(
    cell: &RefCell<Coalescer>,
    port: &P,
    gate: &G,
    knobs: &Knobs,
    pushes: Vec<GatePush>,
) -> Vec<Answer> {
    if pushes.is_empty() {
        return Vec::new();
    }
    if gate.hosted_here() {
        return submit_all(cell, port, knobs, pushes).await;
    }
    let n = pushes.len();
    match gate.forward(pushes.clone()).await {
        Forwarded::Answers(a) if a.len() == n => a,
        Forwarded::Answers(a) => {
            port.log(&format!(
                "[push] gate answered {} for {n} pushes; every one failed",
                a.len()
            ));
            vec![Answer::Failed; n]
        }
        Forwarded::Unbound => submit_all(cell, port, knobs, pushes).await,
        Forwarded::Fault(why) => {
            port.log(&format!("[push] gate fault for {n} pushes: {why}"));
            vec![Answer::Failed; n]
        }
    }
}

thread_local! {
    /// This isolate hosts the gate (set by the gate's first request): its pushes are submitted to the isolate's cell.
    static GATE_HOSTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The worker's gate: the `BOARD_VIEW` object named [`PUSH_GATE_ACTOR`].
pub struct WorkerGate<'a> {
    pub env: &'a worker::Env,
}

impl SeatGate for WorkerGate<'_> {
    fn hosted_here(&self) -> bool {
        GATE_HOSTED.with(|h| h.get())
    }
    async fn forward(&self, pushes: Vec<GatePush>) -> Forwarded {
        let Ok(ns) = self.env.durable_object("BOARD_VIEW") else {
            return Forwarded::Unbound;
        };
        let stub = match ns
            .id_from_name(PUSH_GATE_ACTOR)
            .and_then(|id| id.get_stub())
        {
            Ok(s) => s,
            Err(e) => return Forwarded::Fault(e.to_string()),
        };
        let body = json!({ "pushes": pushes }).to_string();
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Post)
            .with_body(Some(body.into()));
        let req = match worker::Request::new_with_init(
            &format!("https://board-view{PUSH_GATE_PATH}"),
            &init,
        ) {
            Ok(r) => r,
            Err(e) => return Forwarded::Fault(e.to_string()),
        };
        let mut resp = match stub.fetch_with_request(req).await {
            Ok(r) => r,
            Err(e) => return Forwarded::Fault(e.to_string()),
        };
        let status = resp.status_code();
        let text = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Forwarded::Fault(format!("HTTP {status}"));
        }
        answers_of(&text).map_or_else(
            || Forwarded::Fault("an answer that does not read".into()),
            Forwarded::Answers,
        )
    }
}

/// PURE: the gate's answer body, `{"answers": ["sent", ...]}`.
pub fn answers_of(text: &str) -> Option<Vec<Answer>> {
    serde_json::from_str::<Value>(text)
        .ok()?
        .get("answers")?
        .as_array()?
        .iter()
        .map(|a| a.as_str().and_then(Answer::of_word))
        .collect()
}

/// THE GATE (bsv-low #989): the `/push-gate` request of the object named [`PUSH_GATE_ACTOR`]. Marks the isolate as
/// the gate's, submits the pushes to the isolate's coalescer and answers when every one came to something.
pub async fn serve_gate(env: &worker::Env, text: &str) -> worker::Result<worker::Response> {
    GATE_HOSTED.with(|h| h.set(true));
    let pushes: Vec<GatePush> = match serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| v.get("pushes").cloned())
        .and_then(|p| serde_json::from_value(p).ok())
    {
        Some(p) => p,
        None => return worker::Response::error("pushes", 400),
    };
    let knobs = Knobs::from_env(env);
    let port = WorkerPush { env };
    let cell: &'static RefCell<Coalescer> = ISOLATE.with(|c| *c);
    let answers = if crate::internal_events::relay_configured(env) {
        submit_all(cell, &port, &knobs, pushes).await
    } else {
        vec![Answer::NotConfigured; pushes.len()]
    };
    worker::Response::from_json(
        &json!({ "answers": answers.iter().map(|a| a.word()).collect::<Vec<_>>() }),
    )
}

/// THE helper every first-party push to a seat calls: through the fleet's gate ([`deliver`]) under the env's knobs.
pub async fn push_to_seat(
    env: &worker::Env,
    recipient: &str,
    message_id: Option<&str>,
    body: Value,
) -> crate::pot_hook::Delivery {
    // FIX-935-FOLD: with no relay there is no send budget to keep and nothing to coalesce. The raw call answers
    // `NotConfigured` at once, as before #935; a window here only delayed what settles on that answer (the detached
    // announce's parks).
    if !crate::internal_events::relay_configured(env) {
        return first_party_push_answer_raw(env, recipient, message_id, body).await;
    }
    let push = GatePush {
        recipient: recipient.to_string(),
        message_id: message_id.map(str::to_string),
        body,
    };
    deliver_env(env, vec![push])
        .await
        .first()
        .copied()
        .unwrap_or(Answer::Failed)
        .delivery()
}

/// The worker's [`deliver`]: the isolate's cell, the relay port, the `BOARD_VIEW` gate.
async fn deliver_env(env: &worker::Env, pushes: Vec<GatePush>) -> Vec<Answer> {
    let knobs = Knobs::from_env(env);
    let port = WorkerPush { env };
    // The cell is the isolate's; `submit` never holds its borrow across an await.
    let cell: &'static RefCell<Coalescer> = ISOLATE.with(|c| *c);
    deliver(cell, &port, &WorkerGate { env }, &knobs, pushes).await
}

async fn first_party_push_answer_raw(
    env: &worker::Env,
    recipient: &str,
    message_id: Option<&str>,
    body: Value,
) -> crate::pot_hook::Delivery {
    crate::internal_events::first_party_push_answer(env, recipient, message_id, body)
        .await
        .delivery()
}

/// The most queued pushes an isolate holds ([`queue_seat_push`]); one key per (identity, kind's coalescing key), so
/// the bound is only met by that many distinct seats between two flushes. Past it the oldest is let go (the seat
/// re-reads on its next event).
pub const PUSH_QUEUE_MAX: usize = 4_096;

thread_local! {
    static QUEUED: RefCell<BTreeMap<(String, String), (u64, Value)>> = const { RefCell::new(BTreeMap::new()) };
}

/// FIX-935-FOLD: a push that nobody waits on (the owed judge's `owed-changed`) is QUEUED in the isolate's memory,
/// never sent in its caller's path, so a coalescing window never sits in a route's answer or a pass's wall. The
/// queue keeps the newest body per (identity, coalescing key) and is sent by [`flush_queued`], which the fetch
/// door, the scheduled tasks and the belt actor hand to `wait_until` after their work. A lost queue (an isolate's
/// end) costs the seat one re-read on its next event.
pub fn queue_seat_push(identity: &str, body: Value) {
    let identity = identity.to_ascii_lowercase();
    let key = coalesce_key(&body).unwrap_or_else(|| format!("raw|{}", body));
    QUEUED.with(|q| {
        let mut q = q.borrow_mut();
        let seq = q.values().map(|(s, _)| *s).max().unwrap_or(0) + 1;
        q.insert((identity, key), (seq, body));
        while q.len() > PUSH_QUEUE_MAX {
            let oldest = q
                .iter()
                .min_by_key(|(_, (s, _))| *s)
                .map(|(k, _)| k.clone());
            match oldest {
                Some(k) => {
                    q.remove(&k);
                }
                None => break,
            }
        }
    });
}

/// Whether the isolate holds queued pushes (read before handing a flush to `wait_until`).
pub fn has_queued() -> bool {
    QUEUED.with(|q| !q.borrow().is_empty())
}

/// Takes the isolate's queued pushes, oldest first.
pub fn take_queued() -> Vec<(String, Value)> {
    let mut taken: Vec<_> = QUEUED
        .with(|q| std::mem::take(&mut *q.borrow_mut()))
        .into_iter()
        .map(|((identity, _), (seq, body))| (seq, identity, body))
        .collect();
    taken.sort_by_key(|(seq, _, _)| *seq);
    taken.into_iter().map(|(_, i, b)| (i, b)).collect()
}

/// Hands the isolate's queued pushes to `wait_until`, after the answer (a no-op with none queued).
pub fn flush_after(ctx: &worker::Context, env: &worker::Env) {
    if has_queued() {
        ctx.wait_until(flush_queued(env.clone()));
    }
}

/// Sends the isolate's queued pushes through the coalescer ([`push_to_seat`]), together. Called after an answer
/// (`wait_until`) or after a pass's walled work, never inside either.
pub async fn flush_queued(env: worker::Env) {
    let pushes = take_queued();
    if pushes.is_empty() {
        return;
    }
    if !crate::internal_events::relay_configured(&env) {
        futures_util::future::join_all(pushes.into_iter().map(|(identity, body)| {
            let env = &env;
            async move {
                first_party_push_answer_raw(env, &identity, None, body).await;
            }
        }))
        .await;
        return;
    }
    // bsv-low #989: through the fleet's gate, a body of at most PUSH_GATE_BODY_MAX pushes a forward.
    let pushes: Vec<GatePush> = pushes
        .into_iter()
        .map(|(recipient, body)| GatePush {
            recipient,
            message_id: None,
            body,
        })
        .collect();
    let env = &env;
    futures_util::future::join_all(
        pushes
            .chunks(PUSH_GATE_BODY_MAX)
            .map(|chunk| deliver_env(env, chunk.to_vec())),
    )
    .await;
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "push_coalesce_tests.rs"]
mod tests;
