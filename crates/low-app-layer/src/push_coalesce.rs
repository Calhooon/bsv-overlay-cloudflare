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
//! state of every key always goes, at most [`PUSH_HOLD_WINDOWS_MAX`] windows late (then it is sent regardless,
//! counted held). No Durable Object: the state lives in the isolate, because the push is a wake-up the client
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
/// A held state waits at most this many windows for a bucket, then is sent regardless (never dropped; inside the
/// platform's 30 s `wait_until` at the default window).
pub const PUSH_HOLD_WINDOWS_MAX: u32 = 10;

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
    /// This coalescer's own counts (the isolate's are the statics; the pins read these).
    pub tally: Tally,
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
    })
}

/// Take one token of both buckets, or none (the state waits).
fn take(c: &mut Coalescer, identity: &str, knobs: &Knobs, now: u64) -> bool {
    let sender = c
        .sender
        .get_or_insert_with(|| Bucket::full(knobs.sender_per_min, now));
    sender.refill(knobs.sender_per_min, now);
    let seat = c.seats.entry(identity.to_string()).or_default();
    let mine = seat
        .bucket
        .get_or_insert_with(|| Bucket::full(knobs.per_identity_per_min, now));
    mine.refill(knobs.per_identity_per_min, now);
    if sender.tokens >= 1.0 && mine.tokens >= 1.0 {
        sender.tokens -= 1.0;
        mine.tokens -= 1.0;
        true
    } else {
        false
    }
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
        // Not a trigger: passes through, uncoalesced and unheld.
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
        if quiet && take(&mut c, &identity, knobs, now) {
            c.seats.entry(identity.clone()).or_default().window_end = now + knobs.window_ms;
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
    let (seq, replaced) = {
        let mut c = cell.borrow_mut();
        c.seq += 1;
        let seq = c.seq;
        let seat = c.seats.entry(identity.clone()).or_default();
        let replaced = seat
            .pending
            .insert(
                key.clone(),
                Pending {
                    seq,
                    body,
                    message_id: message_id.map(str::to_string),
                },
            )
            .is_some();
        if seat.window_end <= now {
            seat.window_end = now + knobs.window_ms.max(1);
        }
        (seq, replaced)
    };
    if replaced {
        note(cell, Which::Coalesced);
    }
    let mut held_windows = 0u32;
    loop {
        let end = cell
            .borrow()
            .seats
            .get(&identity)
            .map(|s| s.window_end)
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
            } else if take(&mut c, &identity, knobs, now)
                || (held_windows >= PUSH_HOLD_WINDOWS_MAX && force(&mut c, &identity))
            {
                let seat = c.seats.entry(identity.clone()).or_default();
                seat.window_end = now + knobs.window_ms;
                match seat.pending.remove(&key) {
                    Some(p) => Step::Send(p),
                    None => Step::Carried,
                }
            } else {
                // HELD to the next window: never dropped.
                let seat = c.seats.entry(identity.clone()).or_default();
                seat.window_end = now + knobs.window_ms.max(1);
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

/// THE helper every first-party push to a seat calls: the isolate's coalescer under the env's knobs.
pub async fn push_to_seat(
    env: &worker::Env,
    recipient: &str,
    message_id: Option<&str>,
    body: Value,
) -> crate::pot_hook::Delivery {
    let knobs = Knobs::from_env(env);
    let port = WorkerPush { env };
    // The cell is the isolate's; `submit` never holds its borrow across an await.
    let cell: &'static RefCell<Coalescer> = ISOLATE.with(|c| *c);
    submit(cell, &port, &knobs, recipient, message_id, body)
        .await
        .delivery()
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "push_coalesce_tests.rs"]
mod tests;
