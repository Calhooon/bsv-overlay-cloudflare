//! bsv-low #585, door 3's fold: THE ORPHAN SWEEP of the queue's R2 objects (`BEEF_BLOBS`).
//!
//! Door 3 deletes an object when the message naming it is acked, when its dead letter is LOST or dropped as the
//! lighter copy, and on the operator's discard (`queue.rs`, the deletion rule). An object NOTHING names stayed for
//! good: a send that faulted after its write and was never re-presented, a delete that faulted, a batch that died
//! between its acks and its deletes, a message the platform dropped without the LOST line, a letter deleted by
//! SQL. Nothing listed them.
//!
//! ## The rule
//!
//! The scheduled tick (`*/15`) runs ONE PASS: it lists at most [`SWEEP_MAX_OBJECTS`] objects under
//! [`SWEEP_PREFIX`] from the key the last pass stopped at (the cursor is the last KEY, at rest in D1,
//! `beef_blob_sweep`: R2 lists in key order, and a key outlives any listing token), and deletes each object that
//! is both
//!
//! 1. OLDER than [`ORPHAN_WINDOW_S`] by its AGE STAMP ([`Listed::age_ms`]): the LATER of R2's `uploaded` and the
//!    writer's own `customMetadata.touched` (`queue::TOUCHED_META`, written by every put; the d3 fold-2, N2 (b):
//!    whether the platform renews `uploaded` on a re-put of a key is not documented, and no longer decides), and
//! 2. named by NO dead letter row (`mutation_dead_letters.r2_key`, whatever the row's status),
//!
//! at most [`SWEEP_MAX_DELETES`] a pass (a pass that meets more stops there and the next goes on from it). Before
//! each delete the object is read again (`head`): one written again since the listing (a client's
//! re-presentation writes the same key and a new `touched` stamp) is left.
//!
//! ## Why the window is what no queue message can outlive
//!
//! An object's names are queue messages and dead letter rows. A row is read (2). A message carries no stamp the
//! sweep can read, so the sweep waits until none can be left: every message naming an object was sent right
//! after a write of it (the door writes, then sends), so no message naming it is younger than the object's
//! `uploaded` stamp allows; the lever's re-drive sends without a write, and its row names the object from the
//! park to the ack. A message lives in the main queue at most that queue's retention, then in the dead letter
//! queue at most that queue's ([`QUEUE_RETENTION_S`] each, the platform's default of four days: neither queue
//! sets another), where it is parked (a row), LOST (its object deleted) or dropped by the platform. Hence two
//! retentions.
//!
//! #576's dead-letter window (about 48 h: 100 retries of 60 s doubling to 30 min, `dead_letters::dlq_retry_plan`)
//! is NOT enough alone: it starts at the message's first delivery from the dead letter queue, after its whole
//! life in the main queue, so a letter deferred at the ceiling to its last delivery, and parked there, names an
//! object already older than 48 h. An object swept under a live name is a replay with no bytes (a fault letter
//! unless its subject landed, the twin rule), so the window errs long: an orphan costs its bytes for eight days.
//!
//! Counted `queue_r2_orphans_swept_total` and `queue_r2_orphans_swept_bytes_total` (a delete that faults:
//! `queue_r2_orphan_sweep_faults_total`), each sweep logged `[beef-blobs] SWEPT`. `/health/invariants.queue.r2`
//! serves the objects AT REST from the listing itself: the count and bytes of the last complete round over the
//! bucket, and the round in progress.
//!
//! ## The operator's lever (the d3 fold-4, E585-D3-DELTA2-L1)
//!
//! `POST /internal/beef-blob-sweep` (bearer `INTERNAL_TOKEN`, compared in fixed time, as the #576 levers; the body
//! empty or `{}`, anything else a 400) runs ONE pass through [`run_pass`], the very function the scheduled tick
//! calls: the same bounds, the same `SWEEP_BUDGET_MS` race, the same cursor row and the same counters. It answers
//! the pass ([`pass_json`]): what it listed, deleted and could not read, its faults, the cursor before and after,
//! `lastPassAt`; 200 for a pass that ran, 503 for one that stopped (`stopped` says why). A pass stopped by a
//! missing binding or a state, listing or named-keys read that faulted swept nothing and moved no cursor. A pass
//! the 30 s budget DROPPED (the d3 fold-5, E585-D3-DELTA3-L1) answers the deletes it made before the drop
//! (`deleted`, `deletedBytes`, each logged SWEPT, and counted: the counters are bumped from what the pass did,
//! after the race) and `cursorAfter: null`: no cursor saved, unless its save was in flight at the drop, which
//! `stopped` then says (the statement may land after it; the health block's `round.startAfter` tells). For an
//! operator after a bulk discard or an R2 audit, and for the route tier, which no longer fires the whole
//! production tick (its peers, WhatsOnChain, the broadcasters) to test one pass.
//!
//! ## An object the sweep cannot read (the d3 fold-4, the delta-2 lens's N3)
//!
//! A listed object whose key or `uploaded` date does not read is SKIPPED and COUNTED, never swept: the pass goes
//! on over the rest of its page, the cursor passes it (by its key, when the key reads), the next round meets it
//! again, `queue_r2_orphan_sweep_unreadable_total` counts it and `/health/invariants.queue.r2.sweep` names the
//! last pass's count and its first key (`lastUnreadable`, `lastUnreadableKey`). Before the fold one such object
//! failed the whole page closed on every pass: the sweep stalled at it for good, with nothing counted. A `head`
//! that faults before a delete was already a counted fault that leaves the object and goes on. The operator
//! deletes an unreadable object by hand (`wrangler r2 object delete`) if it is the door's. Limit, stated: a page
//! of [`SWEEP_MAX_OBJECTS`] entries NONE of whose keys read leaves the cursor where it was (no key to pass), so
//! that round stalls there, counted on every pass.

use crate::d1::Query;
use serde::Deserialize;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use worker::wasm_bindgen::{JsCast, JsValue};
use worker::{js_sys, D1Database, Env};

/// The objects the door writes (`queue::r2_key`); nothing else in the bucket is listed or touched.
pub const SWEEP_PREFIX: &str = "mutations/";
/// One pass lists at most this many objects.
pub const SWEEP_MAX_OBJECTS: u32 = 200;
/// One pass deletes at most this many (each a `head` and a `delete`): a pass that meets more stops at the last
/// one it handled and the next pass goes on from there.
pub const SWEEP_MAX_DELETES: usize = 50;
/// The pass's wall-clock slice in the scheduled tick (one list, at most three D1 statements and 100 R2 calls). A
/// dropped pass keeps (and counts) the deletes it made, saved no cursor unless its save was in flight, and its
/// page is listed again by the next pass.
pub const SWEEP_BUDGET_MS: u64 = 30_000;
/// Cloudflare Queues' default message retention, four days. Neither the mutation queue nor its dead letter queue
/// sets another (it is set at `wrangler queues create`, not in a wrangler config: the operator's `wrangler queues
/// info` says); a queue given a LONGER retention needs this raised with it.
pub const QUEUE_RETENTION_S: u64 = 345_600;
/// An object older than this that no dead letter row names is an orphan: a message's whole life in the main
/// queue, then in the dead letter queue (the module doc).
pub const ORPHAN_WINDOW_S: u64 = 2 * QUEUE_RETENTION_S;

/// The sweep's state at rest: one row. Transient (a lost row restarts the round at the first key).
pub const SWEEP_STATE_CREATE: &str = "CREATE TABLE IF NOT EXISTS beef_blob_sweep (id INTEGER PRIMARY KEY CHECK (id = 1), start_after TEXT NOT NULL DEFAULT '', round_objects INTEGER NOT NULL DEFAULT 0, round_bytes INTEGER NOT NULL DEFAULT 0, round_started_at INTEGER, full_objects INTEGER, full_bytes INTEGER, full_at INTEGER, last_pass_at INTEGER, last_listed INTEGER NOT NULL DEFAULT 0, last_swept INTEGER NOT NULL DEFAULT 0)";
/// The d3 fold-4 (the delta-2 lens's N3): the last pass's unreadable entries, counted and named. Additive ALTERs; the
/// runner ignores the re-run "duplicate column".
pub const SWEEP_STATE_UNREADABLE_COLUMN: &str =
    "ALTER TABLE beef_blob_sweep ADD COLUMN last_unreadable INTEGER NOT NULL DEFAULT 0";
pub const SWEEP_STATE_UNREADABLE_KEY_COLUMN: &str =
    "ALTER TABLE beef_blob_sweep ADD COLUMN last_unreadable_key TEXT";
pub const SWEEP_STATE_SQL: &str = "SELECT start_after, round_objects, round_bytes, round_started_at, full_objects, full_bytes, full_at, last_pass_at, last_listed, last_swept, last_unreadable, last_unreadable_key FROM beef_blob_sweep WHERE id = 1";
/// Binds: start_after, round_objects, round_bytes, round_started_at, full_objects, full_bytes, full_at,
/// last_pass_at, last_listed, last_swept, last_unreadable, last_unreadable_key.
pub const SWEEP_STATE_SAVE_SQL: &str = "INSERT INTO beef_blob_sweep (id, start_after, round_objects, round_bytes, round_started_at, full_objects, full_bytes, full_at, last_pass_at, last_listed, last_swept, last_unreadable, last_unreadable_key) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12) \
     ON CONFLICT(id) DO UPDATE SET start_after = excluded.start_after, round_objects = excluded.round_objects, round_bytes = excluded.round_bytes, round_started_at = excluded.round_started_at, full_objects = excluded.full_objects, full_bytes = excluded.full_bytes, full_at = excluded.full_at, last_pass_at = excluded.last_pass_at, last_listed = excluded.last_listed, last_swept = excluded.last_swept, last_unreadable = excluded.last_unreadable, last_unreadable_key = excluded.last_unreadable_key";
/// Every object a dead letter row names, whatever its status (at most the ceiling's 2000 rows hold one). Read
/// only by a pass that listed an object past the window.
pub const NAMED_KEYS_SQL: &str =
    "SELECT r2_key FROM mutation_dead_letters WHERE r2_key IS NOT NULL";

/// One listed object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub bytes: u64,
    /// R2's `uploaded` stamp, ms.
    pub uploaded_ms: i64,
    /// The writer's `customMetadata.touched` stamp, ms (`queue::TOUCHED_META`); `None` on an object written before
    /// the d3 fold-2, or one whose stamp does not parse.
    pub touched_ms: Option<i64>,
}

impl Listed {
    /// The object's AGE STAMP: the later of `uploaded` and `touched`.
    #[must_use]
    pub fn age_ms(&self) -> i64 {
        self.touched_ms
            .map_or(self.uploaded_ms, |t| t.max(self.uploaded_ms))
    }
}

/// The state at rest (`beef_blob_sweep`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepState {
    /// The last key the last pass handled; empty at the start of a round.
    pub start_after: String,
    /// The objects and bytes left at rest by the passes of the round in progress.
    pub round_objects: u64,
    pub round_bytes: u64,
    pub round_started_at: Option<i64>,
    /// The last COMPLETE round: every object under the prefix, as the listing said.
    pub full_objects: Option<u64>,
    pub full_bytes: Option<u64>,
    pub full_at: Option<i64>,
    pub last_pass_at: Option<i64>,
    pub last_listed: u64,
    pub last_swept: u64,
    /// The entries the last pass could not read (skipped, never swept), and the first of their keys that read.
    pub last_unreadable: u64,
    pub last_unreadable_key: Option<String>,
}

/// PURE: is the object past the window?
#[must_use]
pub fn past_window(o: &Listed, now_ms: i64, window_s: u64) -> bool {
    now_ms.saturating_sub(o.age_ms()) > (window_s as i64).saturating_mul(1000)
}

/// PURE: does the page hold an object past the window (the only case the named keys are read for)?
#[must_use]
pub fn any_past_window(listed: &[Listed], now_ms: i64, window_s: u64) -> bool {
    listed.iter().any(|o| past_window(o, now_ms, window_s))
}

/// What one pass does with its page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassPlan {
    /// How many of the page's objects, from its first, this pass handles (all of them unless the delete budget
    /// ran out: the cursor stops at the last handled).
    pub handled: usize,
    /// The indexes (into the page) of the orphans to delete: past the window and named by no dead letter.
    pub orphans: Vec<usize>,
}

/// PURE: the pass over one page. `named` is every key a dead letter row names.
#[must_use]
pub fn plan_pass(
    listed: &[Listed],
    named: &HashSet<String>,
    now_ms: i64,
    window_s: u64,
    max_deletes: usize,
) -> PassPlan {
    let mut orphans = Vec::new();
    for (i, o) in listed.iter().enumerate() {
        if past_window(o, now_ms, window_s) && !named.contains(&o.key) {
            if orphans.len() == max_deletes {
                return PassPlan {
                    handled: i,
                    orphans,
                };
            }
            orphans.push(i);
        }
    }
    PassPlan {
        handled: listed.len(),
        orphans,
    }
}

/// PURE: the read just before a delete. The object is deleted only if it is still there under the stamps the
/// listing gave: one written again since (the same key, a new `touched` or `uploaded`, a new message naming it) is
/// left.
#[must_use]
pub fn still_orphan(listed: &Listed, head: Option<&Listed>) -> bool {
    head.is_some_and(|h| h.uploaded_ms == listed.uploaded_ms && h.age_ms() == listed.age_ms())
}

/// PURE: the state after a pass. `handled` are the page's objects the pass handled, `swept` the keys it deleted,
/// `more` whether the bucket holds keys past the last handled one (the listing was truncated, or the delete
/// budget stopped the pass inside its page).
#[must_use]
pub fn next_state(
    state: &SweepState,
    handled: &[Listed],
    swept: &HashSet<String>,
    more: bool,
    now_ms: i64,
) -> SweepState {
    let at_rest = handled.iter().filter(|o| !swept.contains(&o.key));
    let (objects, bytes) = at_rest.fold((0u64, 0u64), |(c, b), o| (c + 1, b + o.bytes));
    let round_objects = state.round_objects + objects;
    let round_bytes = state.round_bytes + bytes;
    let mut next = SweepState {
        last_pass_at: Some(now_ms),
        last_listed: handled.len() as u64,
        last_swept: swept.len() as u64,
        ..state.clone()
    };
    if more {
        next.start_after = handled
            .last()
            .map_or_else(|| state.start_after.clone(), |o| o.key.clone());
        next.round_objects = round_objects;
        next.round_bytes = round_bytes;
        next.round_started_at = state.round_started_at.or(Some(now_ms));
    } else {
        next.full_objects = Some(round_objects);
        next.full_bytes = Some(round_bytes);
        next.full_at = Some(now_ms);
        next.start_after = String::new();
        next.round_objects = 0;
        next.round_bytes = 0;
        next.round_started_at = None;
    }
    next
}

/// One entry of a listing (the d3 fold-4, N3): an object, or one whose key or `uploaded` date does not read (its
/// key when that reads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Object(Listed),
    Unreadable(Option<String>),
}

/// A listed page, split: the objects a pass plans over, the entries it skips, and the last key on the page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Page {
    pub listed: Vec<Listed>,
    /// Each unreadable entry's key, `None` where the key itself does not read.
    pub unreadable: Vec<Option<String>>,
    /// The last key the page names, an unreadable entry's included (R2 lists in key order).
    pub last_key: Option<String>,
}

/// PURE (the d3 fold-4, N3): the page a pass plans over. An unreadable entry is skipped and counted, never fails the
/// page, and its key (when it reads) still moves the cursor past it.
#[must_use]
pub fn split_page(entries: Vec<Entry>) -> Page {
    let mut page = Page::default();
    for e in entries {
        match e {
            Entry::Object(o) => {
                page.last_key = Some(o.key.clone());
                page.listed.push(o);
            }
            Entry::Unreadable(k) => {
                if let Some(k) = &k {
                    page.last_key = Some(k.clone());
                }
                page.unreadable.push(k);
            }
        }
    }
    page
}

/// PURE: the state after a pass over a split page ([`next_state`] over the handled objects, then the unreadable
/// entries). When the plan handled its whole page, the cursor is the page's LAST KEY, so an unreadable entry at the
/// end of a truncated page is passed too; a pass the delete budget stopped keeps [`next_state`]'s cursor (the
/// last handled object) and meets the rest of the page, unreadable entries included, on the next pass.
#[must_use]
pub fn after_pass(
    state: &SweepState,
    page: &Page,
    plan: &PassPlan,
    swept: &HashSet<String>,
    truncated: bool,
    now_ms: i64,
) -> SweepState {
    let whole = plan.handled == page.listed.len();
    let more = truncated || !whole;
    let mut next = next_state(state, &page.listed[..plan.handled], swept, more, now_ms);
    if more && whole {
        if let Some(k) = &page.last_key {
            next.start_after = k.clone();
            next.round_started_at = next.round_started_at.or(Some(now_ms));
        }
    }
    next.last_unreadable = page.unreadable.len() as u64;
    next.last_unreadable_key = page.unreadable.iter().flatten().next().cloned();
    next
}

#[derive(Deserialize)]
struct StateRow {
    start_after: String,
    round_objects: f64,
    round_bytes: f64,
    round_started_at: Option<f64>,
    full_objects: Option<f64>,
    full_bytes: Option<f64>,
    full_at: Option<f64>,
    last_pass_at: Option<f64>,
    last_listed: f64,
    last_swept: f64,
    last_unreadable: Option<f64>,
    last_unreadable_key: Option<String>,
}

impl From<StateRow> for SweepState {
    fn from(r: StateRow) -> Self {
        let n = |v: f64| v.max(0.0) as u64;
        Self {
            start_after: r.start_after,
            round_objects: n(r.round_objects),
            round_bytes: n(r.round_bytes),
            round_started_at: r.round_started_at.map(|v| v as i64),
            full_objects: r.full_objects.map(n),
            full_bytes: r.full_bytes.map(n),
            full_at: r.full_at.map(|v| v as i64),
            last_pass_at: r.last_pass_at.map(|v| v as i64),
            last_listed: n(r.last_listed),
            last_swept: n(r.last_swept),
            last_unreadable: r.last_unreadable.map_or(0, n),
            last_unreadable_key: r.last_unreadable_key,
        }
    }
}

/// The save of the state at rest ([`SWEEP_STATE_SAVE_SQL`]).
#[must_use]
pub fn save_query(s: &SweepState) -> Query {
    let opt = |v: Option<i64>| v.map_or(crate::d1::QVal::Null, crate::d1::QVal::Int);
    Query::new(SWEEP_STATE_SAVE_SQL)
        .bind(s.start_after.as_str())
        .bind(s.round_objects)
        .bind(s.round_bytes)
        .bind(opt(s.round_started_at))
        .bind(opt(s.full_objects.map(|v| v as i64)))
        .bind(opt(s.full_bytes.map(|v| v as i64)))
        .bind(opt(s.full_at))
        .bind(opt(s.last_pass_at))
        .bind(s.last_listed)
        .bind(s.last_swept)
        .bind(s.last_unreadable)
        .bind(
            s.last_unreadable_key
                .as_deref()
                .map_or(crate::d1::QVal::Null, crate::d1::QVal::from),
        )
}

async fn read_state(db: &D1Database) -> Result<SweepState, String> {
    Ok(Query::new(SWEEP_STATE_SQL)
        .fetch_optional::<StateRow>(db)
        .await?
        .map(SweepState::from)
        .unwrap_or_default())
}

/// PURE (the d3 fold-3, E585-D3-DELTA-M1): the `touched` stamp from the value the listing gave for
/// `customMetadata.touched`, `None` when the field, or the whole `customMetadata`, is absent, or does not parse:
/// the object is then aged by `uploaded` alone (the fold-2 rule).
#[must_use]
pub fn touched_ms_of(stamp: Option<&str>) -> Option<i64> {
    stamp.and_then(|t| t.parse::<i64>().ok())
}

/// The raw `customMetadata.touched` of one listed or headed object, read through `Reflect` with a guard (the d3
/// fold-3, E585-D3-DELTA-M1). workers-rs 0.8.5's `Object::custom_metadata` unwraps the getter and then calls
/// `js_sys::Object::keys` (no `catch`) on what may be `undefined` (an object written with no custom metadata: the
/// platform's answer under `include` is not documented, miniflare always answers `{}`): a JS `TypeError` through
/// the wasm frame, which no `.ok()` sees, ended the scheduled tick before its GASP step on every tick. Here
/// anything but an object is "no stamp", and `Reflect::get` is a `catch` binding.
fn touched_field(o: &JsValue) -> Option<String> {
    let meta = js_sys::Reflect::get(o, &JsValue::from_str("customMetadata")).ok()?;
    if !meta.is_object() {
        return None;
    }
    js_sys::Reflect::get(&meta, &JsValue::from_str(crate::queue::TOUCHED_META))
        .ok()?
        .as_string()
}

/// One raw R2 object (`R2Object`, from `list` or `head`), every field read through `Reflect`. `None` when its key
/// or its `uploaded` date cannot be read (the platform's contract broken: the listing skips and counts it, a
/// `head` is a counted fault). Never throws.
fn listed_of_js(o: &JsValue) -> Option<Listed> {
    let get = |k: &str| js_sys::Reflect::get(o, &JsValue::from_str(k)).ok();
    let key = get("key")?.as_string()?;
    let bytes = get("size")
        .and_then(|v| v.as_f64())
        .map_or(0, |s| s.max(0.0) as u64);
    let uploaded = get("uploaded")?.dyn_into::<js_sys::Date>().ok()?.get_time();
    if !uploaded.is_finite() {
        return None;
    }
    Some(Listed {
        key,
        bytes,
        uploaded_ms: uploaded as i64,
        touched_ms: touched_ms_of(touched_field(o).as_deref()),
    })
}

/// Calls `method(arg)` on the bucket's JS object and awaits its promise; every step a `catch`ed `Result`.
async fn bucket_call(
    bucket: &worker::Bucket,
    method: &str,
    arg: &JsValue,
) -> Result<JsValue, String> {
    let this: &JsValue = bucket.as_ref();
    let js = |e: JsValue| worker::Error::from(e).to_string();
    let f = js_sys::Reflect::get(this, &JsValue::from_str(method))
        .map_err(js)?
        .dyn_into::<js_sys::Function>()
        .map_err(js)?;
    let promise = js_sys::Promise::resolve(&f.call1(this, arg).map_err(js)?);
    worker::wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .map_err(js)
}

/// The page a pass lists: at most [`SWEEP_MAX_OBJECTS`] under [`SWEEP_PREFIX`] after `start_after`, with the
/// custom metadata, and whether the bucket holds more. An object whose key or date does not read is an
/// [`Entry::Unreadable`] (the d3 fold-4, N3: skipped and counted by the pass, never a fault of the page).
async fn list_page(
    bucket: &worker::Bucket,
    start_after: &str,
) -> Result<(Vec<Entry>, bool), String> {
    let set = |o: &js_sys::Object, k: &str, v: &JsValue| {
        js_sys::Reflect::set(o, &JsValue::from_str(k), v)
            .map(|_| ())
            .map_err(|e| worker::Error::from(e).to_string())
    };
    let opts = js_sys::Object::new();
    set(&opts, "prefix", &JsValue::from_str(SWEEP_PREFIX))?;
    set(&opts, "limit", &JsValue::from(SWEEP_MAX_OBJECTS))?;
    let include = js_sys::Array::new();
    include.push(&JsValue::from_str("customMetadata"));
    set(&opts, "include", &include)?;
    if !start_after.is_empty() {
        set(&opts, "startAfter", &JsValue::from_str(start_after))?;
    }
    let page = bucket_call(bucket, "list", &opts).await?;
    let objects = js_sys::Reflect::get(&page, &JsValue::from_str("objects"))
        .ok()
        .filter(js_sys::Array::is_array)
        .map(|v| v.unchecked_into::<js_sys::Array>())
        .ok_or("the listing holds no objects array")?;
    let listed = objects
        .iter()
        .map(|o| match listed_of_js(&o) {
            Some(l) => Entry::Object(l),
            None => Entry::Unreadable(
                js_sys::Reflect::get(&o, &JsValue::from_str("key"))
                    .ok()
                    .and_then(|k| k.as_string()),
            ),
        })
        .collect();
    let truncated = js_sys::Reflect::get(&page, &JsValue::from_str("truncated"))
        .ok()
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Ok((listed, truncated))
}

/// The read just before a delete: `None` when the object is gone.
async fn head_of(bucket: &worker::Bucket, key: &str) -> Result<Option<Listed>, String> {
    let o = bucket_call(bucket, "head", &JsValue::from_str(key)).await?;
    if o.is_null() || o.is_undefined() {
        return Ok(None);
    }
    listed_of_js(&o)
        .map(Some)
        .ok_or_else(|| format!("the head of {key} does not read"))
}

/// What one pass did: the lever's answer ([`pass_json`]) and the tick's log line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassOutcome {
    /// Why the pass ended before its plan; `None` for a pass that ran. A pass stopped by a missing binding or a
    /// state, listing or named-keys read swept nothing and moved no cursor; one the budget DROPPED keeps what it
    /// did before the drop (its deletes, counted) and says whether its cursor's save was in flight.
    pub stopped: Option<String>,
    /// The entries the listing returned, the objects the pass handled, the entries it could not read and the first
    /// of their keys.
    pub listed: u64,
    pub handled: u64,
    pub unreadable: u64,
    pub unreadable_key: Option<String>,
    pub swept: u64,
    pub swept_bytes: u64,
    /// `head`s and deletes that faulted (each object stays for the next round).
    pub faults: u64,
    /// The cursor at rest before the pass (`None`: the state did not read) and after it (`None`: not saved).
    pub cursor_before: Option<String>,
    pub cursor_after: Option<String>,
    /// The pass's stamp, as saved (`None`: not saved).
    pub last_pass_at: Option<i64>,
    /// This pass ended a round over the whole bucket (`atRest` was renewed).
    pub round_complete: bool,
}

impl PassOutcome {
    fn stopped(why: String, cursor_before: Option<String>) -> Self {
        Self {
            stopped: Some(why),
            cursor_before,
            ..Self::default()
        }
    }
}

/// What a pass has done SO FAR (the d3 fold-5, E585-D3-DELTA3-L1): written by [`sweep_pass`] as each step lands
/// (a delete the moment it answered), read by [`run_pass_with`] when the budget drops the pass, so a dropped pass
/// answers and counts the deletes it made before the drop.
#[derive(Debug, Default)]
pub struct PassProgress {
    out: RefCell<PassOutcome>,
    /// The save of the cursor was started and has not answered.
    saving: Cell<bool>,
}

impl PassProgress {
    /// The outcome of a pass the budget dropped: what it did up to the drop, no cursor after it (the body cannot
    /// know whether a save in flight landed: it says so).
    #[must_use]
    pub fn dropped(&self) -> PassOutcome {
        let mut o = self.out.borrow().clone();
        let cursor = if self.saving.get() {
            "its cursor's save was IN FLIGHT and may have landed (read /health/invariants.queue.r2.round.startAfter)"
        } else {
            "no cursor saved (the next pass lists again from the cursor at rest)"
        };
        o.stopped = Some(format!(
            "the pass EXCEEDED its {SWEEP_BUDGET_MS} ms budget: dropped after {} deletes of {} bytes (each logged SWEPT and counted); {cursor}",
            o.swept, o.swept_bytes
        ));
        o.cursor_after = None;
        o.last_pass_at = None;
        o.round_complete = false;
        o
    }
}

/// What one pass needs of the platform (the d3 fold-5, E585-D3-DELTA3-L2): the bucket, the state at rest, the dead
/// letters' keys, the counters, the clock and the log. The worker's ([`WorkerSweep`]) is R2 and D1; a native test
/// gives its own, so the shipped pass ([`sweep_pass`], [`run_pass_with`]) runs in the lib's tests as is.
pub(crate) trait SweepPort {
    async fn read_state(&self) -> Result<SweepState, String>;
    async fn list_page(&self, start_after: &str) -> Result<(Vec<Entry>, bool), String>;
    async fn named_keys(&self) -> Result<HashSet<String>, String>;
    async fn head(&self, key: &str) -> Result<Option<Listed>, String>;
    async fn delete(&self, key: &str) -> Result<(), String>;
    async fn save(&self, next: &SweepState) -> Result<(), String>;
    async fn bump(&self, counter: &str, delta: u64);
    fn now_ms(&self) -> i64;
    fn log(&self, line: &str);
}

/// The worker's [`SweepPort`]: `BEEF_BLOBS` and `OVERLAY_DB`.
pub(crate) struct WorkerSweep<'a> {
    pub bucket: &'a worker::Bucket,
    pub db: &'a D1Database,
}

impl SweepPort for WorkerSweep<'_> {
    async fn read_state(&self) -> Result<SweepState, String> {
        read_state(self.db).await
    }

    async fn list_page(&self, start_after: &str) -> Result<(Vec<Entry>, bool), String> {
        list_page(self.bucket, start_after).await
    }

    async fn named_keys(&self) -> Result<HashSet<String>, String> {
        #[derive(Deserialize)]
        struct Named {
            r2_key: String,
        }
        Query::new(NAMED_KEYS_SQL)
            .fetch_all::<Named>(self.db)
            .await
            .map(|rows| rows.into_iter().map(|r| r.r2_key).collect())
    }

    async fn head(&self, key: &str) -> Result<Option<Listed>, String> {
        head_of(self.bucket, key).await
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.bucket.delete(key).await.map_err(|e| e.to_string())
    }

    async fn save(&self, next: &SweepState) -> Result<(), String> {
        save_query(next).execute(self.db).await
    }

    async fn bump(&self, counter: &str, delta: u64) {
        crate::ops::bump_counter(self.db, counter, delta).await;
    }

    fn now_ms(&self) -> i64 {
        worker::Date::now().as_millis() as i64
    }

    fn log(&self, line: &str) {
        worker::console_log!("{line}");
    }
}

/// ONE PASS of the sweep (the module doc). Fail-closed: a state, listing or named-keys read that faults deletes
/// nothing and moves no cursor; a delete that faults leaves its object for the next round; an entry that does not
/// read is skipped and counted. Each step is recorded in `progress` as it lands. Called only through
/// [`run_pass_with`].
pub(crate) async fn sweep_pass<P: SweepPort>(p: &P, progress: &PassProgress) -> PassOutcome {
    let state = match p.read_state().await {
        Ok(s) => s,
        Err(e) => {
            p.log(&format!(
                "[beef-blobs] the orphan sweep's state could not be read ({e}); nothing listed"
            ));
            return PassOutcome::stopped(format!("the state did not read: {e}"), None);
        }
    };
    let before = Some(state.start_after.clone());
    progress.out.borrow_mut().cursor_before = before.clone();
    let (entries, truncated) = match p.list_page(&state.start_after).await {
        Ok(page) => page,
        Err(e) => {
            p.log(&format!(
                "[beef-blobs] the orphan sweep's listing faulted ({e}); nothing swept"
            ));
            return PassOutcome::stopped(format!("the listing faulted: {e}"), before);
        }
    };
    let listed_count = entries.len() as u64;
    let page = split_page(entries);
    for k in &page.unreadable {
        p.log(&format!(
            "[beef-blobs] the orphan sweep could not read the listed object {}; skipped (counted, never swept)",
            k.as_deref().unwrap_or("<no key>")
        ));
    }
    let listed = &page.listed;
    let now = p.now_ms();
    let named: HashSet<String> = if any_past_window(listed, now, ORPHAN_WINDOW_S) {
        match p.named_keys().await {
            Ok(named) => named,
            Err(e) => {
                p.log(&format!("[beef-blobs] the orphan sweep could not read the dead letters' keys ({e}); nothing swept"));
                return PassOutcome::stopped(
                    format!("the dead letters' keys did not read: {e}"),
                    before,
                );
            }
        }
    } else {
        HashSet::new()
    };
    let plan = plan_pass(listed, &named, now, ORPHAN_WINDOW_S, SWEEP_MAX_DELETES);
    {
        let mut out = progress.out.borrow_mut();
        out.listed = listed_count;
        out.handled = plan.handled as u64;
        out.unreadable = page.unreadable.len() as u64;
        out.unreadable_key = page.unreadable.iter().flatten().next().cloned();
    }
    let mut swept: HashSet<String> = HashSet::new();
    for o in plan.orphans.iter().map(|i| &listed[*i]) {
        let head = match p.head(&o.key).await {
            Ok(h) => h,
            Err(e) => {
                progress.out.borrow_mut().faults += 1;
                p.log(&format!(
                    "[beef-blobs] the orphan sweep's read of {} faulted ({e}); the object stays",
                    o.key
                ));
                continue;
            }
        };
        if !still_orphan(o, head.as_ref()) {
            continue;
        }
        match p.delete(&o.key).await {
            Ok(()) => {
                swept.insert(o.key.clone());
                {
                    let mut out = progress.out.borrow_mut();
                    out.swept += 1;
                    out.swept_bytes += o.bytes;
                }
                p.log(&format!(
                    "[beef-blobs] SWEPT {} bytes={} age_s={} (an orphan: past the {ORPHAN_WINDOW_S} s window, no dead letter names it)",
                    o.key,
                    o.bytes,
                    now.saturating_sub(o.age_ms()) / 1000
                ));
            }
            Err(e) => {
                progress.out.borrow_mut().faults += 1;
                p.log(&format!(
                    "[beef-blobs] the orphan sweep's delete of {} faulted ({e}); the object stays",
                    o.key
                ));
            }
        }
    }
    let next = after_pass(&state, &page, &plan, &swept, truncated, now);
    progress.saving.set(true);
    let saved = match p.save(&next).await {
        Ok(()) => true,
        Err(e) => {
            p.log(&format!("[beef-blobs] the orphan sweep's state could not be saved ({e}); the next pass lists this page again"));
            false
        }
    };
    progress.saving.set(false);
    let mut out = progress.out.borrow().clone();
    out.unreadable = next.last_unreadable;
    out.unreadable_key = next.last_unreadable_key.clone();
    out.round_complete = saved && next.full_at == Some(now);
    out.cursor_after = saved.then(|| next.start_after.clone());
    out.last_pass_at = saved.then_some(now);
    out
}

/// ONE bounded pass over a port (the d3 fold-5): [`sweep_pass`] raced against `deadline`, then the counters bumped
/// from what the pass DID, whether it ran or was dropped (E585-D3-DELTA3-L1: the counters were bumped inside the
/// pass after its save, so a dropped pass's deletes, each logged SWEPT, were never counted).
pub(crate) async fn run_pass_with<P: SweepPort, D: std::future::Future<Output = ()>>(
    p: &P,
    deadline: D,
) -> PassOutcome {
    let progress = PassProgress::default();
    let out = overlay_engine::gasp::race_or_deadline(sweep_pass(p, &progress), deadline)
        .await
        .unwrap_or_else(|| progress.dropped());
    p.bump(crate::ops::COUNTER_QUEUE_R2_ORPHANS_SWEPT, out.swept)
        .await;
    p.bump(
        crate::ops::COUNTER_QUEUE_R2_ORPHANS_SWEPT_BYTES,
        out.swept_bytes,
    )
    .await;
    p.bump(crate::ops::COUNTER_QUEUE_R2_ORPHAN_SWEEP_FAULTS, out.faults)
        .await;
    p.bump(
        crate::ops::COUNTER_QUEUE_R2_ORPHAN_SWEEP_UNREADABLE,
        out.unreadable,
    )
    .await;
    out
}

/// ONE bounded pass, as the scheduled tick and the operator's lever both run it (the d3 fold-4, DELTA2-L1): the
/// pass under its [`SWEEP_BUDGET_MS`] race ([`run_pass_with`]: a dropped pass answers and counts its deletes up to
/// the drop and saved no cursor, unless its save was in flight), and one log line.
pub async fn run_pass(env: &Env, db: &D1Database, by: &str) -> PassOutcome {
    let out = match env.bucket(crate::queue::BEEF_BLOBS_BINDING) {
        Ok(bucket) => {
            run_pass_with(
                &WorkerSweep {
                    bucket: &bucket,
                    db,
                },
                crate::broadcaster::sleep_ms(SWEEP_BUDGET_MS),
            )
            .await
        }
        Err(e) => {
            worker::console_log!(
                "[beef-blobs] the orphan sweep found no {} binding ({e}); nothing listed",
                crate::queue::BEEF_BLOBS_BINDING
            );
            PassOutcome::stopped(
                format!("no {} binding", crate::queue::BEEF_BLOBS_BINDING),
                None,
            )
        }
    };
    worker::console_log!("[beef-blobs] sweep pass ({by}): {}", pass_json(&out));
    out
}

/// PURE: the lever's body for one pass.
#[must_use]
pub fn pass_json(o: &PassOutcome) -> serde_json::Value {
    serde_json::json!({
        "ok": o.stopped.is_none(),
        "stopped": o.stopped,
        "listed": o.listed,
        "handled": o.handled,
        "unreadable": o.unreadable,
        "unreadableKey": o.unreadable_key,
        "deleted": o.swept,
        "deletedBytes": o.swept_bytes,
        "faults": o.faults,
        "cursorBefore": o.cursor_before,
        "cursorAfter": o.cursor_after,
        "lastPassAt": o.last_pass_at,
        "roundComplete": o.round_complete,
        "budget": {
            "maxObjectsPerPass": SWEEP_MAX_OBJECTS,
            "maxDeletesPerPass": SWEEP_MAX_DELETES,
            "windowSecs": ORPHAN_WINDOW_S,
            "budgetMs": SWEEP_BUDGET_MS,
        },
    })
}

/// PURE: the lever's body must be empty (whitespace) or a JSON object with no fields (`{}`); a pass takes no
/// argument.
#[must_use]
pub fn parse_sweep_request(raw: &[u8]) -> bool {
    raw.iter().all(u8::is_ascii_whitespace)
        || serde_json::from_slice::<serde_json::Value>(raw)
            .ok()
            .and_then(|v| v.as_object().map(serde_json::Map::is_empty))
            .unwrap_or(false)
}

/// `POST /internal/beef-blob-sweep` (bearer `INTERNAL_TOKEN`, as the #576 levers; the d3 fold-4, DELTA2-L1): ONE
/// pass through [`run_pass`], the scheduled tick's own function. 200 with [`pass_json`] for a pass that ran, 503
/// with it for one that stopped (before its plan, or dropped by the budget with its deletes so far).
pub async fn internal_sweep(
    mut req: worker::Request,
    env: &Env,
) -> worker::Result<worker::Response> {
    let authorization = req.headers().get("authorization").ok().flatten();
    let secret = env.secret("INTERNAL_TOKEN").ok().map(|s| s.to_string());
    if !crate::tip_pass::bearer_ok(authorization.as_deref(), secret.as_deref()) {
        worker::console_log!("POST /internal/beef-blob-sweep -> 401");
        return worker::Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    if !parse_sweep_request(&raw) {
        return worker::Response::error("body must be empty or {}: a pass takes no argument", 400);
    }
    let db = env.d1("OVERLAY_DB")?;
    crate::d1::ensure_overlay_migrations(&db)
        .await
        .map_err(worker::Error::from)?;
    let out = run_pass(env, &db, "lever").await;
    let status = if out.stopped.is_none() { 200 } else { 503 };
    worker::console_log!("POST /internal/beef-blob-sweep -> {status}");
    Ok(worker::Response::from_json(&pass_json(&out))?.with_status(status))
}

/// PURE: `/health/invariants.queue`: the R2 objects at rest as the sweep's listing counted them. `atRest` is the
/// last COMPLETE round over the bucket (`null` before the first), `round` the one in progress; `state` `None` is
/// an unreadable table (`readable: false`), distinct from a sweep that never ran (`lastPassAt: null`).
#[must_use]
pub fn queue_json(state: Option<&SweepState>, bound: bool) -> serde_json::Value {
    let sweep = serde_json::json!({
        "windowSecs": ORPHAN_WINDOW_S,
        "maxObjectsPerPass": SWEEP_MAX_OBJECTS,
        "maxDeletesPerPass": SWEEP_MAX_DELETES,
        "prefix": SWEEP_PREFIX,
        "lastPassAt": state.and_then(|s| s.last_pass_at),
        "lastListed": state.map(|s| s.last_listed),
        "lastSwept": state.map(|s| s.last_swept),
        "lastUnreadable": state.map(|s| s.last_unreadable),
        "lastUnreadableKey": state.and_then(|s| s.last_unreadable_key.clone()),
        "lever": "POST /internal/beef-blob-sweep",
    });
    serde_json::json!({ "r2": {
        "bound": bound,
        "readable": state.is_some(),
        "atRest": state.and_then(|s| Some(serde_json::json!({
            "objects": s.full_objects?, "bytes": s.full_bytes?, "at": s.full_at,
        }))),
        "round": state.map(|s| serde_json::json!({
            "objects": s.round_objects, "bytes": s.round_bytes,
            "startedAt": s.round_started_at, "startAfter": s.start_after,
        })),
        "sweep": sweep,
    }})
}

/// `/health/invariants.queue` (one D1 read).
pub async fn health_json(db: &D1Database, env: &Env) -> serde_json::Value {
    let state = read_state(db).await.ok();
    queue_json(
        state.as_ref(),
        env.bucket(crate::queue::BEEF_BLOBS_BINDING).is_ok(),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::d1::QVal;
    use crate::dead_letters;
    use crate::queue;
    use overlay_engine::types::SubmitMode;
    use std::collections::BTreeMap;

    fn binds(q: &Query) -> Vec<rusqlite::types::Value> {
        q.params()
            .iter()
            .map(|p| match p {
                QVal::Null => rusqlite::types::Value::Null,
                QVal::Int(i) => rusqlite::types::Value::Integer(*i),
                QVal::Text(s) => rusqlite::types::Value::Text(s.clone()),
                QVal::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
                QVal::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
                QVal::Float(f) => rusqlite::types::Value::Real(*f),
            })
            .collect()
    }

    /// The shipped schema of the two tables the sweep reads and writes, from the migration list itself.
    fn db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for m in crate::d1::OVERLAY_MIGRATIONS
            .iter()
            .filter(|m| m.contains("mutation_dead_letters") || m.contains("beef_blob_sweep"))
        {
            conn.execute(m, []).unwrap();
        }
        conn
    }

    fn exec(conn: &rusqlite::Connection, q: &Query) {
        let mut stmt = conn.prepare(q.sql()).unwrap();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(binds(q).iter()))
            .unwrap();
        while rows.next().unwrap().is_some() {}
    }

    /// The state at rest, read by the shipped statement.
    fn state(conn: &rusqlite::Connection) -> SweepState {
        let n = |v: Option<i64>| v.map(|v| v as u64);
        conn.query_row(SWEEP_STATE_SQL, [], |r| {
            Ok(SweepState {
                start_after: r.get(0)?,
                round_objects: r.get::<_, i64>(1)? as u64,
                round_bytes: r.get::<_, i64>(2)? as u64,
                round_started_at: r.get(3)?,
                full_objects: n(r.get(4)?),
                full_bytes: n(r.get(5)?),
                full_at: r.get(6)?,
                last_pass_at: r.get(7)?,
                last_listed: r.get::<_, i64>(8)? as u64,
                last_swept: r.get::<_, i64>(9)? as u64,
                last_unreadable: r.get::<_, i64>(10)? as u64,
                last_unreadable_key: r.get(11)?,
            })
        })
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(SweepState::default()),
            e => Err(e),
        })
        .unwrap()
    }

    /// The bucket: key -> (bytes, uploaded ms), listed in key order as R2 lists.
    #[derive(Default)]
    struct Bucket {
        objects: BTreeMap<String, (u64, i64)>,
        /// Keys whose listing entry does not read (the d3 fold-4, N3); `KEYLESS` marks one whose key does not read
        /// either.
        broken: HashSet<String>,
        lists: usize,
        heads: usize,
        deletes: usize,
    }

    impl Bucket {
        fn put(&mut self, key: &str, bytes: u64, at_ms: i64) {
            self.objects.insert(key.to_string(), (bytes, at_ms));
        }
        fn list(&mut self, start_after: &str, limit: usize) -> (Vec<Entry>, bool) {
            self.lists += 1;
            let broken = &self.broken;
            let mut it = self
                .objects
                .iter()
                .filter(|(k, _)| k.starts_with(SWEEP_PREFIX) && k.as_str() > start_after);
            let page: Vec<Entry> = it
                .by_ref()
                .take(limit)
                .map(|(k, (b, u))| {
                    if broken.contains(&format!("{KEYLESS}{k}")) {
                        Entry::Unreadable(None)
                    } else if broken.contains(k) {
                        Entry::Unreadable(Some(k.clone()))
                    } else {
                        Entry::Object(Listed {
                            key: k.clone(),
                            bytes: *b,
                            uploaded_ms: *u,
                            touched_ms: Some(*u),
                        })
                    }
                })
                .collect();
            (page, it.next().is_some())
        }
        fn head(&mut self, key: &str) -> Option<Listed> {
            self.heads += 1;
            self.objects.get(key).map(|(b, u)| Listed {
                key: key.to_string(),
                bytes: *b,
                uploaded_ms: *u,
                touched_ms: Some(*u),
            })
        }
    }

    const KEYLESS: &str = "keyless:";

    /// What one pass did.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Pass {
        swept: Vec<String>,
        swept_bytes: u64,
        named_read: bool,
    }

    /// A change to the bucket between the listing and the deletes.
    type Between<'a> = Option<Box<dyn FnOnce(&mut Bucket) + 'a>>;

    /// The test's [`SweepPort`] (the d3 fold-5, E585-D3-DELTA3-L2): the bucket above, the SHIPPED statements under
    /// real SQLite for the state and the dead letters' keys, the counters in a map (zero skipped, as
    /// `ops::bump_counter`). `named_faults` is the named-keys read faulting; `between` runs at the first `head` (a
    /// re-presentation landing between the listing and the deletes); `hang_after_deletes` makes the next `head`
    /// after that many deletes never answer, and `hang_on_save` the save (a pass the budget drops there).
    struct Model<'a> {
        conn: &'a rusqlite::Connection,
        bucket: RefCell<Bucket>,
        now: i64,
        named_faults: bool,
        between: RefCell<Between<'a>>,
        hang_after_deletes: Option<usize>,
        hang_on_save: bool,
        named_read: Cell<bool>,
        deleted: RefCell<Vec<(String, u64)>>,
        counters: RefCell<BTreeMap<String, u64>>,
    }

    impl<'a> Model<'a> {
        fn new(conn: &'a rusqlite::Connection, bucket: Bucket, now: i64) -> Self {
            Self {
                conn,
                bucket: RefCell::new(bucket),
                now,
                named_faults: false,
                between: RefCell::new(None),
                hang_after_deletes: None,
                hang_on_save: false,
                named_read: Cell::new(false),
                deleted: RefCell::new(Vec::new()),
                counters: RefCell::new(BTreeMap::new()),
            }
        }
        fn counter(&self, name: &str) -> u64 {
            self.counters.borrow().get(name).copied().unwrap_or(0)
        }
    }

    impl SweepPort for Model<'_> {
        async fn read_state(&self) -> Result<SweepState, String> {
            Ok(state(self.conn))
        }
        async fn list_page(&self, start_after: &str) -> Result<(Vec<Entry>, bool), String> {
            Ok(self
                .bucket
                .borrow_mut()
                .list(start_after, SWEEP_MAX_OBJECTS as usize))
        }
        async fn named_keys(&self) -> Result<HashSet<String>, String> {
            self.named_read.set(true);
            if self.named_faults {
                return Err("D1 down".into());
            }
            Ok(self
                .conn
                .prepare(NAMED_KEYS_SQL)
                .unwrap()
                .query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .map(Result::unwrap)
                .collect())
        }
        async fn head(&self, key: &str) -> Result<Option<Listed>, String> {
            if let Some(f) = self.between.borrow_mut().take() {
                f(&mut self.bucket.borrow_mut());
            }
            if self
                .hang_after_deletes
                .is_some_and(|n| self.deleted.borrow().len() >= n)
            {
                std::future::pending::<()>().await;
            }
            Ok(self.bucket.borrow_mut().head(key))
        }
        async fn delete(&self, key: &str) -> Result<(), String> {
            let mut b = self.bucket.borrow_mut();
            let (bytes, _) = b.objects.remove(key).unwrap();
            b.deletes += 1;
            self.deleted.borrow_mut().push((key.to_string(), bytes));
            Ok(())
        }
        async fn save(&self, next: &SweepState) -> Result<(), String> {
            if self.hang_on_save {
                std::future::pending::<()>().await;
            }
            exec(self.conn, &save_query(next));
            Ok(())
        }
        async fn bump(&self, counter: &str, delta: u64) {
            if delta > 0 {
                *self
                    .counters
                    .borrow_mut()
                    .entry(counter.to_string())
                    .or_default() += delta;
            }
        }
        fn now_ms(&self) -> i64 {
            self.now
        }
        fn log(&self, _line: &str) {}
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    /// ONE PASS, the SHIPPED one ([`run_pass_with`] over [`sweep_pass`], no deadline) over the model port: the state
    /// read, the listing from the key at rest, the named keys (read only for a page with an object past the window;
    /// `named_faults` is that read faulting), the plan, the read before each delete, the state saved, the counters.
    /// `between` runs after the listing and before the deletes (a re-presentation landing meanwhile).
    fn pass<'a>(
        conn: &'a rusqlite::Connection,
        bucket: &mut Bucket,
        now: i64,
        named_faults: bool,
        between: impl FnOnce(&mut Bucket) + 'a,
    ) -> Pass {
        let mut m = Model::new(conn, std::mem::take(bucket), now);
        m.named_faults = named_faults;
        *m.between.borrow_mut() = Some(Box::new(between));
        block_on(run_pass_with(&m, std::future::pending::<()>()));
        *bucket = m.bucket.into_inner();
        let deleted = m.deleted.into_inner();
        Pass {
            swept: deleted.iter().map(|(k, _)| k.clone()).collect(),
            swept_bytes: deleted.iter().map(|(_, b)| b).sum(),
            named_read: m.named_read.get(),
        }
    }

    /// The door's write of a body past the room, and the key its message would name.
    fn written(
        bucket: &mut Bucket,
        seed: u8,
        topics: &[String],
        at_ms: i64,
    ) -> queue::MutationMessage {
        let beef: Vec<u8> = (0..8_000usize).map(|i| (i as u8) ^ seed).collect();
        let queue::Carriage::R2(m) = queue::plan_replay(
            &beef,
            topics,
            SubmitMode::HistoricalTx,
            queue::REPLAY_REASON_PHASE3_FAULT,
            queue::QUEUE_MESSAGE_ROOM_MIN,
        ) else {
            panic!("8 KB rides by key under the lowest room")
        };
        let r = m.r2.as_ref().unwrap();
        assert!(
            r.key.starts_with(SWEEP_PREFIX),
            "the sweep lists the door's own prefix"
        );
        bucket.put(&r.key, r.bytes, at_ms);
        m
    }

    fn park(conn: &rusqlite::Connection, m: &queue::MutationMessage, now: i64) {
        let r = m.r2.as_ref().unwrap();
        let (txid, topics) = dead_letters::letter_key(m, None);
        exec(
            conn,
            &dead_letters::park_query_r2(
                &txid,
                &topics,
                &serde_json::to_string(m).unwrap(),
                dead_letters::FAULT_UNRECORDED,
                0,
                now,
                Some((r.key.as_str(), r.bytes)),
            ),
        );
    }

    const WINDOW_MS: i64 = ORPHAN_WINDOW_S as i64 * 1000;

    /// bsv-low #585, door 3's fold (the ORPHANS). An object written by a send that then faulted (the door writes
    /// R2 first; no message, no letter names it) and aged past the window is SWEPT; an object of the same age a
    /// parked letter names is not; an object younger than the window is not; one written again between the
    /// listing and its delete is not. The objects at rest are served from the listing. On `45aceff` there is no
    /// sweep: the orphan stays for good and nothing lists it.
    #[test]
    fn e585_d3f_an_orphan_past_the_window_is_swept_a_named_and_a_young_one_are_not() {
        let conn = db();
        let mut bucket = Bucket::default();
        let topics = vec!["tm_a".to_string()];
        let t0 = 1_800_000_000_000i64;
        // the faulted send: written, never enqueued, never re-presented
        let orphan = written(&mut bucket, 1, &topics, t0);
        // a dead-lettered submission, parked: its row names its object
        let held = written(&mut bucket, 2, &topics, t0);
        park(&conn, &held, t0 + 60_000);
        // a submission whose message is still in a queue
        let young = written(&mut bucket, 3, &topics, t0 + 2_000);
        // not the door's: never listed
        bucket.put("other/thing", 5, 0);
        let key = |m: &queue::MutationMessage| m.r2.as_ref().unwrap().key.clone();

        // at the window, to the millisecond: nothing is past it, the dead letters are not even read
        assert_eq!(
            pass(&conn, &mut bucket, t0 + WINDOW_MS, false, |_| {}),
            Pass::default()
        );
        assert_eq!(bucket.objects.len(), 4);
        assert_eq!(
            (state(&conn).full_objects, state(&conn).full_bytes),
            (Some(3), Some(24_000)),
            "the objects at rest, from the listing"
        );

        // one second later the two old objects are past it: the orphan is swept, the named one is not
        let now = t0 + WINDOW_MS + 1_000;
        let p = pass(&conn, &mut bucket, now, false, |_| {});
        assert_eq!(
            p,
            Pass {
                swept: vec![key(&orphan)],
                swept_bytes: 8_000,
                named_read: true
            }
        );
        assert!(!bucket.objects.contains_key(&key(&orphan)));
        assert!(
            bucket.objects.contains_key(&key(&held)),
            "a parked letter names it"
        );
        assert!(
            bucket.objects.contains_key(&key(&young)),
            "younger than the window"
        );
        assert!(bucket.objects.contains_key("other/thing"));
        let st = state(&conn);
        assert_eq!(
            (
                st.full_objects,
                st.full_bytes,
                st.full_at,
                st.last_swept,
                st.last_listed
            ),
            (Some(2), Some(16_000), Some(now), 1, 3)
        );
        assert_eq!(
            (bucket.heads, bucket.deletes),
            (1, 1),
            "one read and one delete, the orphan's"
        );

        // the client re-presents the young one's bytes long after: the same key, a NEW stamp, a new message. A
        // pass that listed the old stamp reads the object again before its delete and leaves it.
        let late = t0 + 2 * WINDOW_MS;
        let young_key = key(&young);
        let p = pass(&conn, &mut bucket, late, false, |b| {
            b.put(&young_key, 8_000, late - 1)
        });
        assert_eq!(p.swept, Vec::<String>::new());
        assert!(bucket.objects.contains_key(&young_key));
        // a named-keys read that faults deletes nothing
        let before = bucket.objects.len();
        let p = pass(&conn, &mut bucket, late + 3 * WINDOW_MS, true, |_| {});
        assert!(p.named_read && p.swept.is_empty());
        assert_eq!(bucket.objects.len(), before);
        // the letter row deleted by SQL (an operator's hand): its object is an orphan at the next pass
        conn.execute(
            "DELETE FROM mutation_dead_letters WHERE r2_key = ?1",
            [key(&held)],
        )
        .unwrap();
        let p = pass(&conn, &mut bucket, late + 3 * WINDOW_MS, false, |_| {});
        assert_eq!(
            p.swept.len(),
            2,
            "the unnamed letter's object and the re-presented one, both past the window now"
        );
        assert_eq!(state(&conn).full_objects, Some(0));
    }

    /// The fold: the sweep is BOUNDED per pass and resumes from the key at rest. 450 objects, 130 of them
    /// orphans: a pass lists at most 200 and deletes at most 50, stops at the 51st orphan of its page, and the
    /// next pass goes on from the last key handled (read from D1, not from memory). A round ends when the listing
    /// does; its count is the objects left at rest.
    #[test]
    fn e585_d3f_the_sweep_is_bounded_per_pass_and_resumes_from_the_key_at_rest() {
        let conn = db();
        let mut bucket = Bucket::default();
        let t0 = 1_800_000_000_000i64;
        let now = t0 + WINDOW_MS + 1;
        // keys in listing order; the first 130 are old (orphans), the rest young
        for i in 0..450u64 {
            let at = if i < 130 { t0 } else { now - 1_000 };
            bucket.put(&format!("{SWEEP_PREFIX}{i:064x}/{:032x}", 0), 100 + i, at);
        }
        let mut passes = Vec::new();
        for _ in 0..20 {
            let p = pass(&conn, &mut bucket, now, false, |_| {});
            let st = state(&conn);
            passes.push((p.swept.len(), st.last_listed, st.start_after.is_empty()));
            assert!(p.swept.len() <= SWEEP_MAX_DELETES);
            if st.start_after.is_empty() {
                break;
            }
        }
        assert_eq!(
            passes,
            vec![
                (50, 50, false), // stopped at the 51st orphan of its page
                (50, 50, false),
                (30, 200, false), // the last 30 orphans and 170 young
                (0, 150, true),   // the listing ended: the round is complete
            ]
        );
        assert_eq!(bucket.lists, 4, "one listing a pass");
        assert_eq!((bucket.heads, bucket.deletes), (130, 130));
        let st = state(&conn);
        assert_eq!(st.full_objects, Some(320));
        assert_eq!(st.full_bytes, Some((130..450u64).map(|i| 100 + i).sum()));
        assert_eq!(
            (st.round_objects, st.round_bytes, st.round_started_at),
            (0, 0, None)
        );
        // the next round starts at the first key, and a page with nothing past the window reads no dead letter
        let p = pass(&conn, &mut bucket, now, false, |_| {});
        assert!(!p.named_read && p.swept.is_empty());
        assert_eq!(state(&conn).round_objects, 200);
        assert_eq!(
            state(&conn).full_objects,
            Some(320),
            "the last complete count stands meanwhile"
        );
        // the health block
        let j = queue_json(Some(&state(&conn)), true);
        assert_eq!(j["r2"]["atRest"]["objects"], 320);
        assert_eq!(j["r2"]["round"]["objects"], 200);
        assert_eq!(j["r2"]["sweep"]["windowSecs"], ORPHAN_WINDOW_S);
        assert_eq!(j["r2"]["sweep"]["maxObjectsPerPass"], 200);
        assert_eq!(j["r2"]["sweep"]["maxDeletesPerPass"], 50);
        let never = queue_json(Some(&SweepState::default()), true);
        assert!(never["r2"]["atRest"].is_null() && never["r2"]["sweep"]["lastPassAt"].is_null());
        assert_eq!(queue_json(None, false)["r2"]["readable"], false);
    }

    /// The fold: the window is what no queue message can outlive, and the wiring. Two retentions (the main queue,
    /// then the dead letter queue), each the platform's default four days. #576's dead-letter window alone (about
    /// 48 h of backoff) is under it and is NOT the bound: it starts after the message's life in the main queue.
    #[test]
    fn e585_d3f_the_window_outlives_every_queue_message_and_the_cron_runs_the_pass() {
        assert_eq!(QUEUE_RETENTION_S, 4 * 86_400);
        assert_eq!(ORPHAN_WINDOW_S, 8 * 86_400);
        let dlq_backoff_s: u64 = (1..=dead_letters::DLQ_MAX_RETRIES)
            .map(|a| u64::from(dead_letters::dlq_retry_plan(Some(a)).delay_s))
            .sum();
        assert_eq!(
            dlq_backoff_s, 172_860,
            "#576's window: 48 h and a minute of backoff"
        );
        assert!(
            dlq_backoff_s > 48 * 3600,
            "a letter deferred to its last delivery is older than 48 h before its main-queue life is counted"
        );
        assert!(
            dlq_backoff_s < QUEUE_RETENTION_S,
            "the deferral fits the dead letter queue's retention"
        );
        assert!(QUEUE_RETENTION_S + dlq_backoff_s < ORPHAN_WINDOW_S);
        for cfg in [
            include_str!("../wrangler.toml"),
            include_str!("../wrangler.low.toml"),
        ] {
            assert!(
                !cfg.contains("retention"),
                "no config sets a retention: the default is the one the window is made of"
            );
            assert!(
                cfg.contains("crons = [\"*/15 * * * *\"]"),
                "the sweep's cadence"
            );
        }
        assert_eq!(
            crate::ops::COUNTER_QUEUE_R2_ORPHANS_SWEPT,
            "queue_r2_orphans_swept_total"
        );
        assert_eq!(
            crate::ops::COUNTER_QUEUE_R2_ORPHANS_SWEPT_BYTES,
            "queue_r2_orphans_swept_bytes_total"
        );

        let squash = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<String>()
                .split_whitespace()
                .collect::<String>()
        };
        let lib = include_str!("lib.rs");
        let start = lib.find("async fn scheduled(").unwrap();
        let tick = squash(&lib[start..start + lib[start..].find("\n}\n").unwrap()]);
        let sweep = tick
            .find("crate::beef_blob_sweep::run_pass(&env,&ops_db,\"tick\").await;")
            .expect("the scheduled tick runs one bounded pass of the sweep");
        assert!(
            sweep < tick.find("engine.start_gasp_sync()").unwrap(),
            "before the GASP step"
        );
        let ops = squash(include_str!("ops.rs"));
        assert!(ops.contains("letqueue=crate::beef_blob_sweep::health_json(db,env).await;"));
        assert!(ops.contains("\"queue\":queue,"));
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&SWEEP_STATE_CREATE));

        let me = include_str!("beef_blob_sweep.rs");
        let me = &me[..me.find("#[cfg(test)]").unwrap()];
        let item = |head: &str| {
            let start = me
                .find(head)
                .unwrap_or_else(|| panic!("no `{head}` in the source"));
            squash(&me[start..start + me[start..].find("\n}\n").unwrap()])
        };
        let run = item("pub async fn run_pass(");
        assert!(
            run.contains("run_pass_with(&WorkerSweep{bucket:&bucket,db,},crate::broadcaster::sleep_ms(SWEEP_BUDGET_MS),)"),
            "the pass runs under its budget"
        );
        assert!(
            run.contains("PassOutcome::stopped(format!(\"no{}binding\",crate::queue::BEEF_BLOBS_BINDING),None,)"),
            "no binding: nothing is listed"
        );
        let with = item("pub(crate) async fn run_pass_with<");
        assert!(with.contains(
            "overlay_engine::gasp::race_or_deadline(sweep_pass(p,&progress),deadline).await.unwrap_or_else(||progress.dropped());"
        ));
        let f = item("pub(crate) async fn sweep_pass<");
        assert!(f.contains("p.list_page(&state.start_after).await"));
        let page = squash(
            &me[me.find("async fn list_page(").unwrap()..me.find("async fn head_of(").unwrap()],
        );
        assert!(page.contains("set(&opts,\"prefix\",&JsValue::from_str(SWEEP_PREFIX))?;"));
        assert!(page.contains("set(&opts,\"limit\",&JsValue::from(SWEEP_MAX_OBJECTS))?;"));
        assert!(page.contains("set(&opts,\"startAfter\",&JsValue::from_str(start_after))?;"));
        assert!(f.contains("ifany_past_window(listed,now,ORPHAN_WINDOW_S)"));
        assert!(f.contains("plan_pass(listed,&named,now,ORPHAN_WINDOW_S,SWEEP_MAX_DELETES)"));
        assert!(f.contains("after_pass(&state,&page,&plan,&swept,truncated,now)"));
        let (named, head, check, delete, save) = (
            f.find("p.named_keys().await").unwrap(),
            f.find("p.head(&o.key).await").unwrap(),
            f.find("if!still_orphan(o,head.as_ref()){continue;}")
                .unwrap(),
            f.find("p.delete(&o.key).await").unwrap(),
            f.find("p.save(&next).await").unwrap(),
        );
        assert!(named < head && head < check && check < delete && delete < save);
        assert_eq!(f.matches(".delete(").count(), 1);
        assert_eq!(
            f[..delete].matches("returnPassOutcome::stopped(").count(),
            3,
            "a state read, a listing or a named-keys read that faults: nothing is deleted"
        );
        // the worker's port is the shipped statements and R2 calls
        let port = squash(
            &me[me.find("impl SweepPort for WorkerSweep<'_> {").unwrap()
                ..me.find("pub(crate) async fn sweep_pass<").unwrap()],
        );
        for call in [
            "read_state(self.db).await",
            "list_page(self.bucket,start_after).await",
            "Query::new(NAMED_KEYS_SQL).fetch_all::<Named>(self.db)",
            "head_of(self.bucket,key).await",
            "self.bucket.delete(key).await",
            "save_query(next).execute(self.db).await",
            "crate::ops::bump_counter(self.db,counter,delta).await;",
        ] {
            assert!(port.contains(call), "the worker's port: {call}");
        }
    }

    /// The d3 fold-2 (the lens's L3 and N2 (b)): the sweep's age is the LATER of R2's `uploaded` and the writer's
    /// own `customMetadata.touched`, which every put writes. If the platform does NOT renew `uploaded` when a key
    /// is written again, a re-presentation after day 8 left the object past the window under a live message: swept
    /// within one round. With the stamp, it is young; and an object re-stamped between the listing and its delete is
    /// left. The list asks for the custom metadata, and the put writes it. RED with the age read from `uploaded`
    /// alone (`bc32851`'s rule): the re-put object is past the window.
    #[test]
    fn e585_d3f2_l3_the_age_is_the_later_of_uploaded_and_touched() {
        let day = 86_400_000i64;
        let now = 20 * day;
        let reput = Listed {
            key: format!("{SWEEP_PREFIX}aa/bb"),
            bytes: 9,
            uploaded_ms: now - 10 * day,
            touched_ms: Some(now - day),
        };
        assert_eq!(reput.age_ms(), now - day);
        assert!(
            !past_window(&reput, now, ORPHAN_WINDOW_S),
            "re-put yesterday: young"
        );
        let named = HashSet::new();
        assert!(plan_pass(
            std::slice::from_ref(&reput),
            &named,
            now,
            ORPHAN_WINDOW_S,
            50
        )
        .orphans
        .is_empty());
        let old = Listed {
            touched_ms: Some(now - 10 * day),
            ..reput.clone()
        };
        assert!(past_window(&old, now, ORPHAN_WINDOW_S));
        let unstamped = Listed {
            touched_ms: None,
            ..old.clone()
        };
        assert!(
            past_window(&unstamped, now, ORPHAN_WINDOW_S),
            "an object from before the stamp: uploaded"
        );
        let stale_touch = Listed {
            touched_ms: Some(now - 20 * day),
            ..old.clone()
        };
        assert_eq!(
            stale_touch.age_ms(),
            old.uploaded_ms,
            "the later of the two"
        );
        // re-stamped between the listing and the delete, with `uploaded` unchanged: left
        assert!(still_orphan(&old, Some(&old)));
        assert!(!still_orphan(&old, Some(&reput)));
        // the put writes the stamp; the list reads it
        assert_eq!(
            queue::touched_meta(1_234),
            std::collections::HashMap::from([(
                queue::TOUCHED_META.to_string(),
                "1234".to_string()
            )])
        );
        let code = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let q = code(include_str!("queue.rs"));
        let start = q.find("async fn put_beef(").unwrap();
        let f = &q[start..start + q[start..].find("\n}\n").unwrap()];
        assert!(f.contains(".custom_metadata(touched_meta(now_ms))"));
        let src = code(include_str!("beef_blob_sweep.rs"));
        let src = &src[..src.find("#[cfg(test)]").unwrap()];
        assert!(src.contains("include.push(&JsValue::from_str(\"customMetadata\"));"));
        assert!(src.contains("touched_ms: touched_ms_of(touched_field(o).as_deref()),"));
    }

    /// E585-D3-DELTA-M1, the d3 fold-3: an object with NO custom metadata (an operator's CLI put, the route cell's
    /// leg 7, anything written before the stamp) never throws in the sweep and is aged by `uploaded` alone.
    ///
    /// What this proves natively: the stamp's parse ([`touched_ms_of`]) over an absent field, an absent
    /// `customMetadata`, an empty and a malformed value; that age falls back to `uploaded`; and, by the source,
    /// that the sweep reads no metadata through workers-rs (`custom_metadata()`, whose `js_sys::Object::keys` over
    /// `undefined` threw through the wasm frame) and reads it only behind `is_object()` through `Reflect`, a
    /// `catch` binding. What it cannot: `js_sys` does not run off wasm32, so no native test executes the guard,
    /// and the platform's actual answer for an unstamped object (`undefined` or `{}`) is beta's to show (the
    /// fold-3 REPORT's check). RED on `4f592f0` (with `touched_ms_of` grafted into its `beef_blob_sweep.rs`): "the
    /// sweep reads customMetadata through workers-rs's Object::keys".
    #[test]
    fn e585_d3f3_m1_an_unstamped_object_never_throws_and_ages_by_uploaded() {
        assert_eq!(
            touched_ms_of(None),
            None,
            "no customMetadata, or no touched field"
        );
        assert_eq!(touched_ms_of(Some("")), None);
        assert_eq!(touched_ms_of(Some("yesterday")), None);
        assert_eq!(touched_ms_of(Some("1.5e12")), None);
        assert_eq!(touched_ms_of(Some("1234")), Some(1234));
        let day = 86_400_000i64;
        let now = 20 * day;
        let unstamped = Listed {
            key: format!("{SWEEP_PREFIX}aa/bb"),
            bytes: 9,
            uploaded_ms: now - 10 * day,
            touched_ms: touched_ms_of(None),
        };
        assert_eq!(unstamped.age_ms(), unstamped.uploaded_ms);
        assert_eq!(
            plan_pass(
                std::slice::from_ref(&unstamped),
                &HashSet::new(),
                now,
                ORPHAN_WINDOW_S,
                50
            )
            .orphans,
            vec![0],
            "an unstamped orphan past the window by `uploaded` is swept, not a throw"
        );

        let code = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let src = code(include_str!("beef_blob_sweep.rs"));
        let src = &src[..src.find("#[cfg(test)]").unwrap()];
        assert!(
            !src.contains("custom_metadata()") && !src.contains("Object::keys"),
            "the sweep reads customMetadata through workers-rs's Object::keys"
        );
        let f = &src[src.find("fn touched_field(").unwrap()..src.find("fn listed_of_js(").unwrap()];
        let (read, guard, touched) = (
            f.find("Reflect::get(o, &JsValue::from_str(\"customMetadata\")).ok()?")
                .unwrap(),
            f.find("if !meta.is_object() {\n        return None;\n    }")
                .expect("the guard"),
            f.find("Reflect::get(&meta, &JsValue::from_str(crate::queue::TOUCHED_META))")
                .unwrap(),
        );
        assert!(read < guard && guard < touched);
        assert!(
            f.contains(".as_string()"),
            "a stamp that is not a string is no stamp"
        );
        // every R2 object the sweep reads goes through the guarded reader, the list's and the head's
        assert_eq!(src.matches("listed_of_js(&o)").count(), 2);
        assert!(!src.contains("bucket.head(") && !src.contains(".list()"));
    }

    fn squash_src(s: &str) -> String {
        s.lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<String>()
            .split_whitespace()
            .collect::<String>()
    }

    fn item(src: &str, head: &str) -> String {
        let start = src
            .find(head)
            .unwrap_or_else(|| panic!("no `{head}` in the source"));
        squash_src(&src[start..start + src[start..].find("\n}\n").unwrap()])
    }

    /// The d3 fold-4 (the door 3 delta-2 lens, E585-D3-DELTA2-L1): `POST /internal/beef-blob-sweep` runs ONE pass
    /// through `run_pass`, the very function the scheduled tick calls, so the route tier tests the shipped pass
    /// without firing the whole production tick (its live peers, WhatsOnChain, the broadcasters). The router sends
    /// the route to the lever; the lever checks the bearer first, refuses a body that is not empty or `{}`, and
    /// runs `run_pass` with no listing, plan or delete of its own; the tick runs `run_pass` too. RED on `1d1f7fe`:
    /// "the router sends POST /internal/beef-blob-sweep to the lever"; RED against a lever that runs its own
    /// listing (`list_page(` in the handler): "the lever runs the tick's pass, never its own listing".
    #[test]
    fn e585_d3f4_l1_the_lever_runs_the_scheduled_pass_function() {
        let lib = include_str!("lib.rs");
        assert!(
            squash_src(lib).contains(
                "(Method::Post,\"/internal/beef-blob-sweep\")=>crate::beef_blob_sweep::internal_sweep(req,&env).await,"
            ),
            "the router sends POST /internal/beef-blob-sweep to the lever"
        );
        let tick = item(lib, "async fn scheduled(");
        assert_eq!(
            tick.matches("crate::beef_blob_sweep::run_pass(&env,&ops_db,\"tick\").await;")
                .count(),
            1,
            "the tick runs the same function"
        );
        assert!(
            !tick.contains("sweep_pass("),
            "the tick calls the pass only through run_pass"
        );
        let me = include_str!("beef_blob_sweep.rs");
        let me = &me[..me.find("#[cfg(test)]").unwrap()];
        let h = item(me, "pub async fn internal_sweep(");
        let (auth, body, run) = (
            h.find("if!crate::tip_pass::bearer_ok(authorization.as_deref(),secret.as_deref()){")
                .expect("the bearer, compared in fixed time"),
            h.find("if!parse_sweep_request(&raw){").expect("the body"),
            h.find("letout=run_pass(env,&db,\"lever\").await;")
                .expect("the lever runs the tick's pass"),
        );
        assert!(auth < body && body < run);
        assert!(h.contains("returnworker::Response::error(\"unauthorized\",401);"));
        for own in [
            "list_page(",
            "sweep_pass(",
            "plan_pass(",
            ".delete(",
            "save_query(",
        ] {
            assert!(
                !h.contains(own),
                "the lever runs the tick's pass, never its own listing ({own})"
            );
        }
        assert!(h.contains("Response::from_json(&pass_json(&out))?.with_status(status)"));
        // exactly two callers of the pass: the tick and the lever
        let workers: usize = [lib, me]
            .iter()
            .map(|f| squash_src(f).matches("run_pass(").count())
            .sum();
        assert_eq!(workers, 3, "the definition, the tick and the lever");

        // the body: empty or {}, nothing else
        for ok in ["", "  \n", "{}", " { } "] {
            assert!(parse_sweep_request(ok.as_bytes()), "{ok:?}");
        }
        for bad in ["{\"limit\": 5}", "[]", "null", "x", "{"] {
            assert!(!parse_sweep_request(bad.as_bytes()), "{bad:?}");
        }
        // the answer names the pass
        let ran = PassOutcome {
            listed: 7,
            handled: 7,
            unreadable: 1,
            unreadable_key: Some("mutations/zz".into()),
            swept: 2,
            swept_bytes: 300,
            faults: 1,
            cursor_before: Some(String::new()),
            cursor_after: Some("mutations/ab".into()),
            last_pass_at: Some(42),
            ..PassOutcome::default()
        };
        let j = pass_json(&ran);
        for (k, v) in [
            ("ok", serde_json::json!(true)),
            ("stopped", serde_json::json!(null)),
            ("listed", serde_json::json!(7)),
            ("unreadable", serde_json::json!(1)),
            ("unreadableKey", serde_json::json!("mutations/zz")),
            ("deleted", serde_json::json!(2)),
            ("deletedBytes", serde_json::json!(300)),
            ("faults", serde_json::json!(1)),
            ("cursorBefore", serde_json::json!("")),
            ("cursorAfter", serde_json::json!("mutations/ab")),
            ("lastPassAt", serde_json::json!(42)),
            ("roundComplete", serde_json::json!(false)),
        ] {
            assert_eq!(j[k], v, "{k}");
        }
        assert_eq!(j["budget"]["maxObjectsPerPass"], SWEEP_MAX_OBJECTS);
        assert_eq!(j["budget"]["maxDeletesPerPass"], SWEEP_MAX_DELETES);
        let stopped = pass_json(&PassOutcome::stopped(
            "the listing faulted: x".into(),
            Some("k".into()),
        ));
        assert_eq!(stopped["ok"], false);
        assert_eq!(
            stopped["cursorAfter"],
            serde_json::Value::Null,
            "no cursor moved"
        );
        assert_eq!(stopped["lastPassAt"], serde_json::Value::Null);
        assert_eq!(
            queue_json(None, true)["r2"]["sweep"]["lever"],
            "POST /internal/beef-blob-sweep"
        );
    }

    /// The d3 fold-4 (the delta-2 lens's N3): a listed object whose key or `uploaded` date does not read is
    /// SKIPPED and COUNTED: the pass sweeps the orphans on both sides of it, the cursor passes it (by its key, even
    /// as the last entry of a truncated page), the next round meets it again, and the state at rest names it
    /// (`lastUnreadable`, `lastUnreadableKey`) for the health block. RED on `1d1f7fe`: "one unreadable object
    /// fails the whole page" (its `list_page` collects `Option<Vec<_>>`, so the sweep stalled at the object on every
    /// pass, nothing counted).
    #[test]
    fn e585_d3f4_n3_an_unreadable_object_is_skipped_counted_and_passed() {
        let me = include_str!("beef_blob_sweep.rs");
        let me = &me[..me.find("#[cfg(test)]").unwrap()];
        let page = squash_src(
            &me[me.find("async fn list_page(").unwrap()..me.find("async fn head_of(").unwrap()],
        );
        assert!(
            !page.contains("collect::<Option<Vec<_>>>()"),
            "one unreadable object fails the whole page"
        );
        assert!(page.contains("None=>Entry::Unreadable("));
        let f = item(me, "pub(crate) async fn sweep_pass<");
        assert!(f.contains("letpage=split_page(entries);"));
        assert!(item(me, "pub(crate) async fn run_pass_with<")
            .contains("COUNTER_QUEUE_R2_ORPHAN_SWEEP_UNREADABLE,out.unreadable,"));
        assert_eq!(
            crate::ops::COUNTER_QUEUE_R2_ORPHAN_SWEEP_UNREADABLE,
            "queue_r2_orphan_sweep_unreadable_total"
        );
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&SWEEP_STATE_UNREADABLE_COLUMN));
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&SWEEP_STATE_UNREADABLE_KEY_COLUMN));

        // the SHIPPED pass (`sweep_pass`, through `run_pass_with`) over the model port and the shipped statements
        // (the d3 fold-5, E585-D3-DELTA3-L2: it ran a model pass, and a shipped pass that stopped on an
        // unreadable entry passed every pin)
        let conn = db();
        let mut bucket = Bucket::default();
        let t0 = 1_800_000_000_000i64;
        let now = t0 + WINDOW_MS + 1;
        let k = |i: u32| format!("{SWEEP_PREFIX}{i:064x}/{:032x}", 0);
        for i in 0..5 {
            bucket.put(&k(i), 100, t0);
        }
        bucket.broken.insert(k(1));
        bucket.broken.insert(format!("{KEYLESS}{}", k(3)));
        let p = pass(&conn, &mut bucket, now, false, |_| {});
        assert_eq!(
            p.swept,
            vec![k(0), k(2), k(4)],
            "the orphans on both sides are swept"
        );
        let st = state(&conn);
        assert_eq!(
            (st.last_unreadable, st.last_unreadable_key.as_deref()),
            (2, Some(k(1).as_str()))
        );
        assert!(st.start_after.is_empty(), "the round is complete");
        assert_eq!(
            st.full_objects,
            Some(0),
            "an unreadable object is not counted at rest"
        );
        assert!(
            bucket.objects.contains_key(&k(1)) && bucket.objects.contains_key(&k(3)),
            "never swept"
        );
        let h = queue_json(Some(&st), true);
        assert_eq!(h["r2"]["sweep"]["lastUnreadable"], 2);
        assert_eq!(h["r2"]["sweep"]["lastUnreadableKey"], k(1));
        // the next round meets it again, counted again
        pass(&conn, &mut bucket, now, false, |_| {});
        assert_eq!(state(&conn).last_unreadable, 2);
        // a clean pass clears the name
        bucket.broken.clear();
        pass(&conn, &mut bucket, now, false, |_| {});
        let st = state(&conn);
        assert_eq!((st.last_unreadable, st.last_unreadable_key), (0, None));

        // a truncated page whose LAST entry does not read: the cursor passes it by its key
        let mut entries: Vec<Entry> = (0..3)
            .map(|i| {
                Entry::Object(Listed {
                    key: k(10 + i),
                    bytes: 1,
                    uploaded_ms: now,
                    touched_ms: None,
                })
            })
            .collect();
        entries.push(Entry::Unreadable(Some(k(20))));
        let page = split_page(entries);
        let plan = plan_pass(
            &page.listed,
            &HashSet::new(),
            now,
            ORPHAN_WINDOW_S,
            SWEEP_MAX_DELETES,
        );
        let next = after_pass(
            &SweepState::default(),
            &page,
            &plan,
            &HashSet::new(),
            true,
            now,
        );
        assert_eq!(next.start_after, k(20));
        assert_eq!(next.round_objects, 3);
        // a page with no key that reads stays where it was (the stated limit), counted
        let page = split_page(vec![Entry::Unreadable(None); 2]);
        let plan = plan_pass(
            &page.listed,
            &HashSet::new(),
            now,
            ORPHAN_WINDOW_S,
            SWEEP_MAX_DELETES,
        );
        let at = SweepState {
            start_after: k(30),
            ..SweepState::default()
        };
        let next = after_pass(&at, &page, &plan, &HashSet::new(), true, now);
        assert_eq!(
            (
                next.start_after,
                next.last_unreadable,
                next.last_unreadable_key
            ),
            (k(30), 2, None)
        );
    }

    /// The d3 fold-5 (the door 3 delta-3 lens, E585-D3-DELTA3-L1): a pass the budget DROPS answers and counts the
    /// deletes it made before the drop. Four orphans past the window; the pass deletes two, then its third `head`
    /// never answers and the deadline drops it. The answer is `deleted: 2` with their bytes, `cursorAfter: null`,
    /// a `stopped` that says no cursor was saved, and the counters moved by two; the state at rest is untouched. A
    /// pass dropped in its SAVE says the save was in flight, with all four deletes counted. A pass that runs counts
    /// once, as before. RED on `14f4b2c`'s rule (its `run_pass` answer on a drop, `PassOutcome::stopped(".. dropped,
    /// no cursor saved", None)`, grafted into `run_pass_with`, its counters bumped inside the pass after the save):
    /// `deleted` 0 and the counters unmoved.
    #[test]
    fn e585_d3f5_l1_a_pass_the_budget_drops_answers_and_counts_its_deletes() {
        use crate::ops::{
            COUNTER_QUEUE_R2_ORPHANS_SWEPT as SWEPT, COUNTER_QUEUE_R2_ORPHANS_SWEPT_BYTES as BYTES,
        };
        let conn = db();
        let t0 = 1_800_000_000_000i64;
        let now = t0 + WINDOW_MS + 1;
        let k = |i: u64| format!("{SWEEP_PREFIX}{i:064x}/{:032x}", 0);
        let orphans = || {
            let mut b = Bucket::default();
            for i in 0..4 {
                b.put(&k(i), 100 + i, t0);
            }
            b
        };

        // dropped at the third head: two deletes made
        let mut m = Model::new(&conn, orphans(), now);
        m.hang_after_deletes = Some(2);
        let out = block_on(run_pass_with(&m, std::future::ready(())));
        let j = pass_json(&out);
        assert_eq!(
            (&j["deleted"], &j["deletedBytes"]),
            (&serde_json::json!(2), &serde_json::json!(201)),
            "the deletes made before the drop: {j}"
        );
        assert_eq!(j["ok"], false);
        assert_eq!(j["cursorBefore"], "");
        assert_eq!(j["cursorAfter"], serde_json::Value::Null);
        assert_eq!(j["lastPassAt"], serde_json::Value::Null);
        let why = out.stopped.clone().unwrap();
        assert!(
            why.contains("EXCEEDED")
                && why.contains("dropped after 2 deletes of 201 bytes")
                && why.contains("no cursor saved"),
            "{why}"
        );
        assert_eq!(
            (m.counter(SWEPT), m.counter(BYTES)),
            (2, 201),
            "the counters carry the dropped pass's deletes"
        );
        assert_eq!(m.bucket.borrow().objects.len(), 2, "the deletes were real");
        assert_eq!(state(&conn), SweepState::default(), "no save was started");

        // dropped in its save: four deletes, the save in flight
        let mut m = Model::new(&conn, orphans(), now);
        m.hang_on_save = true;
        let out = block_on(run_pass_with(&m, std::future::ready(())));
        assert_eq!((out.swept, out.swept_bytes), (4, 406));
        assert!(
            out.stopped
                .as_deref()
                .unwrap()
                .contains("IN FLIGHT and may have landed"),
            "{:?}",
            out.stopped
        );
        assert_eq!(out.cursor_after, None);
        assert_eq!((m.counter(SWEPT), m.counter(BYTES)), (4, 406));

        // a pass that runs is counted once, with its cursor
        let m = Model::new(&conn, orphans(), now);
        let out = block_on(run_pass_with(&m, std::future::ready(())));
        assert_eq!(out.stopped, None);
        assert_eq!((out.swept, m.counter(SWEPT), m.counter(BYTES)), (4, 4, 406));
        assert_eq!(out.cursor_after.as_deref(), Some(""));
        assert!(out.round_complete);
        // a pass stopped before its plan counts nothing and keeps its old words
        let mut m = Model::new(&conn, orphans(), now);
        m.named_faults = true;
        let out = block_on(run_pass_with(&m, std::future::ready(())));
        assert!(out
            .stopped
            .as_deref()
            .unwrap()
            .starts_with("the dead letters' keys did not read"));
        assert_eq!((out.swept, m.counter(SWEPT)), (0, 0));
        assert_eq!(m.bucket.borrow().objects.len(), 4);
    }
}
