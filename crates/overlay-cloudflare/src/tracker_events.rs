//! Bounded first-party write events; one resend on the next foreground flush.
use serde_json::{json, Value};
use std::{cell::RefCell, collections::BTreeMap};
thread_local! {
    static EVENTS: RefCell<BTreeMap<(String,String),Value>> = const {RefCell::new(BTreeMap::new())};
    static COUNTS: RefCell<BTreeMap<String,u64>> = const {RefCell::new(BTreeMap::new())};
    static PENDING: RefCell<BTreeMap<String,u64>> = const {RefCell::new(BTreeMap::new())};
}
fn source(frame: &Value) -> &'static str {
    match frame["kind"].as_str() {
        Some("proof") => "index",
        Some("submit") => "door",
        _ => "arcade",
    }
}
fn count(frame: &Value, outcome: &str) {
    let name = format!("tracker_events_{}_{outcome}_total", source(frame));
    COUNTS.with(|c| *c.borrow_mut().entry(name.clone()).or_default() += 1);
    PENDING.with(|c| *c.borrow_mut().entry(name).or_default() += 1);
}
fn enqueue(frame: Value) {
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        let key = (
            frame["txid"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase(),
            frame["kind"].as_str().unwrap_or_default().to_string(),
        );
        if events.len() >= 128 && !events.contains_key(&key) {
            count(&frame, "dropped");
            return;
        }
        events.entry(key).or_insert(frame);
    });
}
/// Queue a write event of `kind` for `txid` (128 held at most; a further key is dropped and counted).
pub fn note(txid: &str, kind: &str, body: Option<Value>) {
    enqueue(
        json!({"txid":txid.to_ascii_lowercase(),"kind":kind,"body":body.unwrap_or(Value::Null)}),
    );
}
/// bsv-low #660 (B-OWED-3-FOLD, the lens's M1): the `seen` frame of a `network_seen` latch, its body exactly the
/// latch's own (`ops::latch_network_seen`): the broadcaster that said SEEN and its stamp in ms.
pub fn seen_body(source: &str, at_ms: i64) -> Value {
    json!({"source": source, "at": at_ms})
}

/// bsv-low #660 (B-OWED-3-FOLD M1): RE-NOTE a latch that beat the engine's mark. The dual push and the pending
/// watch are spawned before the engine write and run beside it, so a settle's `seen` can be latched, shipped and
/// DROPPED as unheld at the app layer (no `pot_records.spendingTxid` names it yet) before `mark_spent` lands; nothing
/// re-sent it, and the winner's owed row read `built` (Withheld) until the block. The submit route calls this after
/// the engine write, for a consuming submit, with the subject's latched pair (`ops::NETWORK_SEEN_PAIR_SQL`): the
/// request's first flush then ships the `seen` before the pot note the mark made, and the hold now exists. A row
/// latched before #621 carries no source and is not re-noted (the latch's body needs one). Answers whether it noted.
pub fn note_latched_seen(txid: &str, latched: Option<(&str, i64)>) -> bool {
    match latched {
        Some((source, at_ms)) => {
            note(txid, "seen", Some(seen_body(source, at_ms)));
            true
        }
        None => false,
    }
}
/// Submit hook admission boundary, shared by the route and native pin.
pub fn note_submit(txid: &str, gated: bool, admitted_any: bool) {
    if gated && admitted_any {
        note(txid, "submit", None);
    }
}

/// bsv-low #757: the one live-hold predicate of the word events, here and at the app layer's `/internal/tx-word`
/// (`tracker_host::runtime::live_hold`; `stored::DUE` and `stored::SWEEP` read the same two tables): the engine holds
/// the subject (`transactions`), or the pot index records it as a pot or hop coin's spender (`pot_records.spendingTxid`:
/// the admitted submit that consumed the coin, a JOIN spending its hops, `discover_missing_spends`' courier-found
/// spender of that outpoint, a hop sweep's included, or #605's mined competitor; a pot evicted to its shadow table, and
/// an evicted spender whose unconfirmed pointer the eviction released, hold nothing). The sweep and the scheduled
/// refresh spare a recorded pot or hop spender only while its word is not yet mined or terminal, so the pointer buys
/// no permanent word row. A settle, refund or sweep admits no output, so it never has a
/// `transactions` row; held by `transactions` alone its `seen`, `proof` and `callback` events were dropped and its
/// word stayed `built`. Its retained bytes (`pot_beefs`) stay no authority.
pub const LIVE_HOLD_SQL: &str = "SELECT 1 AS held FROM transactions WHERE txid = lower(?1) UNION ALL SELECT 1 AS held FROM pot_records WHERE spendingTxid = lower(?1) LIMIT 1";

/// The public txid bearer is a trigger, not authority for a word row.
pub async fn note_held(
    db: Option<&worker::D1Database>,
    txid: &str,
    kind: &str,
    body: Option<Value>,
) {
    #[derive(serde::Deserialize)]
    struct Held {
        held: f64,
    }
    let Some(db) = db else {
        return;
    };
    let held = crate::d1::Query::new(LIVE_HOLD_SQL)
        .bind(txid)
        .fetch_optional::<Held>(db)
        .await;
    if held.is_ok_and(|row| row.is_some_and(|r| r.held == 1.0)) {
        note(txid, kind, body);
    }
}

/// bsv-low #660 (B-OWED-3-FOLD, the lens's L1): a COPY of every queued frame under its key, the set left as it is
/// (a background flush's skip parks the copy beside the pot and hop notes and [`forget`]s only what its park landed).
pub fn snapshot() -> Vec<((String, String), Value)> {
    EVENTS.with(|e| {
        e.borrow()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    })
}

/// B-OWED-3-FOLD L1: take `parked` out of the queue, each ONLY while it holds the frame the snapshot read (a frame of
/// the same key queued since stays; the queue keeps the first frame of a key, so in practice that is a retry flag).
pub fn forget(parked: &[((String, String), Value)]) {
    EVENTS.with(|e| {
        let mut events = e.borrow_mut();
        for (key, frame) in parked {
            if events.get(key) == Some(frame) {
                events.remove(key);
            }
        }
    });
}

/// Take every queued event.
pub fn drain() -> Vec<Value> {
    EVENTS.with(|e| std::mem::take(&mut *e.borrow_mut()).into_values().collect())
}
fn failed(mut frame: Value) {
    if frame["retry"] == true {
        count(&frame, "dropped");
    } else {
        frame["retry"] = json!(true);
        enqueue(frame);
    }
}
/// PURE (bsv-low #660, B-OWED-3-FOLD L2): whether `/internal/tx-word`'s 2xx answer says the frame was HELD (written).
/// An app layer before the fold answers `{"ok":true}` alone: held, as it was counted.
pub fn answered_held(answer: &str) -> bool {
    serde_json::from_str::<Value>(answer)
        .ok()
        .and_then(|v| v.get("held").and_then(Value::as_bool))
        .unwrap_or(true)
}

/// The production POST boundary, also used by the refusal/recovery pin. The post answers `Ok(held)`: a frame the
/// app layer held is counted `shipped`; one it dropped as unheld (B-OWED-3-FOLD L2) `unheld`, never `shipped`; a
/// refused POST is retried once.
pub async fn deliver<F, Fut>(frame: Value, post: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<bool, String>>,
{
    let result = post(frame.to_string()).await;
    record_delivery(frame, result);
}

fn record_delivery(frame: Value, result: Result<bool, String>) {
    match result {
        Ok(true) => count(&frame, "shipped"),
        Ok(false) => count(&frame, "unheld"),
        Err(_) => failed(frame),
    }
}
/// The event counters, for the health block.
pub fn health_json() -> Value {
    COUNTS.with(|c| json!(*c.borrow()))
}
/// bsv-low #660 (B-OWED-3-FOLD, the lens's L3): the words' reserved SLICE of a flush's one wall. The words go first
/// (a note triggers the owed recompute, which reads the spender's word), but a word flood (the tip pass, the cron, a
/// queue batch: N `proof` frames beside N pot notes at a block) took the whole wall and deferred EVERY note of the
/// flush to the isolate's next one. So the words start under `min(deadline, start + WORDS_SHARE_MS)`, then the hop
/// and pot notes under the deadline, then the words left under the deadline (a frame the wall stops keeps its once
/// retry). Stated: a frame past the slice reaches the app layer after the notes of its flush.
pub const WORDS_SHARE_MS: u64 = 10_000;

/// What one ordered flush did with its word frames ([`ship_in_order`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WordsShipped {
    /// Frames POSTed before the notes (inside the slice).
    pub before_notes: usize,
    /// Frames POSTed after the notes (past the slice, before the deadline).
    pub after_notes: usize,
    /// Frames the deadline stopped, counted `walled` without consuming a delivery retry.
    pub walled: usize,
}

async fn post_frames<P, Fut, C, S, Sleep>(
    frames: Vec<Value>,
    post: &mut P,
    now_ms: &C,
    until_ms: u64,
    last: bool,
    sleep: &S,
) -> (usize, Vec<Value>)
where
    P: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<bool, String>>,
    C: Fn() -> u64,
    S: Fn(u64) -> Sleep,
    Sleep: std::future::Future<Output = ()>,
{
    let (mut posted, mut left) = (0, Vec::new());
    for frame in frames {
        if now_ms() >= until_ms {
            if last {
                count(&frame, "walled");
                enqueue(frame.clone());
            }
            left.push(frame);
            continue;
        }
        posted += 1;
        match crate::race_or_deadline(
            post(frame.to_string()),
            sleep(until_ms.saturating_sub(now_ms())),
        )
        .await
        {
            Some(result) => record_delivery(frame, result),
            None => {
                if last {
                    count(&frame, "walled");
                    enqueue(frame.clone());
                }
                left.push(frame);
            }
        }
    }
    (posted, left)
}

/// THE ONE ORDER of a flush (bsv-low #660 under #618; B-OWED-3-FOLD L3 and L4), over any transport: the word frames,
/// each POST awaited, under the words' slice ([`WORDS_SHARE_MS`]); then `notes(deadline_ms)` (the hop and pot sets,
/// which start no POST at or past the deadline); then the frames left under the deadline. The app layer writes a
/// frame before it answers, so a note shipped after a word's answer triggers a recompute that reads that word.
pub async fn ship_in_order<P, Fut, N, NFut, C>(
    words: Vec<Value>,
    post: P,
    notes: N,
    now_ms: C,
    deadline_ms: u64,
) -> WordsShipped
where
    P: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<bool, String>>,
    N: FnOnce(u64) -> NFut,
    NFut: std::future::Future<Output = ()>,
    C: Fn() -> u64,
{
    ship_in_order_with(
        words,
        post,
        notes,
        now_ms,
        deadline_ms,
        crate::broadcaster::sleep_ms,
    )
    .await
    .0
}

pub(crate) async fn ship_in_order_with<P, Fut, N, NFut, C, S, Sleep>(
    words: Vec<Value>,
    mut post: P,
    notes: N,
    now_ms: C,
    deadline_ms: u64,
    sleep: S,
) -> (WordsShipped, Vec<Value>)
where
    P: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<bool, String>>,
    N: FnOnce(u64) -> NFut,
    NFut: std::future::Future<Output = ()>,
    C: Fn() -> u64,
    S: Fn(u64) -> Sleep,
    Sleep: std::future::Future<Output = ()>,
{
    let slice_ms = deadline_ms.min(now_ms().saturating_add(WORDS_SHARE_MS));
    let (before_notes, left) =
        post_frames(words, &mut post, &now_ms, slice_ms, false, &sleep).await;
    notes(deadline_ms).await;
    let (after_notes, walled) =
        post_frames(left, &mut post, &now_ms, deadline_ms, true, &sleep).await;
    let summary = WordsShipped {
        before_notes,
        after_notes,
        walled: walled.len(),
    };
    (summary, walled)
}

/// Retain timed-out word frames in the existing outbox as well as the isolate queue.
pub(crate) async fn park_walled<P: crate::change_park::ParkPort>(
    port: &P,
    frames: Vec<Value>,
    now_ms: u64,
) {
    crate::change_park::park_noted_back(
        port,
        crate::change_park::KIND_WORD,
        frames.into_iter().map(|frame| frame.to_string()).collect(),
        crate::change_park::COUNTER_CHANGE_PARKED,
        now_ms,
    )
    .await;
}

/// [`ship_in_order`] on the Worker: the frames to the app layer's `/internal/tx-word` (each answer read for `held`,
/// L2), then `notes`, under the flush's ONE deadline (bsv-low #649: never a 20 s of the words' own on top). An
/// unconfigured deploy drops the frames (counted) and still runs the notes (which log their own refusal).
pub async fn ship_then<N, NFut>(env: &worker::Env, events: Vec<Value>, notes: N, deadline_ms: u64)
where
    N: FnOnce(u64) -> NFut,
    NFut: std::future::Future<Output = ()>,
{
    let configured = crate::change_flush::configured(env);
    let words = match configured {
        Some(_) => events,
        None => {
            for frame in &events {
                count(frame, "dropped");
            }
            Vec::new()
        }
    };
    let (url, token) = configured.unwrap_or_default();
    let (url, token) = (url.as_str(), token.as_str());
    let (_, walled) = ship_in_order_with(
        words,
        |body| async move {
            crate::change_flush::post_body_answer(
                env,
                url,
                token,
                "/internal/tx-word",
                "tx-word",
                body,
            )
            .await
            .map(|answer| answered_held(&answer))
            .map_err(|_| "undelivered".to_string())
        },
        notes,
        || worker::Date::now().as_millis(),
        deadline_ms,
        crate::broadcaster::sleep_ms,
    )
    .await;
    let pending = PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
    if let Ok(db) = env.d1("OVERLAY_DB") {
        for (name, n) in pending {
            crate::ops::bump_counter(&db, &name, n).await;
        }
        park_walled(
            &crate::change_park::WorkerPark { db: &db },
            walled,
            worker::Date::now().as_millis(),
        )
        .await;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn d2_649_hung_word_yields_to_notes_and_is_counted_at_the_wall() {
        use std::cell::Cell;
        drain();
        let clock = Cell::new(0);
        let notes_at = Cell::new(None);
        let before = health_json()["tracker_events_arcade_walled_total"]
            .as_u64()
            .unwrap_or(0);
        let work = ship_in_order_with(
            vec![json!({"txid": "ab".repeat(32), "kind": "seen", "retry": true})],
            |_| std::future::pending::<Result<bool, String>>(),
            |_| async {
                notes_at.set(Some(clock.get()));
            },
            || clock.get(),
            20_000,
            |ms| {
                let until = clock.get() + ms;
                let clock = &clock;
                async move {
                    clock.set(until);
                }
            },
        );
        let done = crate::change_flush::run(crate::race_or_deadline(work, async {
            clock.set(30_000);
        }));
        assert!(
            done.is_some(),
            "hung word consumed the platform window before notes or deferral"
        );
        assert_eq!(notes_at.get(), Some(WORDS_SHARE_MS));
        let (summary, frames) = done.unwrap();
        assert_eq!(summary.walled, 1);
        assert_eq!(frames.len(), 1);
        assert_eq!(
            health_json()["tracker_events_arcade_walled_total"].as_u64(),
            Some(before + 1)
        );
        assert_eq!(drain().len(), 1, "the timed-out frame remains queued");
    }
    #[tokio::test]
    async fn ns3_m3_refused_frame_is_shipped_on_next_flush() {
        drain();
        note(&"01".repeat(32), "proof", Some(json!({"merklePath":"00"})));
        let first = drain().pop().unwrap();
        deliver(first, |_| async {
            Err("fake APP_LAYER refuses once".into())
        })
        .await;
        let retry = drain();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0]["body"]["merklePath"], "00");
        deliver(retry.into_iter().next().unwrap(), |_| async { Ok(true) }).await;
        assert!(drain().is_empty());
        assert_eq!(health_json()["tracker_events_index_shipped_total"], 1);
    }
    #[test]
    fn ns3d2_note3_submit_route_passes_mode_and_real_steak_admission() {
        let routes = include_str!("routes.rs");
        let at = routes.find("crate::tracker_events::note_submit(").unwrap();
        let call = &routes[at..at + routes[at..].find("\n    }").unwrap()];
        assert!(
            call.contains("gated_subject.is_some()"),
            "submit route replaced the gated mode with a constant"
        );
        assert!(
            call.contains("steak.values().any(|a|"),
            "submit route replaced admission with a constant"
        );
        for field in [
            "!a.outputs_to_admit.is_empty()",
            "!a.coins_to_retain.is_empty()",
            "a.coins_removed",
            ".is_some_and(|coins| !coins.is_empty())",
        ] {
            assert!(call.contains(field), "submit admission omits {field}");
        }
    }
    #[test]
    fn ns3d_m3_ungated_stranger_submit_leaves_no_word_event() {
        drain();
        // historical-tx-no-spv under the lenient window, even if its marker was admitted.
        note_submit(&"01".repeat(32), false, true);
        assert!(
            drain().is_empty(),
            "stranger historical-tx-no-spv wrote a word event"
        );
        note_submit(&"02".repeat(32), true, false);
        assert!(
            drain().is_empty(),
            "admitted-nothing submit wrote a word event"
        );
        note_submit(&"03".repeat(32), true, true);
        assert_eq!(drain().len(), 1);
    }
}
