//! W2-P6 (bsv-low event-driven client, 2026-09-03) — LOBBY CHANGE NOTIFICATIONS.
//!
//! The `tm_low` / `ls_low` storage is the single writer of every lobby advert
//! the Lobby page lists (a TABLE record admitted, a TABLE record evicted by
//! its spend or by the advert-lifecycle reaps). Each successful write NOTES
//! its outpoint + kind here (a per-isolate set — no I/O inside storage), and
//! the request / queue / cron context that did the work DRAINS the set once
//! and ships ONE bounded notification to the app-layer
//! (`POST /internal/lobby-changed`, bearer `INTERNAL_TOKEN`, through the
//! `APP_LAYER` service binding) under `wait_until`, off the critical path.
//! The app-layer fans a `lobby` event into `broadcast-low-lobby`; every open
//! Lobby refetches ONCE (the 60 s list poll is gone). Over-notification is
//! harmless; a lost notification costs one refetch on the next event.
use std::cell::RefCell;
use std::collections::BTreeSet;
use worker::*;

thread_local! {
    static CHANGES: RefCell<BTreeSet<Change>> = const { RefCell::new(BTreeSet::new()) };
}

/// A TABLE advert was admitted (or re-admitted) at `(txid, vout)`.
pub fn note_admitted(txid: &str, vout: u32) {
    note(txid, vout, "admitted");
}

/// A TABLE advert at `(txid, vout)` was evicted (spent, reaped, or refused).
pub fn note_evicted(txid: &str, vout: u32) {
    note(txid, vout, "evicted");
}

fn note(txid: &str, vout: u32, kind: &'static str) {
    let key = (txid.to_ascii_lowercase(), vout, kind);
    CHANGES.with(|c| {
        c.borrow_mut().insert(key);
    });
}

/// Take every noted change (deduped), leaving the set empty.
pub fn drain() -> Vec<(String, u32, &'static str)> {
    CHANGES.with(|c| std::mem::take(&mut *c.borrow_mut()).into_iter().collect())
}

/// The webhook body: `{"changes":[{"txid","vout","kind"},…]}`.
pub fn body_json(changes: &[(String, u32, &'static str)]) -> String {
    let arr: Vec<serde_json::Value> = changes
        .iter()
        .map(|(t, v, k)| serde_json::json!({ "txid": t, "vout": v, "kind": k }))
        .collect();
    serde_json::json!({ "changes": arr }).to_string()
}

/// One noted lobby change: `(txid, vout, kind)`.
pub type Change = (String, u32, &'static str);

/// bsv-low #436 (lens fold, L6): the app layer's bound on one `/internal/lobby-changed` body
/// (`internal_events::LOBBY_CHANGED_MAX`, pinned equal from its tests). One flush used to ship EVERY drained
/// change in one POST and the app layer kept the first eight and broke, with no log line and no count. The
/// event is a refetch signal, so a trimmed entry cost a log line, not a refetch; it is chunked and counted like
/// its two siblings all the same (one rule for the three webhooks, [`crate::change_flush`]).
pub const LOBBY_CHANGED_CHUNK: usize = crate::change_flush::CHANGE_CHUNK;

/// Changes the app layer answered it REFUSED (`dropped` in its answer), or whose POST failed twice.
pub const COUNTER_LOBBY_CHANGED_UNDELIVERED: &str = "lobby_changed_undelivered_total";
/// Changes past one flush's bound, noted back for the next flush.
pub const COUNTER_LOBBY_CHANGED_DEFERRED: &str = "lobby_changed_deferred_total";
/// Changes of a POST the app layer did not accept, noted back to be retried once.
pub const COUNTER_LOBBY_CHANGED_RETRIED: &str = "lobby_changed_retried_total";
/// The part of the undelivered whose retry failed too (the delta lens's D-L2: retried and still failed).
pub const COUNTER_LOBBY_CHANGED_RETRY_FAILED: &str = "lobby_changed_retry_failed_total";
/// Noted-back entries (retried or deferred) a later flush POSTed again, whatever that POST answered.
pub const COUNTER_LOBBY_CHANGED_RESENT: &str = "lobby_changed_resent_total";
/// Served on `/health/invariants`, derived on the read: `retried + deferred - resent`, the note-backs never
/// re-sent (lost with an isolate when it stays above 0; see [`crate::change_flush`]).
pub const LOBBY_CHANGED_NOTED_BACK_UNRESENT: &str = "lobby_changed_noted_back_unresent";
/// The app layer's own count of the changes a body carried past its cap (it writes this row; pinned equal to
/// its `COUNTER_LOBBY_CHANGED_DROPPED` from its tests).
pub const COUNTER_LOBBY_CHANGED_DROPPED: &str = "lobby_changed_dropped_total";

pub(crate) const COUNTERS: crate::change_flush::FlushCounters = crate::change_flush::FlushCounters {
    undelivered: COUNTER_LOBBY_CHANGED_UNDELIVERED,
    deferred: COUNTER_LOBBY_CHANGED_DEFERRED,
    retried: COUNTER_LOBBY_CHANGED_RETRIED,
    retry_failed: COUNTER_LOBBY_CHANGED_RETRY_FAILED,
    resent: COUNTER_LOBBY_CHANGED_RESENT,
    noted_back_unresent: LOBBY_CHANGED_NOTED_BACK_UNRESENT,
};

thread_local! {
    /// The changes noted back once after a failed POST (the retry is bounded at one), and those deferred past a
    /// flush's bound, until a later flush POSTs them again (the delta lens's D-L2).
    static NOTED_BACK: RefCell<crate::change_flush::NotedBack<Change>> = const { RefCell::new(crate::change_flush::NotedBack::new()) };
}

/// THE FLUSH of the lobby set, transport injected (`ship` runs exactly this, and so do the pins, through a fake
/// `post`): the changes in POSTs of at most [`LOBBY_CHANGED_CHUNK`], each body built by [`body_json`], under
/// the bounds of [`crate::change_flush::ship_chunks`].
pub async fn ship_with<P, Fut, C>(changes: Vec<Change>, post: P, now_ms: C, deadline_ms: u64) -> crate::change_flush::Shipped<Change>
where
    P: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<usize, ()>>,
    C: Fn() -> u64,
{
    crate::change_flush::ship_chunks(changes, body_json, post, now_ms, deadline_ms).await
}

/// What a flush owes after its POSTs ([`crate::change_flush::settle`] over this set's memory): the deferred part
/// and a failed POST's entries (once) are noted back for the next flush. Returns the flush's account.
pub fn settle(shipped: &crate::change_flush::Shipped<Change>) -> crate::change_flush::Tally {
    let (back, tally) = NOTED_BACK.with(|m| crate::change_flush::settle(&mut m.borrow_mut(), shipped));
    for (txid, vout, kind) in &back {
        note(txid, *vout, kind);
    }
    tally
}

/// Ship one flush through the APP_LAYER service binding (a plain fetch between two Workers on one zone is
/// refused by Cloudflare: 1042 behind a 404): the changes in POSTs of at most [`LOBBY_CHANGED_CHUNK`], one
/// after another, inside the flush's wall. Unconfigured ⇒ logs and no-ops. Runs under `wait_until`. Nothing is
/// lost silently (the pot shipper's account).
pub async fn ship(env: Env, changes: Vec<Change>) {
    if changes.is_empty() {
        return;
    }
    let Some((url, token)) = crate::change_flush::configured(&env) else {
        console_log!("[lobby-changes] not configured (APP_LAYER_URL / INTERNAL_TOKEN): {} change(s) not notified", changes.len());
        return;
    };
    let total = changes.len();
    let deadline_ms = Date::now().as_millis() + crate::change_flush::FLUSH_WALL_BUDGET_MS;
    let shipped = ship_with(
        changes,
        |body| crate::change_flush::post_body(&env, &url, &token, "/internal/lobby-changed", "lobby-changes", body),
        || Date::now().as_millis(),
        deadline_ms,
    )
    .await;
    if !shipped.delivered.is_empty() {
        console_log!("[lobby-changes] notified {} change(s) in {} POST(s)", shipped.delivered.len(), shipped.posts);
    }
    let tally = settle(&shipped);
    crate::change_flush::account(&env, "lobby-changes", &COUNTERS, total, tally).await;
}

/// Drain and ship under the given `wait_until`. One call per unit of work.
pub fn flush<F: FnOnce(std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>)>(
    env: &Env,
    wait_until: F,
) {
    let changed = drain();
    if changed.is_empty() {
        return;
    }
    wait_until(Box::pin(ship(env.clone(), changed)));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn note_dedupes_by_outpoint_and_kind_and_drain_empties() {
        drain();
        note_admitted("AA", 0);
        note_admitted("aa", 0);
        note_evicted("aa", 0);
        note_admitted("bb", 1);
        let got = drain();
        assert_eq!(got.len(), 3);
        assert!(got.contains(&("aa".into(), 0, "admitted")));
        assert!(got.contains(&("aa".into(), 0, "evicted")));
        assert!(drain().is_empty());
    }

    #[test]
    fn body_json_is_the_changes_shape() {
        let b = body_json(&[
            ("ab".repeat(32), 0, "admitted"),
            ("cd".repeat(32), 2, "evicted"),
        ]);
        let v: serde_json::Value = serde_json::from_str(&b).unwrap();
        assert_eq!(v["changes"].as_array().unwrap().len(), 2);
        assert_eq!(v["changes"][1]["kind"], "evicted");
        assert_eq!(v["changes"][1]["vout"], 2);
    }

    /// bsv-low #436 (lens fold, L6) through the real flush and a fake transport: nineteen changes ride three
    /// bodies of at most eight, each change in exactly one; a failed POST's changes are noted back once.
    #[test]
    fn a_lobby_flush_is_chunked_at_the_bound_and_a_failed_chunk_is_noted_back_once() {
        drain();
        let changes: Vec<Change> = (0..19u32).map(|i| (format!("{i:064x}"), i % 3, if i % 2 == 0 { "admitted" } else { "evicted" })).collect();
        let bodies: RefCell<Vec<String>> = RefCell::new(Vec::new());
        let shipped = crate::change_flush::run(ship_with(
            changes.clone(),
            |b: String| {
                bodies.borrow_mut().push(b);
                let n = bodies.borrow().len();
                async move { if n == 3 { Err(()) } else { Ok(0) } }
            },
            || 0,
            1,
        ));
        assert_eq!(bodies.borrow().len(), 3);
        let mut seen: Vec<(String, u32, String)> = Vec::new();
        for b in bodies.borrow().iter() {
            let v: serde_json::Value = serde_json::from_str(b).unwrap();
            let arr = v["changes"].as_array().unwrap();
            assert!(!arr.is_empty() && arr.len() <= LOBBY_CHANGED_CHUNK);
            seen.extend(arr.iter().map(|o| (o["txid"].as_str().unwrap().to_string(), o["vout"].as_u64().unwrap() as u32, o["kind"].as_str().unwrap().to_string())));
        }
        let want: Vec<(String, u32, String)> = changes.iter().map(|(t, v, k)| (t.clone(), *v, k.to_string())).collect();
        assert_eq!(seen, want, "every change in exactly one body");
        assert_eq!(settle(&shipped), crate::change_flush::Tally { retried: 3, ..Default::default() });
        assert_eq!(drain(), changes[16..].to_vec(), "the failed chunk rides the next flush");
        let again = crate::change_flush::run(ship_with(changes[16..].to_vec(), |_b: String| async { Err(()) }, || 0, 1));
        assert_eq!(settle(&again), crate::change_flush::Tally { undelivered: 3, retry_failed: 3, resent: 3, ..Default::default() }, "a second failure is counted and let go");
        assert!(drain().is_empty());
    }
}
