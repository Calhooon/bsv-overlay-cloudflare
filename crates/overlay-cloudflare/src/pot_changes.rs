//! W2-P4 (bsv-low event-driven client, 2026-09-02) — POT CHANGE NOTIFICATIONS.
//!
//! The D1 pot storage is the single writer of every pot fact a seat's felt
//! renders (admission, spend pointer, confirmation, verdict, spender facts).
//! Each successful write NOTES its outpoint here (a per-isolate set — no I/O
//! inside storage), and the request / queue / cron context that did the
//! work DRAINS the set once and ships ONE bounded notification to the
//! app-layer (`POST /internal/pot-changed`, bearer `INTERNAL_TOKEN`) through
//! `wait_until`, off the critical path. The app-layer assembles the served
//! `/results` entry for both seats and files it into their `low_events`
//! boxes as an EVENT. Over-notification is harmless (the app-layer re-reads
//! the truth); a lost notification costs nothing money-wise (every money
//! view is still served from the same rows on the next read).
use std::cell::RefCell;
use std::collections::BTreeSet;

use worker::*;

thread_local! {
    static CHANGES: RefCell<BTreeSet<(String, u32)>> = const { RefCell::new(BTreeSet::new()) };
}

/// Record that `(txid, vout)`'s row changed. Cheap, never fails.
pub fn note(txid: &str, vout: u32) {
    let key = (txid.to_ascii_lowercase(), vout);
    CHANGES.with(|c| {
        c.borrow_mut().insert(key);
    });
}

/// Take every noted outpoint (deduped), leaving the set empty.
pub fn drain() -> Vec<(String, u32)> {
    CHANGES.with(|c| std::mem::take(&mut *c.borrow_mut()).into_iter().collect())
}

/// The webhook body: `{"outpoints":[{"txid","vout"},…]}`.
pub fn body_json(outpoints: &[(String, u32)]) -> String {
    let arr: Vec<serde_json::Value> = outpoints
        .iter()
        .map(|(t, v)| serde_json::json!({ "txid": t, "vout": v }))
        .collect();
    serde_json::json!({ "outpoints": arr }).to_string()
}

/// bsv-low #436: the app layer's bound on one `/internal/pot-changed` body (`internal_events::POT_CHANGED_MAX`,
/// pinned equal from the app layer's tests, which link this crate). One flush used to ship EVERY drained outpoint
/// in one POST and the app layer kept the first eight: a tip pass confirming nine or more pot spends in one block
/// lost the ninth onward from the `broadcast-low-pots` push and the durable per-seat filing, with no log line
/// there. The flush is chunked at the bound, so nothing a flush ships is refused.
pub const POT_CHANGED_CHUNK: usize = crate::change_flush::CHANGE_CHUNK;

/// The most POSTs one flush makes (128 outpoints): the flush runs under `wait_until`, whose wall is finite, and
/// each POST is a subrequest. The remainder is NOT dropped: it is noted back and rides the next flush on this
/// isolate, logged and counted. The cost of the bound is measured and stated in [`crate::change_flush`].
pub const POT_CHANGED_MAX_CHUNKS: usize = crate::change_flush::CHANGE_MAX_CHUNKS;

/// Outpoints the app layer answered it REFUSED (`dropped` in its answer), or whose POST failed twice.
pub const COUNTER_POT_CHANGED_UNDELIVERED: &str = "pot_changed_undelivered_total";
/// Outpoints past one flush's bound, noted back for the next flush.
pub const COUNTER_POT_CHANGED_DEFERRED: &str = "pot_changed_deferred_total";
/// Outpoints of a POST the app layer did not accept, noted back to be retried once (lens L2).
pub const COUNTER_POT_CHANGED_RETRIED: &str = "pot_changed_retried_total";
/// The part of the undelivered whose retry failed too (the delta lens's D-L2: retried and still failed).
pub const COUNTER_POT_CHANGED_RETRY_FAILED: &str = "pot_changed_retry_failed_total";
/// Noted-back entries (retried or deferred) a later flush POSTed again, whatever that POST answered.
pub const COUNTER_POT_CHANGED_RESENT: &str = "pot_changed_resent_total";
/// Served on `/health/invariants`, derived on the read: `retried + deferred - resent`, the note-backs never
/// re-sent (lost with an isolate when it stays above 0; see [`crate::change_flush`]).
pub const POT_CHANGED_NOTED_BACK_UNRESENT: &str = "pot_changed_noted_back_unresent";
/// The app layer's own count of the outpoints a body carried past its cap (it writes this row; pinned equal to
/// its `COUNTER_POT_CHANGED_DROPPED` from its tests, lens N1).
pub const COUNTER_POT_CHANGED_DROPPED: &str = "pot_changed_dropped_total";

pub(crate) const COUNTERS: crate::change_flush::FlushCounters = crate::change_flush::FlushCounters {
    undelivered: COUNTER_POT_CHANGED_UNDELIVERED,
    deferred: COUNTER_POT_CHANGED_DEFERRED,
    retried: COUNTER_POT_CHANGED_RETRIED,
    retry_failed: COUNTER_POT_CHANGED_RETRY_FAILED,
    resent: COUNTER_POT_CHANGED_RESENT,
    noted_back_unresent: POT_CHANGED_NOTED_BACK_UNRESENT,
};

thread_local! {
    /// The outpoints noted back once after a failed POST (lens L2: the retry is bounded at one), and those deferred
    /// past a flush's bound, until a later flush POSTs them again (the delta lens's D-L2).
    static NOTED_BACK: RefCell<crate::change_flush::NotedBack<(String, u32)>> = const { RefCell::new(crate::change_flush::NotedBack::new()) };
}

/// Noted outpoints, `(txid, vout)`.
pub type Outpoints = Vec<(String, u32)>;

/// THE FLUSH of the pot set, transport injected (lens L1: `ship` runs exactly this, and so do the pins, through
/// a fake `post`): the outpoints in POSTs of at most [`POT_CHANGED_CHUNK`], each body built by [`body_json`],
/// one after another, under the bounds of [`crate::change_flush::ship_chunks`].
pub async fn ship_with<P, Fut, C>(outpoints: Outpoints, post: P, now_ms: C, deadline_ms: u64) -> crate::change_flush::Shipped<(String, u32)>
where
    P: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<usize, ()>>,
    C: Fn() -> u64,
{
    crate::change_flush::ship_chunks(outpoints, body_json, post, now_ms, deadline_ms).await
}

/// What a flush owes after its POSTs ([`crate::change_flush::settle`] over this set's memory): the deferred part
/// and a failed POST's entries (once) are noted back for the next flush. Returns the flush's account.
pub fn settle(shipped: &crate::change_flush::Shipped<(String, u32)>) -> crate::change_flush::Tally {
    let (back, tally) = NOTED_BACK.with(|m| crate::change_flush::settle(&mut m.borrow_mut(), shipped));
    for (txid, vout) in &back {
        note(txid, *vout);
    }
    tally
}

/// Ship one flush: the outpoints in POSTs of at most [`POT_CHANGED_CHUNK`] (bsv-low #436), one after another,
/// no POST started at or past `deadline_ms`. Unconfigured (`APP_LAYER_URL` / `INTERNAL_TOKEN`) ⇒ logs and
/// no-ops. Meant to run under `wait_until`. Nothing is lost silently: an outpoint the app layer answered it
/// refused, or whose POST failed twice, is logged and counted (`pot_changed_undelivered_total`); a failed
/// POST's outpoints are noted back once (`pot_changed_retried_total`); the part of a flood past the flush's
/// bound is noted back for the next flush (`pot_changed_deferred_total`). The note-back is not durable: the
/// hole and its bound are named in [`crate::change_flush`].
pub async fn ship(env: Env, outpoints: Vec<(String, u32)>, deadline_ms: u64) {
    if outpoints.is_empty() {
        return;
    }
    let Some((url, token)) = crate::change_flush::configured(&env) else {
        console_log!("[pot-changes] not configured (APP_LAYER_URL / INTERNAL_TOKEN): {} outpoint(s) not notified", outpoints.len());
        return;
    };
    let total = outpoints.len();
    let shipped = ship_with(
        outpoints,
        |body| crate::change_flush::post_body(&env, &url, &token, "/internal/pot-changed", "pot-changes", body),
        || Date::now().as_millis(),
        deadline_ms,
    )
    .await;
    if !shipped.delivered.is_empty() {
        console_log!("[pot-changes] notified {} outpoint(s) in {} POST(s)", shipped.delivered.len(), shipped.posts);
    }
    let tally = settle(&shipped);
    crate::change_flush::account(&env, "pot-changes", &COUNTERS, total, tally).await;
}

/// Drain and ship INSIDE a detached task, awaited there (2026-09-04).
///
/// THE STRANDED-NOTE HOLE (found the same day in a sibling overlay by the
/// wallet terminal): a pot write made from a `wait_until` task that runs
/// AFTER the request's [`flush`] queues its note into this isolate-global set
/// — nothing drains it until the NEXT request on this isolate flushes, and it
/// is lost when the isolate goes first. Every detached task that writes pot
/// rows (the post-submit signers self-heal, `routes.rs`) must end with
/// `flush_inline(env.clone()).await` so its own notes ride its own task.
pub async fn flush_inline(env: Env) {
    let changed = drain();
    // bsv-low #469 (2026-09-19): the hop-marker notes ride every flush the pot notes ride (one set of flush points)
    let hops = crate::hop_changes::drain();
    // one wall for both sets (the hop set first: the owed list's hop rows)
    let deadline_ms = Date::now().as_millis() + crate::change_flush::FLUSH_WALL_BUDGET_MS;
    if !hops.is_empty() {
        crate::hop_changes::ship(env.clone(), hops, deadline_ms).await;
    }
    if changed.is_empty() {
        return;
    }
    ship(env, changed, deadline_ms).await;
}

/// Drain and ship under the given `wait_until` (a request, queue or cron
/// context). One call per unit of work. NOT sufficient for notes queued by a
/// task that runs after this call — see [`flush_inline`].
pub fn flush<F: FnOnce(std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>)>(
    env: &Env,
    wait_until: F,
) {
    let changed = drain();
    // bsv-low #469 (2026-09-19): the hop-marker notes ride the same flush (one detached task carries both sets)
    let hops = crate::hop_changes::drain();
    if changed.is_empty() && hops.is_empty() {
        return;
    }
    let env2 = env.clone();
    wait_until(Box::pin(async move {
        // one wall for both sets, read when the task starts (the hop set first: the owed list's hop rows)
        let deadline_ms = Date::now().as_millis() + crate::change_flush::FLUSH_WALL_BUDGET_MS;
        if !hops.is_empty() {
            crate::hop_changes::ship(env2.clone(), hops, deadline_ms).await;
        }
        if !changed.is_empty() {
            ship(env2, changed, deadline_ms).await;
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn note_dedupes_and_drain_empties() {
        drain();
        note("AA", 0);
        note("aa", 0);
        note("bb", 1);
        let d = drain();
        assert_eq!(d, vec![("aa".to_string(), 0), ("bb".to_string(), 1)]);
        assert!(drain().is_empty());
    }

    /// SOURCE PIN (2026-09-04; admit-fast step 4, 2026-09-15): every detached
    /// pot-writing task in routes.rs ships its own notes. Three such tasks:
    /// the signers self-heal (`backfill_settle_signers(&storage`), and — since
    /// the `network_seen` latch notes the pot rows it witnesses — the two
    /// background latchers, the ungated corroboration closure (`seen_env`)
    /// and the #413 dual push (`dual_env`). A fourth detached writer without a
    /// matching `flush_inline` re-opens the stranded-note hole.
    #[test]
    fn every_detached_pot_writing_task_ships_its_own_notes() {
        let routes = include_str!("routes.rs");
        // Production code only: the route tests carry these needles as literals.
        let routes = &routes[..routes.find("#[cfg(test)]").unwrap_or(routes.len())];
        let heals = routes.matches("backfill_settle_signers(&storage").count();
        let latchers = routes.matches("let seen_env = env.clone();").count()
            + routes.matches("let dual_env = env.clone();").count();
        let inline = routes.matches("pot_changes::flush_inline(").count();
        // bsv-low loop 18 (2026-09-21, #513): the write-side guard re-evicts INSIDE the request after the engine
        // write and answers 422 — an early return that skips the route's end flush, so it ships the eviction's
        // notes inline itself (counted by its counter's name; a comment cannot satisfy a needle).
        let guard = routes.matches("COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE").count();
        // round 2 of the same gate (NEW-4): the Rejected arm re-runs an open eviction (an incomplete pass
        // converges on the network's second refusal) and ships its notes inline the same way
        let rejected_rerun = routes.matches("refused again under an open eviction").count();
        assert!(
            heals >= 1,
            "the signers self-heal task moved — re-point this pin"
        );
        assert_eq!(latchers, 2, "the two background latchers capture the env");
        assert_eq!(guard, 1, "the write-side guard re-evicts once, after the write");
        assert_eq!(rejected_rerun, 1, "the Rejected arm re-runs an open eviction once");
        assert_eq!(
            heals + latchers + guard + rejected_rerun,
            inline,
            "a detached task (or the write-side guard's early return) writes pot rows without shipping its own notes (see flush_inline)"
        );
    }

    /// bsv-low #436, through the REAL flush (`ship_with`, what `ship` runs; lens L1) and a fake transport: a
    /// flush is chunked at the app layer's bound, every outpoint in exactly one body, no empty body; a flood
    /// past the flush's own bound is deferred for the next flush, never dropped. To red: POST one whole body.
    #[test]
    fn a_flush_is_chunked_at_the_bound_and_a_flood_is_deferred_never_dropped() {
        let ops = |n: usize| -> Vec<(String, u32)> { (0..n).map(|i| (format!("{i:064x}"), 0)).collect() };
        let bound = POT_CHANGED_CHUNK * POT_CHANGED_MAX_CHUNKS;
        for n in [0, 1, 8, 9, 16, 17, 128, 131] {
            let bodies: RefCell<Vec<String>> = RefCell::new(Vec::new());
            let shipped = crate::change_flush::run(ship_with(
                ops(n),
                |b: String| {
                    bodies.borrow_mut().push(b);
                    async { Ok(0) }
                },
                || 0,
                1,
            ));
            let now = n.min(bound);
            assert_eq!(bodies.borrow().len(), now.div_ceil(POT_CHANGED_CHUNK), "{n}");
            let mut seen = Vec::new();
            for b in bodies.borrow().iter() {
                let v: serde_json::Value = serde_json::from_str(b).unwrap();
                let arr = v["outpoints"].as_array().unwrap();
                assert!(!arr.is_empty() && arr.len() <= POT_CHANGED_CHUNK, "{n}");
                seen.extend(arr.iter().map(|o| (o["txid"].as_str().unwrap().to_string(), o["vout"].as_u64().unwrap() as u32)));
            }
            assert_eq!(seen, ops(now), "every outpoint in exactly one body, in order ({n})");
            assert_eq!([shipped.delivered, shipped.deferred].concat(), ops(n), "shipped now or deferred: nothing else ({n})");
        }
    }

    /// Lens L2 and L3 through the real flush and the real note-back: a POST the app layer answers 5xx loses
    /// nothing the first time (its eight outpoints are noted back and ride the next flush); a second failure is
    /// counted undelivered and let go (bounded: once); the deferred part of a flood is noted back too.
    /// To red: drop the note-back of `again` in `settle`, or never forget a retried outpoint.
    #[test]
    fn a_failed_chunk_is_noted_back_once_and_a_second_failure_is_counted() {
        drain();
        NOTED_BACK.with(|m| m.borrow_mut().clear());
        let ops: Vec<(String, u32)> = (0..19u32).map(|i| (format!("{i:064x}"), 0)).collect();
        // the first flush: the second of three POSTs is answered 503
        let calls = std::cell::Cell::new(0usize);
        let failing_second = |_b: String| {
            calls.set(calls.get() + 1);
            let n = calls.get();
            async move { if n == 2 { Err(()) } else { Ok(0) } }
        };
        let first = crate::change_flush::run(ship_with(ops.clone(), failing_second, || 0, 1));
        assert_eq!(settle(&first), crate::change_flush::Tally { retried: 8, ..Default::default() }, "eight retried, nothing lost yet");
        let back = drain();
        assert_eq!(back, ops[8..16].to_vec(), "the failed chunk rides the next flush");
        // the next flush ships them with eight new ones: their POST fails again, the new ones' is accepted
        let calls = std::cell::Cell::new(0usize);
        let second = crate::change_flush::run(ship_with(
            [back, (100..108u32).map(|i| (format!("{i:064x}"), 0)).collect()].concat(),
            |_b: String| {
                calls.set(calls.get() + 1);
                let n = calls.get();
                async move { if n == 1 { Err(()) } else { Ok(0) } }
            },
            || 0,
            1,
        ));
        assert_eq!(second.failed, ops[8..16].to_vec(), "the eight retried outpoints fail a second time");
        assert_eq!(settle(&second), crate::change_flush::Tally { undelivered: 8, retry_failed: 8, resent: 8, ..Default::default() }, "counted undelivered, not noted back again");
        assert!(drain().is_empty(), "the retry is bounded at one");
        // a flood: the part past the bound is noted back
        let flood: Vec<(String, u32)> = (0..131u32).map(|i| (format!("{i:064x}"), 1)).collect();
        let third = crate::change_flush::run(ship_with(flood.clone(), |_b: String| async { Ok(0) }, || 0, 1));
        assert_eq!(settle(&third), crate::change_flush::Tally { deferred: 3, ..Default::default() });
        assert_eq!(drain(), flood[128..].to_vec());
    }

    /// SOURCE PIN: `ship` runs the one flush (`ship_with`) over the real transport, notes back and accounts.
    /// To red: POST `body_json(&outpoints)` whole again, or drop the settle.
    #[test]
    fn ship_runs_the_one_flush_and_accounts_for_what_it_does_not_deliver() {
        let code_only = |s: &str| s.lines().map(|l| l.split("//").next().unwrap_or("")).collect::<Vec<_>>().join("\n");
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let src = include_str!("pot_changes.rs");
        let src = &src[..src.find("#[cfg(test)]").unwrap()];
        let start = src.find("pub async fn ship(").expect("ship");
        let ship = squash(&code_only(&src[start..start + src[start..].find("\npub async fn flush_inline(").expect("the next fn")]));
        assert!(ship.contains(&squash(r#"let shipped = ship_with( outpoints, |body| crate::change_flush::post_body(&env, &url, &token, "/internal/pot-changed", "pot-changes", body),"#)));
        assert!(ship.contains(&squash("let tally = settle(&shipped);")));
        assert!(ship.contains(&squash(r#"crate::change_flush::account(&env, "pot-changes", &COUNTERS, total, tally).await;"#)));
        assert!(!ship.contains("body_json("), "ship never builds a body of its own");
        let with = squash(&code_only(&src[src.find("pub async fn ship_with<").expect("ship_with")..start]));
        assert!(with.contains(&squash("crate::change_flush::ship_chunks(outpoints, body_json, post, now_ms, deadline_ms).await")));
    }

    #[test]
    fn body_json_is_the_outpoint_list() {
        let v: serde_json::Value =
            serde_json::from_str(&body_json(&[("aa".into(), 0), ("bb".into(), 2)])).unwrap();
        assert_eq!(v["outpoints"][0]["txid"], "aa");
        assert_eq!(v["outpoints"][1]["vout"], 2);
    }
}
