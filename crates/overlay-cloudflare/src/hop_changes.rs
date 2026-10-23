//! bsv-low #469 (2026-09-19) — HOP MARKER CHANGE NOTIFICATIONS.
//!
//! The owed list is COMPUTED ON WRITE (design §2.2). Every pot write already
//! notes its outpoint for the app layer (`pot_changes`); a HOP marker admission
//! (`tm_hopparty`: the hop tx's own OP_RETURN output naming the seat, its
//! opponent and the hop outpoint) noted nothing, so an identity whose hop just
//! funded — the very seat a device switch or a strand leaves with money in a
//! hop and no pot — kept its owed rows until the read cadence re-derived them
//! (up to 5 min; the device-switch unit's hop branch read "nothing owed" for
//! its whole 90 s window while the marker sat verified in the index). Each
//! admitted marker now notes BOTH identities it names; the drain rides the
//! same flush points as the pot notes (`pot_changes::flush` ships both sets)
//! and the app layer marks those identities stale and re-derives their rows
//! (`POST /internal/hop-changed`, bearer `INTERNAL_TOKEN`, through the
//! `APP_LAYER` service binding). Over-notification is harmless (a recompute
//! is idempotent); a lost notification costs the cadence, never money.
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use worker::*;

thread_local! {
    /// Each noted identity with the VERSION of its latest note (bsv-low #649 D2-M2; see `pot_changes::CHANGES`).
    static CHANGES: RefCell<BTreeMap<String, u64>> = const { RefCell::new(BTreeMap::new()) };
    static VERSION: Cell<u64> = const { Cell::new(0) };
}

/// One identity into the set at a new version.
fn insert(id: String) {
    let version = VERSION.with(|v| {
        v.set(v.get() + 1);
        v.get()
    });
    CHANGES.with(|c| {
        c.borrow_mut().insert(id, version);
    });
}

/// Record that a hop marker naming `identity` (and `opponent`) was admitted. Cheap, never fails.
pub fn note(identity: &str, opponent: &str) {
    for id in [identity, opponent] {
        let id = id.trim().to_ascii_lowercase();
        if id.len() == 66 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            insert(id);
        }
    }
}

/// Take every noted identity (deduped, sorted), leaving the set empty.
pub fn drain() -> Vec<String> {
    CHANGES.with(|c| std::mem::take(&mut *c.borrow_mut()).into_keys().collect())
}

/// bsv-low #649 M1: a COPY of every noted identity with the version of its note, the set left as it is (see
/// `pot_changes::snapshot`).
pub fn snapshot() -> Vec<(String, u64)> {
    CHANGES.with(|c| c.borrow().iter().map(|(k, v)| (k.clone(), *v)).collect())
}

/// bsv-low #649 M1: take `parked` out of the set, each ONLY at the version the snapshot read (D2-M2: a later note
/// of the same identity stays).
pub fn forget(parked: &[(String, u64)]) {
    CHANGES.with(|c| {
        let mut set = c.borrow_mut();
        for (id, version) in parked {
            if set.get(id) == Some(version) {
                set.remove(id);
            }
        }
    });
}

/// The webhook body: `{"identities":["02…","03…"]}`.
pub fn body_json(identities: &[String]) -> String {
    serde_json::json!({ "identities": identities }).to_string()
}

/// bsv-low #436 (lens fold, L6): the app layer's bound on one `/internal/hop-changed` body
/// (`internal_events::HOP_CHANGED_MAX`, pinned equal from its tests). One flush used to ship EVERY drained
/// identity in one POST and the app layer kept the first eight and broke, with no log line and no count: the
/// ninth identity onward kept its owed rows until its read cadence (the #436 class exactly, on the owed list's
/// hop rows).
pub const HOP_CHANGED_CHUNK: usize = crate::change_flush::CHANGE_CHUNK;

/// Identities the app layer answered it REFUSED (`dropped` in its answer), or whose POST failed twice.
pub const COUNTER_HOP_CHANGED_UNDELIVERED: &str = "hop_changed_undelivered_total";
/// Identities past one flush's bound, noted back for the next flush.
pub const COUNTER_HOP_CHANGED_DEFERRED: &str = "hop_changed_deferred_total";
/// Identities of a POST the app layer did not accept, noted back to be retried once.
pub const COUNTER_HOP_CHANGED_RETRIED: &str = "hop_changed_retried_total";
/// The part of the undelivered whose retry failed too (the delta lens's D-L2: retried and still failed).
pub const COUNTER_HOP_CHANGED_RETRY_FAILED: &str = "hop_changed_retry_failed_total";
/// Noted-back entries (retried or deferred) a later flush POSTed again, whatever that POST answered.
pub const COUNTER_HOP_CHANGED_RESENT: &str = "hop_changed_resent_total";
/// Served on `/health/invariants`, derived on the read: `retried + deferred - resent`, the note-backs never
/// re-sent (lost with an isolate when it stays above 0; see [`crate::change_flush`]).
pub const HOP_CHANGED_NOTED_BACK_UNRESENT: &str = "hop_changed_noted_back_unresent";
/// bsv-low #560 (B-COST-1): new note-backs parked in the D1 outbox (`change_parked`, kind `hop`).
pub const COUNTER_HOP_CHANGED_OUTBOXED: &str = "hop_changed_outboxed_total";
/// The app layer's own count of the identities a body carried past its cap (it writes this row; pinned equal
/// to its `COUNTER_HOP_CHANGED_DROPPED` from its tests).
pub const COUNTER_HOP_CHANGED_DROPPED: &str = "hop_changed_dropped_total";

pub(crate) const COUNTERS: crate::change_flush::FlushCounters =
    crate::change_flush::FlushCounters {
        undelivered: COUNTER_HOP_CHANGED_UNDELIVERED,
        deferred: COUNTER_HOP_CHANGED_DEFERRED,
        retried: COUNTER_HOP_CHANGED_RETRIED,
        retry_failed: COUNTER_HOP_CHANGED_RETRY_FAILED,
        resent: COUNTER_HOP_CHANGED_RESENT,
        noted_back_unresent: HOP_CHANGED_NOTED_BACK_UNRESENT,
        outboxed: COUNTER_HOP_CHANGED_OUTBOXED,
    };

thread_local! {
    /// The identities noted back once after a failed POST (the retry is bounded at one), and those deferred past a
    /// flush's bound, until a later flush POSTs them again (the delta lens's D-L2).
    static NOTED_BACK: RefCell<crate::change_flush::NotedBack<String>> = const { RefCell::new(crate::change_flush::NotedBack::new()) };
}

/// THE FLUSH of the hop set, transport injected (`ship` runs exactly this, and so do the pins, through a fake
/// `post`): the identities in POSTs of at most [`HOP_CHANGED_CHUNK`], each body built by [`body_json`], under
/// the bounds of [`crate::change_flush::ship_chunks`].
pub async fn ship_with<P, Fut, C>(
    identities: Vec<String>,
    post: P,
    now_ms: C,
    deadline_ms: u64,
) -> crate::change_flush::Shipped<String>
where
    P: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<usize, ()>>,
    C: Fn() -> u64,
{
    crate::change_flush::ship_chunks(identities, body_json, post, now_ms, deadline_ms).await
}

/// What a flush owes after its POSTs ([`crate::change_flush::settle`] over this set's memory): the deferred part
/// and a failed POST's entries (once) are noted back for the next flush. Returns the flush's account.
pub fn settle(shipped: &crate::change_flush::Shipped<String>) -> crate::change_flush::Tally {
    settle_noting(shipped).0
}

/// [`settle`], answering also the flush's NEW note-backs (bsv-low #560: what [`settle_and_park`] parks).
fn settle_noting(
    shipped: &crate::change_flush::Shipped<String>,
) -> (crate::change_flush::Tally, Vec<String>) {
    let (back, fresh, tally) =
        NOTED_BACK.with(|m| crate::change_flush::settle_noting(&mut m.borrow_mut(), shipped));
    for id in back {
        insert(id);
    }
    (tally, fresh)
}

/// bsv-low #560 (B-COST-1): [`settle`], then the NEW note-backs parked in the D1 outbox (kind `hop`; the pot set's
/// `settle_and_park`, the same rule).
pub(crate) async fn settle_and_park<P: crate::change_park::ParkPort>(
    port: &P,
    shipped: &crate::change_flush::Shipped<String>,
    now_ms: u64,
) -> (crate::change_flush::Tally, crate::change_park::Outboxed) {
    let (tally, fresh) = settle_noting(shipped);
    let parked = crate::change_park::park_noted_back(
        port,
        crate::change_park::KIND_HOP,
        fresh,
        COUNTER_HOP_CHANGED_OUTBOXED,
        now_ms,
    )
    .await;
    (tally, parked)
}

/// Ship one flush: the identities in POSTs of at most [`HOP_CHANGED_CHUNK`], one after another, no POST started
/// at or past `deadline_ms`. Unconfigured (`APP_LAYER_URL` / `INTERNAL_TOKEN`) ⇒ logs and no-ops. Meant to run
/// under `wait_until` (the pot shipper's twin; the same service binding, the same bearer, the same account:
/// nothing is lost silently).
pub async fn ship(env: Env, identities: Vec<String>, deadline_ms: u64) {
    if identities.is_empty() {
        return;
    }
    let Some((url, token)) = crate::change_flush::configured(&env) else {
        console_log!("[hop-changes] not configured (APP_LAYER_URL / INTERNAL_TOKEN): {} identity(ies) not notified", identities.len());
        return;
    };
    let total = identities.len();
    let shipped = ship_with(
        identities,
        |body| {
            crate::change_flush::post_body(
                &env,
                &url,
                &token,
                "/internal/hop-changed",
                "hop-changes",
                body,
            )
        },
        || Date::now().as_millis(),
        deadline_ms,
    )
    .await;
    if !shipped.delivered.is_empty() {
        console_log!(
            "[hop-changes] notified {} identity(ies) in {} POST(s)",
            shipped.delivered.len(),
            shipped.posts
        );
    }
    // bsv-low #560: the note-backs into the D1 outbox too (no binding: memory only, as before, logged)
    let tally = match env.d1("OVERLAY_DB") {
        Ok(db) => {
            settle_and_park(
                &crate::change_park::WorkerPark { db: &db },
                &shipped,
                Date::now().as_millis(),
            )
            .await
            .0
        }
        Err(_) => {
            console_log!("[hop-changes] no OVERLAY_DB: the flush's note-backs stay in the isolate's memory only");
            settle(&shipped)
        }
    };
    crate::change_flush::account(&env, "hop-changes", &COUNTERS, total, tally).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn note_keeps_well_formed_identities_once_and_drain_empties() {
        drain();
        let a = format!("02{}", "aa".repeat(32));
        let b = format!("03{}", "bb".repeat(32));
        note(&a.to_ascii_uppercase(), &b);
        note(&b, "not-an-identity");
        let d = drain();
        assert_eq!(d, vec![a.clone(), b.clone()]);
        assert!(drain().is_empty());
        assert_eq!(
            body_json(&d),
            format!("{{\"identities\":[\"{a}\",\"{b}\"]}}")
        );
    }

    /// bsv-low #436 (lens fold, L6) through the real flush and a fake transport: nineteen identities ride three
    /// bodies of at most eight, each identity in exactly one; a failed POST's identities are noted back once.
    #[test]
    fn a_hop_flush_is_chunked_at_the_bound_and_a_failed_chunk_is_noted_back_once() {
        drain();
        let ids: Vec<String> = (0..19u32).map(|i| format!("02{i:064x}")).collect();
        let bodies: RefCell<Vec<String>> = RefCell::new(Vec::new());
        let shipped = crate::change_flush::run(ship_with(
            ids.clone(),
            |b: String| {
                bodies.borrow_mut().push(b);
                let n = bodies.borrow().len();
                async move {
                    if n == 3 {
                        Err(())
                    } else {
                        Ok(0)
                    }
                }
            },
            || 0,
            1,
        ));
        assert_eq!(bodies.borrow().len(), 3);
        let mut seen: Vec<String> = Vec::new();
        for b in bodies.borrow().iter() {
            let v: serde_json::Value = serde_json::from_str(b).unwrap();
            let arr = v["identities"].as_array().unwrap();
            assert!(!arr.is_empty() && arr.len() <= HOP_CHANGED_CHUNK);
            seen.extend(arr.iter().map(|x| x.as_str().unwrap().to_string()));
        }
        assert_eq!(seen, ids, "every identity in exactly one body");
        assert_eq!(
            settle(&shipped),
            crate::change_flush::Tally {
                retried: 3,
                ..Default::default()
            }
        );
        assert_eq!(
            drain(),
            ids[16..].to_vec(),
            "the failed chunk rides the next flush"
        );
        let again = crate::change_flush::run(ship_with(
            ids[16..].to_vec(),
            |_b: String| async { Err(()) },
            || 0,
            1,
        ));
        assert_eq!(
            settle(&again),
            crate::change_flush::Tally {
                undelivered: 3,
                retry_failed: 3,
                resent: 3,
                ..Default::default()
            },
            "a second failure is counted and let go"
        );
        assert!(drain().is_empty());
    }
}
