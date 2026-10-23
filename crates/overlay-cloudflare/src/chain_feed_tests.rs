//! bsv-low #620 (the words map's L4) pins: the overlay consumes chaintracks' typed chain-event feed on its own durable
//! cursor and reacts per kind on the rows whose stored proof the event touches, through the existing judgement.
#![allow(clippy::unwrap_used, reason = "test code")]

use super::*;
use crate::arcade_reorg::tests::{
    confirmed_pot_with_stored_proof, pot, CourierStub, FaultyTracker, MemoryTxStore,
};
use crate::proof_fetcher::tests::single_tx_bump;
use crate::reorg_sweep::stored_bump_anchor;
use bsv_rs::transaction::{ChainTrackerError, MockChainTracker};
use overlay_discovery::pot::reorg::BumpAnchor;
use overlay_discovery::pot::storage::MemoryPotStorage;
use std::cell::{Cell, RefCell};

// The stack's docs/CHAIN-EVENTS.md examples, verbatim (the app layer's #771 pins carry the same bytes): the regtest
// witness of the a3-32 scenario (checkpoint 100, A1 101, B1 101, B2 102; a depth-1 fork at 101, then the reorg
// forkHeight 101, deactivated [A1], newTip B2).
const TIP: &str = r#"{"v":1,"kind":"tip","height":102,"hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","time":1757280102,"header":{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"merkleRoot":"0000000000000000baddb2278a30410c9cb24d661391dfe4d0de91080e2538b1","nonce":2,"previousHash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","time":1757280102,"version":536870912}}"#;
const FORK: &str = r#"{"v":1,"kind":"fork","height":101,"competingTips":[{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004","hash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b","height":101,"merkleRoot":"00000000000000000419948426d5797c378cdabd979f3b34a708a6f9e621e3d9","nonce":0,"previousHash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","time":1757280101,"version":536870912},{"bits":545259519,"hash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","height":101,"merkleRoot":"0000000000000000ed99394f49f49e75b96e4deb070e14301c2e57cfa4bc902d","nonce":0,"previousHash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","time":1757280101,"version":536870912,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004"}],"depth":1}"#;
const REORG: &str = r#"{"deactivatedHeaders":[{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004","hash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b","height":101,"merkleRoot":"00000000000000000419948426d5797c378cdabd979f3b34a708a6f9e621e3d9","nonce":0,"previousHash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","time":1757280101,"version":536870912}],"depth":1,"forkHeight":101,"kind":"reorg","newTip":{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"merkleRoot":"0000000000000000baddb2278a30410c9cb24d661391dfe4d0de91080e2538b1","nonce":2,"previousHash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","time":1757280102,"version":536870912},"v":1}"#;
const INVALIDATED: &str = r#"{"v":1,"kind":"invalidated","blockHash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b"}"#;
const FROZEN: &str = r#"{"v":1,"kind":"frozen","outpoint":{"txid":"00000000000000000419948426d5797c378cdabd979f3b34a708a6f9e621e3d9","vout":0}}"#;
const TIP_AGE: &str = r#"{"v":1,"kind":"tipAge","seconds":600,"tip":{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"merkleRoot":"0000000000000000baddb2278a30410c9cb24d661391dfe4d0de91080e2538b1","nonce":2,"previousHash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","time":1757280102,"version":536870912}}"#;
const SPLIT: &str = r#"{"v":2,"kind":"split","key":"split:578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","forkHeight":101,"tips":[{"hash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b","height":101,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004","rungs":["bitails"]},{"hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","rungs":["woc"]}]}"#;
const SPLIT_RESOLVED: &str = r#"{"v":2,"kind":"splitResolved","key":"split:578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","forkHeight":101}"#;
const UNREADABLE: &str = r#"{"v":1,"kind":"unreadable","cursor":2,"fault":"fork depth 0"}"#;
const UNKNOWN_KIND: &str = r#"{"v":1,"kind":"retarget","height":101}"#;
const UNKNOWN_VERSION: &str = r#"{"v":3,"kind":"tip","height":102}"#;
const GO_SHAPE: &str = r#"{"orphanedHashes":["1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b"],"commonAncestor":{"height":100,"hash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85"},"newTip":{"height":102,"hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf"},"depth":1}"#;

/// A header source holding a SET of (height, root) pairs (a single-leaf bump's root is its txid) and a tip.
struct Roots {
    valid: std::sync::Mutex<std::collections::HashSet<(u32, String)>>,
    tip: u32,
}
impl Roots {
    fn at(tip: u32) -> Self {
        Roots {
            valid: std::sync::Mutex::default(),
            tip,
        }
    }
    fn add_root(&self, height: u32, root: String) {
        self.valid.lock().unwrap().insert((height, root));
    }
}
#[async_trait::async_trait]
impl ChainTracker for Roots {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        Ok(self
            .valid
            .lock()
            .unwrap()
            .contains(&(height, root.to_string())))
    }
    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(self.tip)
    }
}

fn decode(s: &str) -> ChainEvent {
    decode_envelope(s.as_bytes()).unwrap()
}

/// One page as the transport answers it (status, body), or its fault.
type PageAnswer = Result<(u16, Vec<u8>), String>;

/// The feed as `GET /events?since=` serves it: pages of at most 100 in cursor order after `since`, `hasMore`, a 409
/// past its head; or a fixed answer.
struct Journal {
    events: RefCell<Vec<(u64, Value)>>,
    calls: RefCell<Vec<String>>,
    answer: RefCell<Option<PageAnswer>>,
}
impl Journal {
    fn of(events: &[(u64, &str)]) -> Self {
        Journal {
            events: RefCell::new(
                events
                    .iter()
                    .map(|(c, e)| (*c, serde_json::from_str(e).unwrap()))
                    .collect(),
            ),
            calls: RefCell::default(),
            answer: RefCell::default(),
        }
    }
}
impl EventFeed for Journal {
    async fn page(&self, since: &str) -> Result<(u16, Vec<u8>), String> {
        self.calls.borrow_mut().push(since.to_string());
        if let Some(a) = self.answer.borrow().clone() {
            return a;
        }
        let since_n: u64 = since.parse().map_err(|_| "400".to_string())?;
        let events = self.events.borrow();
        let head = events.iter().map(|e| e.0).max().unwrap_or(0);
        if since_n > head {
            return Ok((409, br#"{"error":"cursorAhead"}"#.to_vec()));
        }
        let after: Vec<&(u64, Value)> = events.iter().filter(|e| e.0 > since_n).collect();
        let page: Vec<Value> = after
            .iter()
            .take(100)
            .map(|(c, e)| json!({"cursor": c.to_string(), "event": e}))
            .collect();
        let cursor = after
            .iter()
            .take(100)
            .next_back()
            .map_or(since_n, |e| e.0)
            .to_string();
        Ok((
            200,
            json!({"v": 1, "cursor": cursor, "events": page, "hasMore": after.len() > 100})
                .to_string()
                .into_bytes(),
        ))
    }
}

/// The row as D1 holds it: the document's text and its version, a compare-and-set write.
#[derive(Default)]
struct Row {
    doc: RefCell<Option<String>>,
    version: Cell<Option<u64>>,
    writes: Cell<usize>,
}
impl Row {
    fn state(&self) -> FeedState {
        FeedState::from_json(self.doc.borrow().as_deref().unwrap()).unwrap()
    }
}
impl FeedStateStore for Row {
    async fn read(&self) -> Result<Option<(FeedState, u64)>, String> {
        self.doc
            .borrow()
            .as_deref()
            .map(|d| Ok((FeedState::from_json(d)?, self.version.get().unwrap_or(0))))
            .transpose()
    }
    async fn write(
        &self,
        state: &FeedState,
        expected_version: Option<u64>,
        _now_ms: u64,
    ) -> Result<Option<u64>, String> {
        if expected_version != self.version.get() {
            return Ok(None);
        }
        self.writes.set(self.writes.get() + 1);
        *self.doc.borrow_mut() = Some(state.to_json());
        let v = self.version.get().unwrap_or(0) + 1;
        self.version.set(Some(v));
        Ok(Some(v))
    }
}

fn limits(events: u32, leg: u64, fault_ceiling: u32) -> PassLimits {
    PassLimits {
        events_per_pass: events,
        leg_limit: leg,
        fault_ceiling,
        ..PassLimits::cron()
    }
}

#[allow(clippy::too_many_arguments)]
async fn pass(
    feed: &Journal,
    row: &Row,
    store: &MemoryPotStorage,
    txs: &MemoryTxStore,
    tracker: &dyn ChainTracker,
    ladder: &CourierStub,
    lim: PassLimits,
    start: &Start,
) -> FeedPassSummary {
    consume_chain_feed(
        feed,
        row,
        store,
        Some(txs),
        Some(tracker),
        Some(ladder),
        lim,
        &mut RootMemo::default(),
        start,
        1_792_504_017_000,
    )
    .await
}

/// (a) THE REORG WORD. A journal of the stack's own envelopes (a fork at 101, the reorg from 101 to the new tip 102,
/// a tip, a tip age) is consumed from the configured start: each envelope through the crate's gate; the fork and the
/// reorg select the rows anchored in their windows (101, then 101..=102) and each row is judged by ITS OWN stored proof
/// against the active root at its height: a standing proof stands untouched; a refuted one the ladder re-proves is
/// RE-ANCHORED IN PLACE at 102 (never through SEEN); a refuted one no courier proves is DEMOTED, its proof unlatched,
/// its bytes kept; a refuted row BELOW the window (100) is never examined. The cursor passes every event, the kinds
/// are counted once each, the tip is recorded, and the D7 and R2 counters see the same rows.
#[tokio::test]
async fn e620_a_the_reorg_event_reanchors_reproven_rows_and_demotes_the_rest() {
    let store = MemoryPotStorage::new();
    let tracker = Roots::at(102);
    let standing = confirmed_pot_with_stored_proof(&store, &pot(1), 101).await;
    tracker.add_root(101, standing.clone());
    let reproven = confirmed_pot_with_stored_proof(&store, &pot(2), 101).await;
    tracker.add_root(102, reproven.clone());
    let orphaned = confirmed_pot_with_stored_proof(&store, &pot(3), 101).await;
    let below = confirmed_pot_with_stored_proof(&store, &pot(4), 100).await; // refuted, outside every window
    let above = confirmed_pot_with_stored_proof(&store, &pot(5), 102).await;
    tracker.add_root(102, above.clone());
    let ladder = CourierStub::with(
        [(
            reproven.clone(),
            Ok(Some(single_tx_bump(&reproven, 102).to_hex())),
        )]
        .into_iter()
        .collect(),
    );
    let feed = Journal::of(&[(41, FORK), (42, REORG), (43, TIP), (44, TIP_AGE)]);
    let row = Row::default();
    let txs = MemoryTxStore::default();
    let s = pass(
        &feed,
        &row,
        &store,
        &txs,
        &tracker,
        &ladder,
        limits(2, 50, 12),
        &Start::At("40".into()),
    )
    .await;
    assert_eq!(s.seeded.as_deref(), Some("40"));
    assert_eq!(
        (s.applied, s.released, s.faults, s.errors, s.contended),
        (2, 0, 0, 0, 0),
        "{s:?}"
    );
    assert_eq!((s.reanchored(), s.demoted()), (1, 1), "{s:?}");
    assert_eq!(
        ladder.calls.get(),
        2,
        "the ladder asked once per REFUTED row"
    );
    let r = store.get_spent_status(&pot(1), 0).await.unwrap().unwrap();
    assert!(r.spent_confirmed && r.spent_height == Some(101), "standing");
    let r = store.get_spent_status(&pot(2), 0).await.unwrap().unwrap();
    assert!(r.spent_confirmed, "a re-anchor never passes through SEEN");
    assert_eq!(r.spent_height, Some(102));
    assert_eq!(
        stored_bump_anchor(
            &store.get_beef(&reproven).await.unwrap().unwrap(),
            &reproven
        ),
        Some(BumpAnchor {
            height: 102,
            root: reproven.clone()
        })
    );
    let r = store.get_spent_status(&pot(3), 0).await.unwrap().unwrap();
    assert!(
        r.spent && !r.spent_confirmed && r.spent_height.is_none(),
        "demoted"
    );
    assert_eq!(r.spending_txid.as_deref(), Some(orphaned.as_str()));
    assert!(!store.pot_beef_proof_verified(&orphaned).await.unwrap());
    assert!(store.get_beef(&orphaned).await.unwrap().is_some());
    assert!(
        store
            .get_spent_status(&pot(4), 0)
            .await
            .unwrap()
            .unwrap()
            .spent_confirmed,
        "a row below the window is never examined"
    );
    assert!(store.pot_beef_proof_verified(&below).await.unwrap());
    assert!(
        store
            .get_spent_status(&pot(5), 0)
            .await
            .unwrap()
            .unwrap()
            .spent_confirmed
    );
    let st = row.state();
    assert_eq!((st.cursor.as_str(), st.pending.clone()), ("44", None));
    assert_eq!(st.tip_height, Some(102));
    assert_eq!(st.last_kind.as_deref(), Some("tip_age"));
    assert_eq!(
        s.consumed,
        BTreeMap::from([("fork", 1), ("reorg", 1), ("tip", 1), ("tip_age", 1)])
    );
    let c = s.counters();
    let n = |name: &str| c.iter().find(|(k, _)| k == name).map_or(0, |(_, n)| *n);
    assert_eq!(
        (
            n("overlay_chain_events_total"),
            n("overlay_chain_events_reorg_total"),
            n("overlay_chain_events_applied_total"),
            n("overlay_chain_events_reanchored_total"),
            n("overlay_chain_events_demoted_total"),
            n("overlay_chain_events_pass_total"),
        ),
        (4, 1, 2, 1, 1, 1)
    );
    let totals = s.reorg_totals();
    assert!(totals.contains(&(crate::ops::COUNTER_REORG_DEMOTED, 1)));
    assert!(totals.contains(&(crate::ops::COUNTER_REORG_REANCHORED, 1)));
    assert!(s.is_reorg_word());
    assert!(crate::hop_probe_memos::feed_pass_rejudged(&s));
    // at least once: the same journal again moves nothing and asks no courier
    let again = pass(
        &feed,
        &row,
        &store,
        &txs,
        &tracker,
        &ladder,
        limits(2, 50, 12),
        &Start::At("40".into()),
    )
    .await;
    assert_eq!(
        (again.applied, again.consumed.len(), ladder.calls.get()),
        (0, 0, 2)
    );
    assert!(again.idle);
    assert!(!crate::hop_probe_memos::feed_pass_rejudged(&again));
}

/// (a2) The selection per kind: the crate's own `on_chain` set, each window closed at the highest height the event
/// names; an invalidation names no height (below the tip); tips, tip ages, frozen outputs and resolved splits select
/// nothing.
#[test]
fn e620_a2_each_kind_selects_its_window() {
    assert_eq!(selection(&decode(REORG)), Selection::Heights(101, 102));
    assert_eq!(selection(&decode(FORK)), Selection::Heights(101, 101));
    assert_eq!(selection(&decode(SPLIT)), Selection::Heights(101, 102));
    assert_eq!(selection(&decode(INVALIDATED)), Selection::BelowTip);
    for none in [TIP, TIP_AGE, FROZEN, SPLIT_RESOLVED] {
        assert_eq!(selection(&decode(none)), Selection::None, "{none}");
    }
}

/// (b) THE GATE STOPS THE PULL. An envelope `decode_envelope` refuses (an unknown kind, an unknown version, the
/// Go server's kindless shape, the feed's own `unreadable`) stops the pull AT its cursor: the events before it are
/// consumed, nothing past it (a reorg behind it demotes nothing), the stop recorded on the row and counted once
/// across passes; the operator's lever moves the cursor past it and the next pass consumes the reorg.
#[tokio::test]
async fn e620_b_an_envelope_the_crate_refuses_stops_the_pull_at_its_cursor() {
    for bad in [UNKNOWN_KIND, UNKNOWN_VERSION, GO_SHAPE, UNREADABLE] {
        let store = MemoryPotStorage::new();
        let tracker = MockChainTracker::new(102);
        let orphaned = confirmed_pot_with_stored_proof(&store, &pot(3), 101).await;
        let feed = Journal::of(&[(41, TIP), (42, bad), (43, REORG)]);
        let row = Row::default();
        let txs = MemoryTxStore::default();
        let ladder = CourierStub::none();
        let start = Start::At("40".into());
        let run = || {
            pass(
                &feed,
                &row,
                &store,
                &txs,
                &tracker,
                &ladder,
                limits(2, 50, 12),
                &start,
            )
        };
        let s = run().await;
        let st = row.state();
        assert_eq!(st.cursor, "41", "{bad}: the tip before it consumed");
        assert_eq!(
            st.stop.as_ref().map(|x| x.cursor.as_str()),
            Some("42"),
            "{bad}"
        );
        assert!(s.unreadable, "{bad}");
        assert_eq!(s.applied, 0);
        assert!(
            store.pot_beef_proof_verified(&orphaned).await.unwrap(),
            "{bad}: nothing past the stop"
        );
        let again = run().await;
        assert!(!again.unreadable, "{bad}: counted once");
        assert_eq!(row.state().cursor, "41");
        let set = lever(&row, true, br#"{"cursor":"42"}"#, 2).await;
        assert_eq!(set.status, 200, "{}", set.body);
        assert_eq!(
            set.counters,
            vec![("overlay_chain_events_cursor_set_total".to_string(), 1)]
        );
        let on = run().await;
        assert_eq!(on.applied, 1, "{bad}: {on:?}");
        assert!(!store.pot_beef_proof_verified(&orphaned).await.unwrap());
        assert_eq!(row.state().stop, None);
    }
}

/// (c) A WINDOW WIDER THAN A PAGE IS HELD; A FAULTING ONE IS RELEASED AND RECORDED. One row a leg a pass: the reorg's
/// window is held as `pending` (each leg's cursor in the row) and walked on across passes, the cursor not passed until
/// every leg is exhausted. Under a header source that cannot be read, the window faults each pass, keeps its cursor,
/// and at the fault ceiling is released unresolved with its heal recorded, the cursor passed, nothing demoted.
#[tokio::test]
async fn e620_c_a_wide_window_is_held_and_a_faulting_one_released_with_its_heal() {
    let store = MemoryPotStorage::new();
    let tracker = Roots::at(102);
    for i in 1..=3 {
        let t = confirmed_pot_with_stored_proof(&store, &pot(i), 101).await;
        tracker.add_root(101, t);
    }
    let feed = Journal::of(&[(41, REORG)]);
    let row = Row::default();
    let txs = MemoryTxStore::default();
    let ladder = CourierStub::none();
    let mut passes = 0;
    loop {
        passes += 1;
        let s = pass(
            &feed,
            &row,
            &store,
            &txs,
            &tracker,
            &ladder,
            limits(2, 1, 12),
            &Start::At("40".into()),
        )
        .await;
        if s.applied == 1 {
            break;
        }
        let st = row.state();
        assert_eq!(st.cursor, "40", "not passed while a leg is open");
        assert_eq!(st.pending.as_ref().map(|p| (p.lo, p.hi)), Some((101, 102)));
        assert!(passes < 20);
    }
    assert!(passes >= 3, "one row a leg a pass: {passes}");
    assert_eq!(row.state().cursor, "41");

    let store = MemoryPotStorage::new();
    let spender = confirmed_pot_with_stored_proof(&store, &pot(9), 101).await;
    let row = Row::default();
    let faulty = FaultyTracker;
    for k in 1..=2 {
        let s = pass(
            &feed,
            &row,
            &store,
            &txs,
            &faulty,
            &ladder,
            limits(2, 50, 2),
            &Start::At("40".into()),
        )
        .await;
        assert_eq!((s.faults, s.applied, s.released), (1, 0, 0), "{s:?}");
        assert_eq!(row.state().pending.as_ref().unwrap().fault_passes, k);
        assert_eq!(row.state().cursor, "40");
    }
    let s = pass(
        &feed,
        &row,
        &store,
        &txs,
        &faulty,
        &ladder,
        limits(2, 50, 2),
        &Start::At("40".into()),
    )
    .await;
    assert_eq!(s.released, 1, "{s:?}");
    let st = row.state();
    assert_eq!((st.cursor.as_str(), st.pending.clone()), ("41", None));
    assert_eq!(st.unresolved.len(), 1);
    assert_eq!(
        (
            st.unresolved[0].lo,
            st.unresolved[0].hi,
            st.unresolved[0].why.as_str()
        ),
        (101, 102, "fault-ceiling")
    );
    assert_eq!(
        state_json(&st, None, None)["unresolved"][0]["heal"],
        json!("POST /internal/reorg {\"fromHeight\": 101, \"toHeight\": 102}")
    );
    assert!(
        store.pot_beef_proof_verified(&spender).await.unwrap(),
        "nothing demoted"
    );
    assert!(
        crate::hop_probe_memos::feed_pass_rejudged(&s),
        "a release is evidence"
    );
}

/// (d) THE START AND THE 409. With no row and no start cursor the consumer asks the feed nothing, writes nothing and
/// is not the reorg word (D7 stays the input); with a start it seeds the row there. A 409 `cursorAhead` is a stop
/// recorded at the stored cursor (counted once), never a reset to 0. An `invalidated` selects the heights below the
/// tracker's tip.
#[tokio::test]
async fn e620_d_unconfigured_reads_nothing_and_a_409_is_a_stop_not_a_reset() {
    let store = MemoryPotStorage::new();
    let tracker = MockChainTracker::new(1_000);
    let txs = MemoryTxStore::default();
    let ladder = CourierStub::none();
    let feed = Journal::of(&[(41, TIP)]);
    let row = Row::default();
    for start in [Start::Unset, Start::of(Some("twelve"))] {
        let s = pass(
            &feed,
            &row,
            &store,
            &txs,
            &tracker,
            &ladder,
            limits(2, 50, 12),
            &start,
        )
        .await;
        assert!(s.unconfigured && !s.is_reorg_word());
        assert_eq!(
            s.counters()
                .iter()
                .find(|(k, _)| k == "overlay_chain_events_unconfigured_total")
                .map(|c| c.1),
            Some(1)
        );
    }
    assert!(feed.calls.borrow().is_empty() && row.writes.get() == 0);
    let s = pass(
        &feed,
        &row,
        &store,
        &txs,
        &tracker,
        &ladder,
        limits(2, 50, 12),
        &Start::At("999".into()),
    )
    .await;
    assert!(s.cursor_ahead && s.is_reorg_word(), "{s:?}");
    let st = row.state();
    assert_eq!(st.cursor, "999", "never reset to 0");
    assert_eq!(
        st.stop
            .as_ref()
            .map(|x| (x.cursor.as_str(), x.fault.as_str())),
        Some(("999", "cursorAhead"))
    );
    let again = pass(
        &feed,
        &row,
        &store,
        &txs,
        &tracker,
        &ladder,
        limits(2, 50, 12),
        &Start::Unset,
    )
    .await;
    assert!(
        !again.cursor_ahead,
        "counted once; a present row ignores the start"
    );

    let row = Row::default();
    let feed = Journal::of(&[(41, INVALIDATED)]);
    let s = pass(
        &feed,
        &row,
        &store,
        &txs,
        &tracker,
        &ladder,
        limits(2, 50, 12),
        &Start::At("40".into()),
    )
    .await;
    assert_eq!(s.applied, 1, "{s:?}");
    assert_eq!(row.state().last_kind.as_deref(), Some("invalidated"));
}

/// (e) THE LEVER. `GET` answers the row; `POST {"cursor"}` sets it by the row's compare-and-set, an event in hand
/// released and RECORDED (`operator`, with its heal), never dropped silently; named 400s.
#[tokio::test]
async fn e620_e_the_lever_sets_the_cursor_and_records_an_event_in_hand() {
    let row = Row::default();
    assert_eq!(lever(&row, false, b"", 1).await.body, json!({"row": null}));
    let mut st = FeedState::seeded("40", 1);
    st.pending = Some(PendingWindow::new(41, "reorg", 101, 102));
    *row.doc.borrow_mut() = Some(st.to_json());
    row.version.set(Some(3));
    let read = lever(&row, false, b"", 1).await;
    assert_eq!(read.body["row"]["pending"]["fromHeight"], json!(101));
    let set = lever(&row, true, br#"{"cursor":"41"}"#, 5).await;
    assert_eq!(set.status, 200, "{}", set.body);
    let st = row.state();
    assert_eq!((st.cursor.as_str(), st.pending.clone()), ("41", None));
    assert_eq!(
        (st.unresolved[0].why.as_str(), st.unresolved[0].lo),
        ("operator", 101)
    );
    for (body, want) in [
        (&b"nope"[..], 400),
        (br#"{}"#, 400),
        (br#"{"cursor":7}"#, 400),
        (br#"{"cursor":"-1"}"#, 400),
    ] {
        assert_eq!(lever(&row, true, body, 6).await.status, want);
    }
}

/// (f) THE ROW'S STATEMENTS under real SQLite with the production migrations: the table is migration 347; the first
/// write inserts version 1 and answers it; a write against the version read replaces the document and answers the
/// next; a write against a stale version, or a first write over a present row, answers no row and changes nothing.
#[test]
fn e620_f_the_rows_compare_and_set_under_real_sqlite() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for sql in crate::d1::OVERLAY_MIGRATIONS {
        conn.execute_batch(sql).unwrap();
    }
    assert_eq!(crate::d1::OVERLAY_MIGRATIONS.last(), Some(&CREATE));
    assert_eq!(
        crate::d1::OVERLAY_MIGRATIONS.len(),
        crate::d1::OVERLAY_MIGRATION_COUNT
    );
    let write = |doc: &str, expected: Option<i64>| -> Option<i64> {
        conn.query_row(STATE_WRITE, rusqlite::params![doc, 7, expected], |r| {
            r.get(0)
        })
        .ok()
    };
    let doc = |c: &str| FeedState::seeded(c, 1).to_json();
    assert_eq!(write(&doc("40"), None), Some(1));
    assert_eq!(
        write(&doc("41"), None),
        None,
        "a first write over a present row"
    );
    assert_eq!(write(&doc("42"), Some(1)), Some(2));
    assert_eq!(write(&doc("43"), Some(1)), None, "a stale version");
    let (state, version): (String, i64) = conn
        .query_row(STATE_READ, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!(
        (
            FeedState::from_json(&state).unwrap().cursor.as_str(),
            version
        ),
        ("42", 2)
    );
    let mut newer = FeedState::seeded("1", 1);
    newer.v = FEED_STATE_VERSION + 1;
    assert!(
        FeedState::from_json(&newer.to_json()).is_err(),
        "a newer document is unreadable"
    );
}

/// (g) THE WIRING. Both consumers judge through ONE function (`judge_window_legs`: the spenders by their own proof,
/// re-anchor first, never a blind demotion; the pots' and the hop proofs after). The cron and the block-event pass run
/// the feed first and run D7's Arcade consumer only where the feed is not the reorg word; the feed's pass bumps its
/// counters and the R2 totals and clears the confirmed probe memos on evidence, as D7's pass does; the operator's
/// routes and `/health/invariants` serve both.
#[test]
fn e620_g_the_feed_is_wired_as_the_reorg_word_and_d7_is_gated() {
    let squash = |s: &str| -> String {
        s.lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<String>()
            .split_whitespace()
            .collect()
    };
    let arcade = squash(include_str!("arcade_reorg.rs"));
    let judge = &arcade[arcade.find("pubasyncfnjudge_window_legs<").unwrap()..];
    let judge = &judge[..judge.find("fnwindow_name(").unwrap()];
    assert!(judge.contains("ReverifyMode{demote_proofless:false,reanchor_first:true,}"));
    for leg in [
        "reverify_window_with(",
        "reverify_pot_beefs_window_with(",
        "reverify_transactions_window_with(",
    ] {
        assert_eq!(judge.matches(leg).count(), 1, "{leg}");
    }
    let consume = &arcade[arcade
        .find("pubasyncfnconsume_arcade_reorg_events<")
        .unwrap()..];
    let consume = &consume[..consume.find("pubenumWindowLegs").unwrap()];
    assert!(
        consume.contains("judge_window_legs("),
        "D7 judges through the shared function"
    );
    assert!(
        !consume.contains("reverify_window_with("),
        "and through nothing else"
    );
    let feed = squash(include_str!("chain_feed.rs"));
    let feed_consume = &feed[feed.find("pubasyncfnconsume_chain_feed<").unwrap()..];
    let feed_consume = &feed_consume[..feed_consume.find("fnkind_of_name(").unwrap()];
    assert!(feed_consume.contains("judge_window_legs("));
    assert!(!feed_consume.contains("reverify_window_with("));
    assert!(feed_consume.contains("decode_envelope(&bytes)"));
    let run = &feed[feed.find("pubasyncfnrun_chain_feed_pass(").unwrap()..];
    assert!(run.contains("crate::hop_probe_memos::clear_on_reorg(db,origin,crate::hop_probe_memos::feed_pass_rejudged(&s),)"));
    assert!(run.contains("for(name,n)ins.reorg_totals()"));

    let tick = squash(include_str!("tick.rs"));
    let at_feed = tick
        .find("crate::chain_feed::run_chain_feed_pass(")
        .expect("the cron runs the feed");
    let at_gate = tick.find("iffeed.is_reorg_word(){").expect("the gate");
    let at_d7 = tick
        .find("crate::tip_pass::run_arcade_reorg_pass(")
        .unwrap();
    assert!(at_feed < at_gate && at_gate < at_d7);
    assert!(
        tick[at_gate..at_d7].contains("}else{"),
        "D7 only in the else arm"
    );

    let tip = squash(include_str!("tip_pass.rs"));
    let tip = &tip[..tip.find("#[cfg(test)]").unwrap_or(tip.len())];
    let changed = &tip[tip.find("pubasyncfninternal_tip_changed(").unwrap()..];
    let changed = &changed[..changed.find("pubasyncfnrun_arcade_reorg_pass(").unwrap()];
    let at_feed = changed
        .find("crate::chain_feed::run_chain_feed_pass(")
        .expect("the block-event pass runs the feed");
    let at_gate = changed
        .find("is_some_and(crate::chain_feed::FeedPassSummary::is_reorg_word)")
        .unwrap();
    let at_d7 = changed.find("run_arcade_reorg_pass(").unwrap();
    assert!(at_feed < at_gate && at_gate < at_d7);
    assert!(
        changed.find("reorg_revalidation_sweep_with(").unwrap() > at_d7,
        "both before the routine sweep, which takes the same memo"
    );

    let lib = squash(include_str!("lib.rs"));
    assert!(lib.contains("(Method::Get|Method::Post,\"/internal/chain-events-cursor\")=>{crate::chain_feed::internal_cursor(req,&env,Some(&ops_db)).await}"));
    assert!(
        lib.contains("(Method::Post,\"/internal/arcade-reorg\")"),
        "D7's operator route kept"
    );
    let ops = squash(include_str!("ops.rs"));
    assert!(ops.contains("chain_feed=crate::chain_feed::health_json(db),"));
    assert!(ops.contains("body[\"chainFeed\"]=chain_feed;"));
    assert!(
        ops.contains("\"arcadeReorg\":arcade_reorg,"),
        "D7's health kept"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// W15E-L4-LENS (2026-10-23): the lens's own cases, built in the lens worktree against PR 154's head. Not the lane's
// pins; each documents a behaviour the lens examined. They reuse this file's fixtures.
// ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════

/// A feed whose page never answers (the binding hung): the model of a pass dropped at a wall.
struct Hang;
impl EventFeed for Hang {
    async fn page(&self, _since: &str) -> Result<(u16, Vec<u8>), String> {
        std::future::pending::<()>().await;
        unreachable!()
    }
}

/// A row whose every write loses its compare-and-set (another isolate owns the pass).
struct Contend(FeedState);
impl FeedStateStore for Contend {
    async fn read(&self) -> Result<Option<(FeedState, u64)>, String> {
        Ok(Some((self.0.clone(), 1)))
    }
    async fn write(
        &self,
        _state: &FeedState,
        _expected_version: Option<u64>,
        _now_ms: u64,
    ) -> Result<Option<u64>, String> {
        Ok(None)
    }
}

/// The gate as a LIVENESS predicate (the lens's fix shape for L1): the feed is the reorg word only while it is not
/// resting on a recorded stop and read its page this pass.
fn feed_is_live(s: &FeedPassSummary) -> bool {
    s.is_reorg_word() && s.faults == 0 && s.state.as_ref().is_some_and(|st| st.stop.is_none())
}

/// LENS L1 (permanence): a feed STOPPED at an envelope the crate refuses, at a 409, or faulting at the binding, still
/// answers `is_reorg_word() == true` whenever its row read, so the cron and the block-event pass skip Arcade's orphan
/// consumer on every pass of the stop; a reorg BEHIND the stop demotes nothing on any of them. The row's state is the
/// one `tick.rs` and `tip_pass.rs` consult. The liveness predicate answers false in each stopped shape and true in the
/// healthy one.
#[tokio::test]
async fn lens_l1_a_stopped_feed_still_gates_d7_off() {
    // (1) an unreadable envelope with a reorg behind it
    let store = MemoryPotStorage::new();
    let tracker = MockChainTracker::new(102);
    let orphaned = confirmed_pot_with_stored_proof(&store, &pot(3), 101).await;
    let feed = Journal::of(&[(41, TIP), (42, UNKNOWN_KIND), (43, REORG)]);
    let row = Row::default();
    let txs = MemoryTxStore::default();
    let ladder = CourierStub::none();
    let start = Start::At("40".into());
    for n in 1..=3 {
        let s = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &start).await;
        assert!(s.is_reorg_word(), "pass {n}: the gate reads the stopped feed as the reorg word: {s:?}");
        assert!(!feed_is_live(&s), "pass {n}: a liveness predicate would hand D7 back");
        assert_eq!(s.applied, 0);
        assert!(
            store.pot_beef_proof_verified(&orphaned).await.unwrap(),
            "pass {n}: the reorg behind the stop demotes nothing; no other consumer ran"
        );
        assert_eq!(row.state().stop.as_ref().map(|x| x.cursor.as_str()), Some("42"));
    }
    // (2) a 409 cursorAhead (pin d pins `cursor_ahead && is_reorg_word`; stated here beside the liveness reading)
    let row = Row::default();
    let feed = Journal::of(&[(41, TIP)]);
    let s = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::At("999".into())).await;
    assert!(s.cursor_ahead && s.is_reorg_word() && !feed_is_live(&s), "{s:?}");
    let again = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::Unset).await;
    assert!(again.is_reorg_word() && !feed_is_live(&again), "{again:?}");
    // (3) the binding down, the row present
    let row = Row::default();
    let feed = Journal::of(&[(41, TIP)]);
    let seeded = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::At("40".into())).await;
    assert!(seeded.is_reorg_word() && feed_is_live(&seeded), "{seeded:?}");
    *feed.answer.borrow_mut() = Some(Err("CHAINTRACKS binding: connection refused".into()));
    let down = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::Unset).await;
    assert_eq!(down.faults, 1, "{down:?}");
    assert!(down.is_reorg_word(), "a transport fault with a row present still gates D7 off this pass");
    assert!(!feed_is_live(&down));
    // (4) the healthy shape (pin a's journal) is live under both readings
    let store = MemoryPotStorage::new();
    let tracker = Roots::at(102);
    let standing = confirmed_pot_with_stored_proof(&store, &pot(1), 101).await;
    tracker.add_root(101, standing);
    let feed = Journal::of(&[(41, FORK), (42, REORG), (43, TIP), (44, TIP_AGE)]);
    let row = Row::default();
    let s = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::At("40".into())).await;
    assert_eq!(s.applied, 2, "{s:?}");
    assert!(s.is_reorg_word() && feed_is_live(&s));
}

/// LENS L2 (selection edges): a reorg `forkHeight` 101, deactivated [101], newTip 102 selects exactly 101..=102. A
/// refuted row at 100 (the common ancestor's height) and a refuted row at 103 (above the new tip) are not examined;
/// the refuted row at 101 is demoted; the standing row at 102 stands. The spenders leg scanned two rows.
#[tokio::test]
async fn lens_l2_the_reorg_window_is_exact_at_both_edges() {
    let store = MemoryPotStorage::new();
    let tracker = Roots::at(103);
    let below = confirmed_pot_with_stored_proof(&store, &pot(10), 100).await; // refuted, below lo
    let at_lo = confirmed_pot_with_stored_proof(&store, &pot(11), 101).await; // refuted, at lo
    let at_hi = confirmed_pot_with_stored_proof(&store, &pot(12), 102).await; // standing, at hi
    tracker.add_root(102, at_hi.clone());
    let above = confirmed_pot_with_stored_proof(&store, &pot(13), 103).await; // refuted, above hi
    let feed = Journal::of(&[(41, REORG)]);
    let row = Row::default();
    let txs = MemoryTxStore::default();
    let ladder = CourierStub::none();
    let s = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::At("40".into())).await;
    assert_eq!((s.applied, s.spenders.scanned, s.demoted()), (1, 2, 1), "{s:?}");
    let verified = |t: &str| {
        let store = &store;
        let t = t.to_string();
        async move { store.pot_beef_proof_verified(&t).await.unwrap() }
    };
    assert!(verified(&below).await, "100 is the ancestor's height: never examined");
    assert!(!verified(&at_lo).await, "101 is the lowest affected height: judged and demoted");
    assert!(verified(&at_hi).await, "102 stands by its own proof");
    assert!(verified(&above).await, "103 is above the new tip: not this event's (a later reorg names it)");
    assert_eq!(row.state().cursor, "41");
}

/// LENS L3 (the invalidated window): with chaintracks' tip at 1000 an `invalidated` selects 856..=1000. A refuted row
/// at 856 is demoted; a refuted row at 855, one deeper than `INVALIDATED_DEPTH`, is not examined and keeps its
/// confirmation (the stated limit: deeper is the operator's `/internal/reorg`, or the `reorg` event that follows an
/// invalidation that moves the tip).
#[tokio::test]
async fn lens_l3_an_invalidation_145_deep_is_not_examined() {
    let store = MemoryPotStorage::new();
    let tracker = Roots::at(1000);
    let inside = confirmed_pot_with_stored_proof(&store, &pot(20), 856).await;
    let deeper = confirmed_pot_with_stored_proof(&store, &pot(21), 855).await;
    let feed = Journal::of(&[(41, INVALIDATED)]);
    let row = Row::default();
    let txs = MemoryTxStore::default();
    let ladder = CourierStub::none();
    let s = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::At("40".into())).await;
    assert_eq!((s.applied, s.spenders.scanned, s.demoted()), (1, 1, 1), "{s:?}");
    assert!(!store.pot_beef_proof_verified(&inside).await.unwrap(), "856 = tip - 144: judged, demoted");
    assert!(store.pot_beef_proof_verified(&deeper).await.unwrap(), "855 = tip - 145: not examined");
    assert_eq!(INVALIDATED_DEPTH, 144);
}

/// LENS L4 (both consumers on one deployment in one tick): the feed applies the reorg at 101 (the refuted row
/// demoted); D7's consumer, run on demand over an Arcade orphan event at the same height (`POST
/// /internal/arcade-reorg` is kept), judges the same height through the same function and demotes nothing a second
/// time: the demoted row is no longer a confirmed row, the standing one stands. Each consumer keeps its own cursor
/// over its own source; neither reads the other's row.
#[tokio::test]
async fn lens_l4_two_consumers_one_height_demote_once() {
    use crate::arcade_reorg::{
        consume_arcade_reorg_events, CanonicalHeader, ConsumerStateStore, FeedRead, HeaderSource, OrphanFeed,
    };
    use overlay_discovery::pot::arcade_events::{ConsumerState, OrphanEvent};
    struct OneOrphan(OrphanEvent);
    impl OrphanFeed for OneOrphan {
        async fn recent_orphans(&self) -> Result<FeedRead, String> {
            Ok(FeedRead {
                events: vec![self.0.clone()],
                rows: 1,
                malformed: 0,
                pages: 1,
                newest_height: Some(102),
            })
        }
    }
    struct Canon(String);
    impl HeaderSource for Canon {
        async fn hash_at(&self, height: u64) -> Result<Option<CanonicalHeader>, String> {
            Ok((height == 101).then(|| CanonicalHeader {
                hash: self.0.clone(),
                merkle_root: None,
            }))
        }
        async fn tip(&self) -> Result<u64, String> {
            Ok(102)
        }
    }
    #[derive(Default)]
    struct D7Row {
        doc: RefCell<Option<String>>,
        version: Cell<Option<u64>>,
    }
    impl ConsumerStateStore for D7Row {
        async fn read(&self) -> Result<(ConsumerState, Option<u64>), String> {
            let doc = self.doc.borrow();
            let state: ConsumerState = serde_json::from_str(doc.as_deref().unwrap_or("{}")).unwrap();
            Ok((state, self.version.get()))
        }
        async fn write(&self, state: &ConsumerState, expected: Option<u64>) -> Result<Option<u64>, String> {
            if expected != self.version.get() {
                return Ok(None);
            }
            *self.doc.borrow_mut() = Some(serde_json::to_string(state).unwrap());
            let v = self.version.get().unwrap_or(0) + 1;
            self.version.set(Some(v));
            Ok(Some(v))
        }
    }
    let store = MemoryPotStorage::new();
    let tracker = Roots::at(102);
    let standing = confirmed_pot_with_stored_proof(&store, &pot(1), 101).await;
    tracker.add_root(101, standing.clone());
    let orphaned = confirmed_pot_with_stored_proof(&store, &pot(3), 101).await;
    let txs = MemoryTxStore::default();
    let ladder = CourierStub::none();
    // the feed first, as the cron and the block-event pass run it
    let feed = Journal::of(&[(41, REORG)]);
    let row = Row::default();
    let s = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::At("40".into())).await;
    assert_eq!((s.applied, s.spenders.scanned, s.demoted()), (1, 2, 1), "{s:?}");
    assert!(!store.pot_beef_proof_verified(&orphaned).await.unwrap());
    // then D7 on demand over Arcade's orphan row at 101 (its hash differs from chaintracks': corroborated)
    let arcade_feed = OneOrphan(OrphanEvent {
        orphaned_at: "2026-10-23T18:00:00.000Z".into(),
        height: 101,
        hash: "1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b".into(),
    });
    let headers = Canon("11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105".into());
    let d7_row = D7Row::default();
    let d7 = consume_arcade_reorg_events(
        &arcade_feed,
        &headers,
        &d7_row,
        &store,
        Some(&txs),
        Some(&tracker),
        Some(&ladder),
        PassLimits::cron(),
        &mut RootMemo::default(),
        false,
    )
    .await;
    assert_eq!(d7.applied, 1, "{d7:?}");
    assert_eq!((d7.spenders.scanned, d7.spenders.stale, d7.spenders.standing), (1, 0, 1), "{d7:?}");
    assert_eq!(ladder.calls.get(), 1, "the ladder was asked once, by the feed's pass, for the one refuted row");
    assert!(store.pot_beef_proof_verified(&standing).await.unwrap());
    let r = store.get_spent_status(&pot(3), 0).await.unwrap().unwrap();
    assert!(r.spent && !r.spent_confirmed, "demoted once, still demoted");
    assert_eq!(row.state().cursor, "41", "the feed's cursor is its own");
    assert!(d7_row.doc.borrow().as_deref().unwrap().contains("2026-10-23T18:00:00.000Z"), "D7's cursor is its own");
}

/// LENS L5 (the invariants read): `chain_feed::health_json` is ONE statement (`STATE_READ`), issued inside the
/// handler's one join (the fix838 pin lists it), so `/health/invariants` carries one statement more than the tier's
/// measured 32 on main `35ddb423`: 33 of the ceiling's 34 (arithmetic, not a tier run).
#[test]
fn lens_l5_the_health_read_is_one_statement() {
    let src = include_str!("chain_feed.rs");
    let body = &src[src.find("pub async fn health_json(").unwrap()..];
    let body = &body[..body.find("\n}\n").unwrap()];
    assert_eq!(body.matches("Query::new(").count(), 1, "one statement");
    assert!(body.contains("Query::new(STATE_READ)"));
}

/// LENS L6 (the wall inside the feed): a pass whose feed page never answers, dropped by the caller's race, leaves the
/// row where the seed put it (nothing judged, nothing written after the seed) and the next pass continues from it. In
/// production the page GET is raced at `FEED_TIMEOUT_MS` and the cron's whole step list at `TICK_BUDGET_MS`; the
/// block-event handler has no wall of its own around the feed.
#[tokio::test]
async fn lens_l6_a_pass_dropped_at_a_wall_keeps_the_row() {
    let store = MemoryPotStorage::new();
    let tracker = MockChainTracker::new(102);
    let txs = MemoryTxStore::default();
    let ladder = CourierStub::none();
    let row = Row::default();
    let dropped = overlay_engine::gasp::race_or_deadline(
        consume_chain_feed(
            &Hang,
            &row,
            &store,
            Some(&txs),
            Some(&tracker),
            Some(&ladder),
            limits(2, 50, 12),
            &mut RootMemo::default(),
            &Start::At("40".into()),
            1,
        ),
        tokio::time::sleep(std::time::Duration::from_millis(50)),
    )
    .await;
    assert!(dropped.is_none(), "the pass was dropped at the wall");
    assert_eq!((row.state().cursor.as_str(), row.writes.get()), ("40", 1), "the seed landed, nothing after it");
    let feed = Journal::of(&[(41, TIP)]);
    let s = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::Unset).await;
    assert_eq!((row.state().cursor.as_str(), s.consumed.get("tip").copied()), ("41", Some(1)));
    assert_eq!(FEED_TIMEOUT_MS, 10_000);
}

/// LENS L8 (a contended write is counted twice): the in-loop `write_now!` after an applied window loses its
/// compare-and-set and the pass breaks; the trailing `write_now!` tries the same stale version again and counts a
/// second contention. Cosmetic (the counter and the `stopped` text), stated.
#[tokio::test]
async fn lens_l8_a_contended_pass_counts_its_contention_twice() {
    let store = MemoryPotStorage::new();
    let tracker = Roots::at(102);
    let standing = confirmed_pot_with_stored_proof(&store, &pot(1), 101).await;
    tracker.add_root(101, standing);
    let txs = MemoryTxStore::default();
    let ladder = CourierStub::none();
    let feed = Journal::of(&[(41, REORG)]);
    let contend = Contend(FeedState::seeded("40", 1));
    let s = consume_chain_feed(
        &feed,
        &contend,
        &store,
        Some(&txs),
        Some(&tracker),
        Some(&ladder),
        limits(2, 50, 12),
        &mut RootMemo::default(),
        &Start::Unset,
        1,
    )
    .await;
    assert_eq!((s.applied, s.contended), (0, 2), "{s:?}");
    assert_eq!(s.state.as_ref().map(|st| st.cursor.as_str()), Some("40"), "the summary's state is the read one");
}

/// LENS L7 (the pin the lane lacks: "never a blind demotion" as BEHAVIOUR). A confirmed spender whose stored BEEF
/// carries NO bump (a courier-confirmed row the routine sweep has not healed) sits at the reorg's height. The feed's
/// judgement runs `demote_proofless: false`: with no courier proof the row is left alone and counted
/// `no_stored_proof`; only a courier proof naming ANOTHER block demotes it (evidence). Under the mutant
/// `demote_proofless: true` in `judge_window_legs` the first arm demotes blind and this case is RED; the lane's e620
/// pins catch that mutant by source text alone (`e620_g`).
#[tokio::test]
async fn lens_l7_a_proofless_confirmed_row_is_never_demoted_blind() {
    use crate::proof_fetcher::tests::real_spender_raw;
    use bsv_rs::transaction::{Beef, Transaction};
    use overlay_discovery::pot::storage::PotRecord;
    async fn proofless_confirmed(store: &MemoryPotStorage, pot: &str, height: u32) -> String {
        let raw = real_spender_raw(pot, 0);
        let spender = Transaction::from_hex(&raw).unwrap().id();
        store
            .store_record(&PotRecord {
                txid: pot.into(),
                output_index: 0,
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .mark_spent(pot, 0, &spender, true, None, Some(u64::from(height)), Some(true))
            .await
            .unwrap();
        let mut beef = Beef::new();
        beef.merge_raw_tx(hex::decode(&raw).unwrap(), None);
        store.store_beef(&spender, &beef.to_binary()).await.unwrap();
        spender
    }
    let txs = MemoryTxStore::default();
    let tracker = Roots::at(102);
    // (a) no courier proof: left alone
    let store = MemoryPotStorage::new();
    let _spender = proofless_confirmed(&store, &pot(30), 101).await;
    let feed = Journal::of(&[(41, REORG)]);
    let row = Row::default();
    let ladder = CourierStub::none();
    let s = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::At("40".into())).await;
    assert_eq!((s.applied, s.spenders.scanned), (1, 1), "{s:?}");
    assert_eq!(
        (s.spenders.no_stored_proof, s.spenders.demoted_blind, s.demoted()),
        (1, 0, 0),
        "no proof to refute and no courier word: nothing demoted: {s:?}"
    );
    assert_eq!(ladder.calls.get(), 1, "the courier was asked once");
    let r = store.get_spent_status(&pot(30), 0).await.unwrap().unwrap();
    assert!(r.spent_confirmed && r.spent_height == Some(101), "still confirmed");
    // (b) a courier proof naming another block: demoted on that evidence
    let store = MemoryPotStorage::new();
    let spender = proofless_confirmed(&store, &pot(31), 101).await;
    let ladder = CourierStub::with(
        [(spender.clone(), Ok(Some(single_tx_bump(&spender, 102).to_hex())))]
            .into_iter()
            .collect(),
    );
    let row = Row::default();
    let s = pass(&feed, &row, &store, &txs, &tracker, &ladder, limits(2, 50, 12), &Start::At("40".into())).await;
    assert_eq!((s.applied, s.demoted(), s.spenders.demoted_blind), (1, 1, 0), "{s:?}");
    let r = store.get_spent_status(&pot(31), 0).await.unwrap().unwrap();
    assert!(r.spent && !r.spent_confirmed, "demoted on the courier's disagreeing proof");
}
