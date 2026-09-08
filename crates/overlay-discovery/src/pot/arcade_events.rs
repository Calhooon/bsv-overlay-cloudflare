//! bsv-low M19B-G1 (2026-09-08): the PURE half of the overlay's consumer of
//! Arcade's reorg EVENTS.
//!
//! Arcade (`~/bsv/arcade`, deployed v0.13.3) emits its `ReorgEvent`s on an
//! SSE stream (`GET /chaintracks/v2/reorg/stream`) that carries no event ids,
//! no replay and nothing on connect: a Worker that holds no connection
//! between passes cannot consume it. What it CAN consume is the durable
//! projection of the same events: `GET /api/v1/blocks/processing-status`
//! lists the blocks Arcade tracked, newest first, and every block a
//! `ReorgEvent` orphaned (or its tie-scan found orphaned) carries `status:
//! "orphaned"` with an `orphanedAt` stamp. One such row IS one reorg event,
//! addressable by `(orphanedAt, height, hash)`: the event id the stream
//! lacks, so a persisted cursor gives idempotent replay.
//!
//! Everything here is a decision over facts already in hand: the parse of
//! Arcade's REAL page shape, the total order and the cursor, the
//! corroboration verdict (an event is a HINT: Arcade's own table holds the
//! CANONICAL 965773 block as `orphaned` since 2026-09-07 22:48:27Z, so a
//! demotion may only follow chaintracks' refutation of a stored proof, never
//! the row alone), and the persisted state document. The I/O lives in the
//! overlay crate (`arcade_reorg.rs`).

use serde::{Deserialize, Serialize};

use crate::pot::reorg::RowKey;

/// One block Arcade reports ORPHANED: the durable form of a
/// `ReorgEvent.orphanedHashes` element (or a tie-scan find).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrphanEvent {
    /// Arcade's stamp, `YYYY-MM-DDTHH:MM:SS.mmmZ` (UTC, millisecond):
    /// lexicographic order IS chronological order for this fixed shape, and
    /// [`parse_block_status_page`] admits no other shape.
    pub orphaned_at: String,
    pub height: u64,
    /// Lower-cased 64-hex display hash.
    pub hash: String,
}

impl OrphanEvent {
    /// The event's position in the consumer's total order.
    pub fn key(&self) -> EventKey {
        EventKey {
            orphaned_at: self.orphaned_at.clone(),
            height: self.height,
            hash: self.hash.clone(),
        }
    }
}

/// The consumer's cursor: events are totally ordered by `(orphanedAt,
/// height, hash)` (derived `Ord` compares the fields in this order). Two
/// blocks one reorg orphaned share a stamp and differ by height; a block
/// resurrected and orphaned again carries a NEWER stamp and is a new event.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventKey {
    pub orphaned_at: String,
    pub height: u64,
    pub hash: String,
}

/// What one page of `GET /api/v1/blocks/processing-status` said.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BlockStatusPage {
    /// The orphaned rows, in the page's order (newest height first).
    pub orphans: Vec<OrphanEvent>,
    /// Every row on the page (any status).
    pub rows: usize,
    /// Orphaned rows dropped for a missing or malformed field (counted,
    /// never guessed).
    pub malformed: usize,
    /// Arcade's keyset cursor (the lowest height on the page); absent on the
    /// last page.
    pub next_cursor: Option<u64>,
}

/// True for Arcade's stamp shape `YYYY-MM-DDTHH:MM:SS.mmmZ` (24 bytes, UTC).
pub fn is_arcade_stamp(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 24 {
        return false;
    }
    b.iter().enumerate().all(|(i, c)| match i {
        4 | 7 => *c == b'-',
        10 => *c == b'T',
        13 | 16 => *c == b':',
        19 => *c == b'.',
        23 => *c == b'Z',
        _ => c.is_ascii_digit(),
    })
}

fn is_block_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Parse one page of Arcade's block processing-status listing (the REAL
/// shape: `{"blocks": [{blockHash, blockHeight, status, orphanedAt?, …}],
/// "nextCursor"?: n}`). Only `status == "orphaned"` rows become events; an
/// orphaned row missing a well-formed `blockHash`, a positive `blockHeight`
/// or an `orphanedAt` stamp is dropped and counted (Arcade files height-0
/// placeholder rows, and an event without a height or a stamp cannot be
/// targeted or ordered). A body that is not the listing is `Err`.
pub fn parse_block_status_page(body: &str) -> Result<BlockStatusPage, String> {
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("block status page: not JSON: {e}"))?;
    let blocks = v
        .get("blocks")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "block status page: no `blocks` array".to_string())?;
    let mut page = BlockStatusPage {
        rows: blocks.len(),
        next_cursor: v.get("nextCursor").and_then(serde_json::Value::as_u64),
        ..Default::default()
    };
    for row in blocks {
        let status = row.get("status").and_then(serde_json::Value::as_str).unwrap_or("");
        if status != "orphaned" {
            continue;
        }
        let hash = row
            .get("blockHash")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|h| is_block_hash(h))
            .map(str::to_ascii_lowercase);
        let height = row
            .get("blockHeight")
            .and_then(serde_json::Value::as_u64)
            .filter(|h| *h > 0);
        let orphaned_at = row
            .get("orphanedAt")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| is_arcade_stamp(s))
            .map(str::to_string);
        match (hash, height, orphaned_at) {
            (Some(hash), Some(height), Some(orphaned_at)) => {
                page.orphans.push(OrphanEvent { orphaned_at, height, hash });
            }
            _ => page.malformed += 1,
        }
    }
    Ok(page)
}

/// The events STRICTLY after `cursor`, ascending in the total order, deduped
/// by key (Arcade's pager may hand a boundary row twice). `None` = every
/// event (a first run consumes the whole fetched window, bounded by the
/// caller's per-pass event budget).
pub fn events_after(events: &[OrphanEvent], cursor: Option<&EventKey>) -> Vec<OrphanEvent> {
    let mut out: Vec<OrphanEvent> = events
        .iter()
        .filter(|e| cursor.is_none_or(|c| e.key() > *c))
        .cloned()
        .collect();
    out.sort_by_key(OrphanEvent::key);
    out.dedup_by_key(|e| e.key());
    out
}

/// Whether an Arcade orphan event may be ACTED on, from what chaintracks
/// (our header source) holds. The event is a hint: only chaintracks'
/// disagreement with the orphaned hash makes it actionable, and even then
/// every row is judged by its own stored proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Corroboration {
    /// Chaintracks holds a DIFFERENT block at that height: the orphaned
    /// block is not on our chain. Re-verify the rows anchored there.
    Corroborated,
    /// Chaintracks still holds the orphaned hash as canonical, `skip_depth`
    /// or more blocks below its tip: it is not lagging, Arcade's row is
    /// wrong for our header source (the 2026-09-07 965773 row). Skip the
    /// event (counted); nothing is re-verified on its account.
    Uncorroborated,
    /// Chaintracks holds the orphaned hash (or no header yet at that
    /// height) near its tip: it may simply lag Arcade by a sync. Hold the
    /// event for the next pass; nothing changes.
    Held,
}

/// Classify an orphan event at `height` with `orphan_hash` against the
/// header chaintracks holds there (`None` = no header at that height yet)
/// and chaintracks' current `tip`. Case-insensitive on the hash. A read
/// FAULT is not an input here: the caller counts it and changes nothing.
pub fn classify_corroboration(
    canonical_at_height: Option<&str>,
    orphan_hash: &str,
    tip: u64,
    height: u64,
    skip_depth: u64,
) -> Corroboration {
    match canonical_at_height {
        Some(canonical) if !canonical.eq_ignore_ascii_case(orphan_hash) => Corroboration::Corroborated,
        Some(_) if tip >= height.saturating_add(skip_depth) => Corroboration::Uncorroborated,
        _ => Corroboration::Held,
    }
}

/// One leg's walk inside the event's height: where the last bounded page
/// stopped, and whether the leg reached the end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LegProgress {
    pub after: Option<RowKey>,
    pub exhausted: bool,
}

/// The event the consumer is applying, with each leg's persisted progress:
/// a bounded pass continues where the last one stopped, across passes and
/// isolates, until every leg is exhausted; only then does the cursor move.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingEvent {
    pub event: OrphanEvent,
    pub spenders: LegProgress,
    pub pot_beefs: LegProgress,
    pub transactions: LegProgress,
    /// Consecutive passes this event was HELD (chaintracks not yet past it).
    pub held_passes: u32,
}

impl PendingEvent {
    pub fn new(event: OrphanEvent) -> Self {
        Self {
            event,
            spenders: LegProgress::default(),
            pot_beefs: LegProgress::default(),
            transactions: LegProgress::default(),
            held_passes: 0,
        }
    }

    /// Every leg walked its height to the end: the event is applied.
    pub fn all_exhausted(&self) -> bool {
        self.spenders.exhausted && self.pot_beefs.exhausted && self.transactions.exhausted
    }
}

/// The current document version; a reader that finds a higher one treats
/// the document as unreadable (a fault, counted) rather than guessing.
pub const CONSUMER_STATE_VERSION: u32 = 1;

/// The consumer's persisted state: ONE row, a JSON document. `cursor` is
/// the last event FINISHED (applied, or skipped as uncorroborated); it moves
/// only through [`ConsumerState::advance_past_pending`], never on a fault
/// (the pending event replays, idempotently).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerState {
    #[serde(default = "default_version")]
    pub v: u32,
    #[serde(default)]
    pub cursor: Option<EventKey>,
    #[serde(default)]
    pub pending: Option<PendingEvent>,
}

fn default_version() -> u32 {
    CONSUMER_STATE_VERSION
}

impl Default for ConsumerState {
    fn default() -> Self {
        Self { v: CONSUMER_STATE_VERSION, cursor: None, pending: None }
    }
}

impl ConsumerState {
    /// Parse the persisted document. A document from a NEWER writer is an
    /// error (never silently reinterpreted); a missing field reads as its
    /// default (an older document still reads).
    pub fn from_json(doc: &str) -> Result<Self, String> {
        let s: Self = serde_json::from_str(doc).map_err(|e| format!("consumer state: {e}"))?;
        if s.v > CONSUMER_STATE_VERSION {
            return Err(format!(
                "consumer state: document version {} is newer than this reader's {}",
                s.v, CONSUMER_STATE_VERSION
            ));
        }
        Ok(s)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Take `event` up (no event may be pending).
    pub fn start(&mut self, event: OrphanEvent) {
        debug_assert!(self.pending.is_none(), "an event is already pending");
        self.pending = Some(PendingEvent::new(event));
    }

    /// The pending event is FINISHED (applied in full, or skipped as
    /// uncorroborated): the cursor moves to it and nothing is pending.
    /// Returns the key the cursor moved to (`None` = nothing was pending).
    pub fn advance_past_pending(&mut self) -> Option<EventKey> {
        let key = self.pending.take()?.event.key();
        self.cursor = Some(key.clone());
        Some(key)
    }

    /// The pending event was HELD this pass (chaintracks not yet past it).
    pub fn hold_pending(&mut self) {
        if let Some(p) = self.pending.as_mut() {
            p.held_passes = p.held_passes.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `GET /api/v1/blocks/processing-status?limit=8&before-height=965776`
    /// on `arcade-v2-us-1.bsvblockchain.tech` (v0.13.3), read 2026-09-08
    /// 12:3xZ: the 2026-09-07 double reorg as Arcade holds it, verbatim.
    /// Note the two rows at 965773: `…146bc084…6e33` is the CANONICAL block
    /// (chaintracks + WoC) and Arcade holds it `orphaned` (its tie-scan
    /// marked it when the near-empty sibling was first seen; nothing
    /// resurrected it).
    pub(crate) const REAL_PAGE_965769_965775: &str =
        include_str!("arcade_events_fixture_965769_965775.json");

    /// `GET /api/v1/blocks/processing-status/<the 965771 orphan>`, verbatim.
    pub(crate) const REAL_ORPHAN_ROW_965771: &str = r#"{"blockHash":"0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1","blockHeight":965771,"headerSeenAt":"2026-09-07T22:39:23.208Z","processedAt":"2026-09-07T22:39:39.393Z","bumpBuiltAt":"2026-09-07T22:39:39.392Z","status":"orphaned","orphanedAt":"2026-09-07T22:45:22.316Z","hasBlockProcessed":true,"hasCompoundBUMP":true}"#;

    pub(crate) const ORPHAN_965771: &str = "0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1";
    pub(crate) const CANONICAL_965771: &str = "00000000000000001de5aa96baa3566ce66e4941f8295cc44cc85fc75949db4d";
    pub(crate) const CANONICAL_965773: &str = "0000000000000000146bc084ec137a3c9608a07159128c66302051b6fe176e33";
    pub(crate) const EMPTY_SIBLING_965773: &str = "00000000000000000851a554167480b696b3dbd36ab1fd862f2520217988afa0";

    #[test]
    fn parses_arcades_real_block_status_page_into_ordered_orphan_events() {
        let page = parse_block_status_page(REAL_PAGE_965769_965775).unwrap();
        assert_eq!(page.rows, 8);
        assert_eq!(page.malformed, 0);
        assert_eq!(page.next_cursor, Some(965769), "Arcade's keyset cursor rides along");
        // the three orphaned rows, in the page's own order (height DESC)
        assert_eq!(
            page.orphans.iter().map(|o| (o.height, o.hash.as_str(), o.orphaned_at.as_str())).collect::<Vec<_>>(),
            vec![
                (965773, CANONICAL_965773, "2026-09-07T22:48:27.809Z"),
                (965773, EMPTY_SIBLING_965773, "2026-09-07T23:14:57.307Z"),
                (965771, ORPHAN_965771, "2026-09-07T22:45:22.316Z"),
            ]
        );
        // the consumer's order: by the stamp, so the 34 MB orphan is the FIRST event
        let ordered = events_after(&page.orphans, None);
        assert_eq!(
            ordered.iter().map(|o| (o.orphaned_at.as_str(), o.height)).collect::<Vec<_>>(),
            vec![
                ("2026-09-07T22:45:22.316Z", 965771),
                ("2026-09-07T22:48:27.809Z", 965773),
                ("2026-09-07T23:14:57.307Z", 965773),
            ]
        );
        // a single-row GET body is not a page
        assert!(parse_block_status_page(REAL_ORPHAN_ROW_965771).is_err());
        assert!(parse_block_status_page("nope").is_err());
        assert!(parse_block_status_page(r#"{"blocks": 3}"#).is_err());
        // an empty last page
        let last = parse_block_status_page(r#"{"blocks":[]}"#).unwrap();
        assert_eq!((last.rows, last.orphans.len(), last.next_cursor), (0, 0, None));
    }

    #[test]
    fn an_orphaned_row_missing_a_field_is_dropped_and_counted_never_guessed() {
        // Arcade files a height-0 placeholder for a hash it saw before any
        // header (the canonical 965771 row reads `blockHeight: 0` live);
        // an orphan without a height, a stamp or a well-formed hash cannot
        // be targeted or ordered.
        let body = format!(
            r#"{{"blocks":[
              {{"blockHash":"{ORPHAN_965771}","blockHeight":0,"status":"orphaned","orphanedAt":"2026-09-07T22:45:22.316Z"}},
              {{"blockHash":"{ORPHAN_965771}","blockHeight":965771,"status":"orphaned"}},
              {{"blockHash":"{ORPHAN_965771}","blockHeight":965771,"status":"orphaned","orphanedAt":"2026-09-07 22:45:22"}},
              {{"blockHash":"abc","blockHeight":965771,"status":"orphaned","orphanedAt":"2026-09-07T22:45:22.316Z"}},
              {{"blockHeight":965771,"status":"orphaned","orphanedAt":"2026-09-07T22:45:22.316Z"}},
              {{"blockHash":"{CANONICAL_965771}","blockHeight":965771,"status":"active"}},
              {{"blockHash":"{CANONICAL_965771}","blockHeight":965771,"status":"parked"}},
              {{"blockHash":"{}","blockHeight":965771,"status":"orphaned","orphanedAt":"2026-09-07T22:45:22.316Z"}}
            ]}}"#,
            ORPHAN_965771.to_ascii_uppercase()
        );
        let page = parse_block_status_page(&body).unwrap();
        assert_eq!(page.rows, 8);
        assert_eq!(page.malformed, 5, "the five broken orphan rows are counted");
        assert_eq!(page.orphans.len(), 1, "active and parked rows are not events");
        assert_eq!(page.orphans[0].hash, ORPHAN_965771, "the hash is lower-cased");
        assert!(is_arcade_stamp("2026-09-07T22:45:22.316Z"));
        assert!(!is_arcade_stamp("2026-09-07T22:45:22Z"), "no millisecond field");
        assert!(!is_arcade_stamp("2026-09-07T22:45:22.316+02:00"), "a zone offset is not Arcade's UTC shape");
        assert!(!is_arcade_stamp("2026-09-07T22:45:22.31Z"));
    }

    /// The SSE stream's own frame (`ReorgEvent`, go-chaintracks
    /// `chaintracks/types.go:44`, marshalled as Arcade's
    /// `broadcastReorg` writes it) is NOT the listing: fed to the page
    /// parser it is refused, never read as zero events. An unknown
    /// `status` value on a listing row is not an event either (counted as
    /// a row, nothing else).
    #[test]
    fn the_sse_reorg_frame_and_an_unknown_status_are_not_events() {
        // the frame's data payload, as `json.Marshal(ev)` renders a ReorgEvent
        // (block hashes in display hex; the two BlockHeader fields we read)
        let sse_frame = format!(
            r#"{{"orphanedHashes":["{ORPHAN_965771}"],"commonAncestor":{{"height":965770,"hash":"00000000000000000e9fda93c1e2d4a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1"}},"newTip":{{"height":965771,"hash":"{CANONICAL_965771}"}},"depth":1}}"#
        );
        assert!(parse_block_status_page(&sse_frame).is_err(), "the stream frame is not the listing");
        let unknown = format!(
            r#"{{"blocks":[
              {{"blockHash":"{ORPHAN_965771}","blockHeight":965771,"status":"resurrected","orphanedAt":"2026-09-07T22:45:22.316Z"}},
              {{"blockHash":"{ORPHAN_965771}","blockHeight":965771,"status":"ORPHANED","orphanedAt":"2026-09-07T22:45:22.316Z"}},
              {{"blockHash":"{ORPHAN_965771}","blockHeight":965771,"orphanedAt":"2026-09-07T22:45:22.316Z"}},
              {{"blockHash":"{ORPHAN_965771}","blockHeight":965771,"status":7}}
            ]}}"#
        );
        let page = parse_block_status_page(&unknown).unwrap();
        assert_eq!((page.rows, page.orphans.len(), page.malformed), (4, 0, 0), "an unknown, differently-cased, absent or non-string status is no event and no malformation: {page:?}");
    }

    #[test]
    fn events_are_ordered_by_stamp_then_height_then_hash_and_the_cursor_is_strict() {
        let ev = |stamp: &str, height: u64, hash: &str| OrphanEvent {
            orphaned_at: stamp.into(),
            height,
            hash: hash.into(),
        };
        // one reorg orphaning two blocks shares the stamp: the height breaks the tie
        let a = ev("2026-09-07T22:45:22.316Z", 965772, &"bb".repeat(32));
        let b = ev("2026-09-07T22:45:22.316Z", 965771, &"aa".repeat(32));
        let c = ev("2026-09-07T23:14:57.307Z", 965773, &"cc".repeat(32));
        let d = ev("2026-09-07T22:48:27.809Z", 965773, &"dd".repeat(32));
        let all = events_after(&[c.clone(), a.clone(), b.clone(), d.clone(), a.clone()], None);
        assert_eq!(all, vec![b.clone(), a.clone(), d.clone(), c.clone()], "ascending, deduped");
        // the cursor excludes itself and everything before
        assert_eq!(events_after(&[c.clone(), a.clone(), b.clone(), d.clone()], Some(&a.key())), vec![d.clone(), c.clone()]);
        assert_eq!(events_after(&[c.clone(), a.clone(), b.clone(), d.clone()], Some(&c.key())), vec![]);
        // a resurrected block orphaned AGAIN is a newer stamp: a new event after the old cursor
        let again = ev("2026-09-08T01:00:00.000Z", 965771, &"aa".repeat(32));
        assert_eq!(events_after(&[b.clone(), again.clone()], Some(&b.key())), vec![again]);
        // the key's derived order is the stamp first
        assert!(b.key() < a.key() && a.key() < d.key() && d.key() < c.key());
    }

    #[test]
    fn corroboration_needs_chaintracks_to_disagree_with_the_orphan() {
        // the real 965771: chaintracks holds the canonical block there
        assert_eq!(
            classify_corroboration(Some(CANONICAL_965771), ORPHAN_965771, 965_860, 965_771, 3),
            Corroboration::Corroborated
        );
        // the real 965773: Arcade says orphaned, chaintracks holds THAT hash deep below its tip
        assert_eq!(
            classify_corroboration(Some(CANONICAL_965773), CANONICAL_965773, 965_860, 965_773, 3),
            Corroboration::Uncorroborated,
            "Arcade's row is wrong for our header source: skipped, counted"
        );
        assert_eq!(
            classify_corroboration(Some(&CANONICAL_965773.to_ascii_uppercase()), CANONICAL_965773, 965_860, 965_773, 3),
            Corroboration::Uncorroborated,
            "case-insensitive"
        );
        // the same hash near the tip: chaintracks may lag Arcade by a sync
        for tip in [965_773, 965_774, 965_775] {
            assert_eq!(classify_corroboration(Some(CANONICAL_965773), CANONICAL_965773, tip, 965_773, 3), Corroboration::Held, "tip {tip}");
        }
        assert_eq!(classify_corroboration(Some(CANONICAL_965773), CANONICAL_965773, 965_776, 965_773, 3), Corroboration::Uncorroborated);
        // no header at that height yet: held, however far the tip reads
        assert_eq!(classify_corroboration(None, ORPHAN_965771, 965_860, 965_771, 3), Corroboration::Held);
        // a disagreement is corroborated even at the tip
        assert_eq!(classify_corroboration(Some(CANONICAL_965771), ORPHAN_965771, 965_771, 965_771, 3), Corroboration::Corroborated);
    }

    #[test]
    fn the_state_document_round_trips_and_the_cursor_moves_only_past_a_finished_event() {
        let mut s = ConsumerState::default();
        assert_eq!(s.v, CONSUMER_STATE_VERSION);
        assert_eq!(ConsumerState::from_json(&s.to_json()).unwrap(), s);
        let page = parse_block_status_page(REAL_PAGE_965769_965775).unwrap();
        let events = events_after(&page.orphans, None);
        s.start(events[0].clone());
        // a leg's progress persists; the cursor does not move
        s.pending.as_mut().unwrap().spenders = LegProgress { after: Some(RowKey { height: 965771, rowid: 40 }), exhausted: false };
        s.hold_pending();
        let back = ConsumerState::from_json(&s.to_json()).unwrap();
        assert_eq!(back, s);
        assert_eq!(back.cursor, None);
        assert_eq!(back.pending.as_ref().unwrap().spenders.after, Some(RowKey { height: 965771, rowid: 40 }));
        assert_eq!(back.pending.as_ref().unwrap().held_passes, 1);
        assert!(!back.pending.as_ref().unwrap().all_exhausted());
        // finished: the cursor is the event's key, nothing pending
        let moved = s.advance_past_pending();
        assert_eq!(moved, Some(events[0].key()));
        assert_eq!(s.cursor, Some(events[0].key()));
        assert_eq!(s.pending, None);
        assert_eq!(s.advance_past_pending(), None, "nothing pending: the cursor stays");
        assert_eq!(s.cursor, Some(events[0].key()));
        // the next event is the one after the cursor
        assert_eq!(events_after(&page.orphans, s.cursor.as_ref())[0].hash, CANONICAL_965773);
        // an older document (no version field) reads; a newer one is refused
        let old = ConsumerState::from_json(r#"{"cursor":null,"pending":null}"#).unwrap();
        assert_eq!(old, ConsumerState::default());
        assert!(ConsumerState::from_json(r#"{"v":2}"#).is_err(), "a newer writer's document is not guessed at");
        assert!(ConsumerState::from_json("garbage").is_err());
    }
}
