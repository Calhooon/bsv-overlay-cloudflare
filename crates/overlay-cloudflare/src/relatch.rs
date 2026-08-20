//! The lazy RE-LATCH pass for the two admission-latched verdict columns —
//! `potparty_records.sigValid` (bsv-low#355) and `hopparty_records.markerValid`
//! (bsv-low#367) — as ONE pass over two tables.
//!
//! # Why a pass exists at all
//!
//! Both columns are written ONCE, by `INSERT OR IGNORE`, at admission, and
//! before this module nothing in production re-evaluated either of them. Two
//! populations fall out of that, and only one of them is the obvious one:
//!
//!  - **`NULL` — rows admitted before the migration.** For potparty this is
//!    permanent in practice (`decidePartyStep` stops the moment an indexed row
//!    exists for the pot, and a legacy row IS an indexed row, so the #252 sweep
//!    never republishes for exactly the pots that need it). For hopparty it is
//!    permanent by CONSTRUCTION: a hop marker must ride the hop transaction,
//!    and that transaction is already on chain — no client behaviour could ever
//!    re-latch it.
//!  - **`0` — rows a TRANSIENT predicate fault refuted.** A `bsv-rs`
//!    DER/`to_der` behaviour change, a wallet emitting a non-canonical
//!    signature mid-rollout, a partial deploy: every honest row admitted in
//!    that window is demoted to rank 0, which sorts BELOW the legacy `NULL`
//!    tier, forever. That is the epoch Rule 6 trade (a self-healing failure
//!    swapped for a permanent one) and Rule 14 names who pays — wiped-device
//!    users seeing a silently short recovery enumeration, the population least
//!    able to report it.
//!
//! # The criterion is a FIXPOINT, not a NULL census
//!
//! > Every row's verdict equals the predicate recomputed at the pass's own
//! > predicate version.
//!
//! `WHERE sigValid IS NULL` (or `markerValid IS NULL`) as the pass's FILTER is
//! the one shape that cannot work: a faulted row is a `0`, not a `NULL`, so a
//! NULL-census structurally skips exactly the population the pass exists to
//! repair. The scan here is over ALL rows, `rowid > ?cursor ORDER BY rowid ASC
//! LIMIT ?`, and a row is written whenever the recomputed verdict DIFFERS from
//! the stored one — `NULL`→`0/1`, `0`→`1`, and `1`→`0`.
//!
//! The `NULL` count is still REPORTED ([`RelatchSummary::still_null`]), because
//! it is the honest measure of how much of the legacy tier is left. It is a
//! readout, never a filter.
//!
//! # Observability IS the detector (epoch Rule 13)
//!
//! [`RelatchSummary::changed`] is split three ways on purpose:
//!
//!  - `latched` (`NULL` → a verdict) — the expected deploy-day traffic, which
//!    drains monotonically;
//!  - `promoted` (`0` → `1`) — a repair: rows a fault had refuted;
//!  - `demoted` (`1` → `0`) — **the alarm**. In steady state this is zero. A
//!    non-zero run means the predicate now refuses rows it previously accepted,
//!    which is the cross-language-disagreement failure (epoch Rule 16) arriving
//!    as a number rather than as silence. Each one is logged individually with
//!    its outpoint, because a handful of them is a bug report and a page of
//!    them is an incident.
//!
//! A count nobody can act on is telemetry, so note what the action is: a
//! sustained `demoted` stream means STOP and compare the predicate against the
//! client's signer (the frozen golden cells in `potparty::validity` and
//! `hopparty::validity` are the first thing to run) — never "re-run the pass".
//!
//! # Bounds
//!
//! [`RELATCH_PAGE_LIMIT`] rows per table per tick, each costing at most two
//! ECDSA verifies plus a BRC-42 derivation. No network, no chain read, no BEEF
//! re-parse: every input the predicates need is already in the row (#310's
//! decode-at-write put the hopparty container facts there). Writes are
//! single-row `UPDATE`s issued one at a time — there is no multi-row
//! transaction and therefore no write lock that outlives one statement.
//!
//! The pass is idempotent (a second run over a converged page changes nothing
//! and writes nothing), resumable (the cursor is durable in `relatch_cursors`),
//! and safe to run repeatedly on cold start. When a page comes back SHORT the
//! table's tail has been reached and the cursor WRAPS to 0, so the pass is a
//! continuous fixpoint sweep rather than a one-shot backfill — which is what
//! makes it re-runnable after any change to a predicate or to `bsv-rs`.
//!
//! # What this is NOT
//!
//! It cannot hide a row and it cannot refuse one. Both columns are LEADING
//! SORT KEYS and never a `WHERE` (epoch Rule 23) — a 0-latched or legacy row is
//! served and labelled exactly as before. The worst a wrong verdict here can do
//! is mis-order candidates, which every consumer that draws a conclusion
//! re-verifies against the signatures carried back verbatim on the wire.

use async_trait::async_trait;

use crate::proof_fetcher::push_log;

/// Rows scanned per TABLE per tick.
///
/// Each row costs at most two ECDSA verifies plus one BRC-42 derivation
/// (~4 EC operations), so a tick is ~256 EC ops per table — hundreds of
/// milliseconds of CPU inside a cron invocation that is otherwise dominated by
/// network I/O. At the 15-minute cron cadence that is ~6k rows/table/day, which
/// converges a table of any size this system can plausibly hold well inside a
/// day while never being the reason a tick runs long.
pub const RELATCH_PAGE_LIMIT: u64 = 64;

/// A durable per-table scan position.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RelatchCursor {
    /// Rows at or below this `rowid` were visited in the current sweep.
    pub cursor: i64,
    /// Completed sweeps of the whole table (monotonic; the fixpoint counter).
    pub sweeps: u64,
}

/// Table-wide counts read after a page is applied.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RelatchCensus {
    /// Rows still ahead of the cursor in THIS sweep.
    pub remaining: u64,
    /// Rows whose verdict column is still `NULL` — the legacy tier's size.
    pub still_null: u64,
}

/// The tally of one table's tick. Every field is logged; see the module doc for
/// which one is the alarm.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RelatchSummary {
    /// The table this summarises (also the cursor key).
    pub table: &'static str,
    /// Rows read this tick.
    pub scanned: usize,
    /// Rows whose stored verdict was `NULL` and now carries one.
    pub latched: usize,
    /// Rows REPAIRED: stored `0`, recomputed `1`.
    pub promoted: usize,
    /// Rows DEMOTED: stored `1`, recomputed `0`. **The alarm** — see the
    /// module doc.
    pub demoted: usize,
    /// Rows still ahead of the cursor in this sweep.
    pub remaining: u64,
    /// Rows whose verdict is still `NULL`, table-wide.
    pub still_null: u64,
    /// The cursor persisted for the next tick (0 after a wrap).
    pub cursor: i64,
    /// Completed sweeps after this tick.
    pub sweeps: u64,
    /// This tick reached the table's tail and wrapped the cursor.
    pub wrapped: bool,
    /// Storage faults (scan, write, census or cursor). Never fatal — the row
    /// stays a candidate for the next sweep.
    pub errors: usize,
}

impl RelatchSummary {
    /// Rows whose stored verdict DIFFERED from the recomputed one, i.e. rows
    /// written. Derived rather than counted separately so it cannot disagree
    /// with its own parts (epoch Rule 10).
    pub fn changed(&self) -> usize {
        self.latched + self.promoted + self.demoted
    }
}

/// One re-latchable table.
///
/// `Row` carries the stored verdict AND the record, because the decision "does
/// this need writing?" belongs to the pass while the VERDICT belongs to the
/// write (epoch Rule 15: never hand a call site a decision it can get wrong —
/// [`RelatchTable::relatch`] derives the verdict from the record itself and is
/// given no way to be told one).
#[async_trait(?Send)]
pub trait RelatchTable {
    /// The row shape the scan yields.
    type Row;

    /// The verdict the table's column stores. `bool` for the original two
    /// arms (`sigValid`, `markerValid`); `i64` for TIERED latches
    /// (`claimValid` 0/1/2 — brain-cutover M1). `Ord` is load-bearing: the
    /// pass classifies a change as `promoted` (toward more-verified) or
    /// `demoted` (the alarm) by comparison, which for `bool` is exactly the
    /// old false<true behaviour.
    type Verdict: Copy + Ord + std::fmt::Debug;

    /// The table's name — the cursor key and the log label.
    fn table(&self) -> &'static str;

    /// This row's `rowid` (the pass advances the cursor with it).
    fn rowid(row: &Self::Row) -> i64;

    /// This row's stored verdict: `None` = the legacy `NULL` tier.
    fn stored(row: &Self::Row) -> Option<Self::Verdict>;

    /// Rows STRICTLY after `after_rowid`, `rowid` ascending, at most `limit`.
    /// Never filtered on the verdict column — see the module doc.
    async fn scan(&self, after_rowid: i64, limit: u64) -> Result<Vec<Self::Row>, String>;

    /// RECOMPUTE this row's verdict and write it IFF it differs from the
    /// stored one. `Ok(None)` = converged, nothing written.
    ///
    /// The compare lives here, next to the single evaluation, on purpose. The
    /// obvious split — the pass recomputes to decide, the writer recomputes to
    /// bind — evaluates the predicate TWICE per changed row while a comment
    /// claims once, which is the exact defect the #283 gate found in the
    /// admission path (round 2, LOW-2). One evaluation, carried from the
    /// decision into the bind.
    ///
    /// The verdict is still DERIVED by the write and never handed to it
    /// (epoch Rule 15): implementations build their capability-typed UPDATE
    /// first and read the verdict off it.
    async fn relatch_if_changed(&self, row: &Self::Row) -> Result<Option<Self::Verdict>, String>;

    /// `(rows after `after_rowid`, rows whose verdict is NULL table-wide)`.
    async fn census(&self, after_rowid: i64) -> Result<RelatchCensus, String>;
}

/// Durable storage for [`RelatchCursor`]s, keyed by table name.
#[async_trait(?Send)]
pub trait RelatchCursorStore {
    async fn load(&self, table: &str) -> Result<RelatchCursor, String>;
    async fn store(&self, table: &str, cursor: RelatchCursor) -> Result<(), String>;
}

/// Run ONE bounded tick of the re-latch fixpoint over `table`.
///
/// The cursor is loaded, advanced and persisted HERE and nowhere else, so no
/// call site can page wrongly, forget to wrap, or skip the write (epoch Rule
/// 15). A caller supplies the table and the cursor store; it does not get to
/// supply a position or a verdict.
pub async fn relatch_pass<T: RelatchTable, C: RelatchCursorStore>(
    table: &T,
    cursors: &C,
    limit: u64,
) -> RelatchSummary {
    let name = table.table();
    let mut summary = RelatchSummary {
        table: name,
        ..Default::default()
    };

    // A cursor read fault restarts the sweep from the head rather than
    // aborting: re-verifying a page is idempotent and free of side effects,
    // whereas skipping the tick would let a broken cursor store silently stop
    // the only repair path (epoch Rule 4b — degrade toward working).
    let start = match cursors.load(name).await {
        Ok(c) => c,
        Err(e) => {
            push_log(&format!(
                "[relatch:{name}] cursor read failed, restarting sweep: {e}"
            ));
            summary.errors += 1;
            RelatchCursor::default()
        }
    };

    let page = match table.scan(start.cursor, limit).await {
        Ok(p) => p,
        Err(e) => {
            push_log(&format!("[relatch:{name}] scan failed: {e}"));
            summary.errors += 1;
            summary.cursor = start.cursor;
            summary.sweeps = start.sweeps;
            return summary;
        }
    };
    summary.scanned = page.len();

    let mut last_rowid = start.cursor;
    for row in &page {
        last_rowid = T::rowid(row);
        let stored = T::stored(row);
        match table.relatch_if_changed(row).await {
            // Converged: the stored verdict already equals the predicate at
            // this pass's version. No write, which is what makes a re-run over
            // a settled table free.
            Ok(None) => {}
            Ok(Some(written)) => {
                debug_assert_ne!(
                    stored,
                    Some(written),
                    "a write is reported only when the verdict CHANGED"
                );
                match stored {
                    None => summary.latched += 1,
                    Some(prev) if written > prev => summary.promoted += 1,
                    Some(_) => {
                        // THE alarm (epoch Rule 13): logged per row, because a
                        // handful is a bug report and a page of them is a
                        // cross-language predicate regression in progress.
                        summary.demoted += 1;
                        push_log(&format!(
                            "[relatch:{name}] DEMOTED rowid={last_rowid} — the predicate now \
                             refuses a row it previously accepted; compare it against the \
                             client's signer"
                        ));
                    }
                }
            }
            Err(e) => {
                // The row stays a candidate for the next sweep; the cursor
                // still advances, so one poisoned row cannot wedge the pass.
                push_log(&format!(
                    "[relatch:{name}] write failed rowid={last_rowid}: {e}"
                ));
                summary.errors += 1;
            }
        }
    }

    // A SHORT page means the tail was reached: wrap, so the pass is a
    // continuous fixpoint rather than a one-shot backfill and stays re-runnable
    // after any predicate change.
    summary.wrapped = (page.len() as u64) < limit;
    summary.cursor = if summary.wrapped { 0 } else { last_rowid };
    summary.sweeps = start.sweeps + u64::from(summary.wrapped);

    match table.census(last_rowid).await {
        Ok(c) => {
            summary.remaining = c.remaining;
            summary.still_null = c.still_null;
        }
        Err(e) => {
            push_log(&format!("[relatch:{name}] census failed: {e}"));
            summary.errors += 1;
        }
    }

    if let Err(e) = cursors
        .store(
            name,
            RelatchCursor {
                cursor: summary.cursor,
                sweeps: summary.sweeps,
            },
        )
        .await
    {
        push_log(&format!("[relatch:{name}] cursor write failed: {e}"));
        summary.errors += 1;
    }

    summary
}

/// The D1-backed cursor store (`relatch_cursors`, migration 103).
pub struct D1RelatchCursors(std::rc::Rc<worker::D1Database>);

impl D1RelatchCursors {
    pub fn new(db: std::rc::Rc<worker::D1Database>) -> Self {
        Self(db)
    }
}

/// `SELECT cursorRowid, sweeps FROM relatch_cursors WHERE tableName = ?`.
pub const RELATCH_CURSOR_LOAD_SQL: &str =
    "SELECT cursorRowid, sweeps FROM relatch_cursors WHERE tableName = ?";

/// The upsert. `ON CONFLICT` rather than `INSERT OR REPLACE` so a future
/// column on this table cannot be silently zeroed by a re-write.
pub const RELATCH_CURSOR_STORE_SQL: &str =
    "INSERT INTO relatch_cursors (tableName, cursorRowid, sweeps) VALUES (?, ?, ?) \
     ON CONFLICT(tableName) DO UPDATE SET \
         cursorRowid = excluded.cursorRowid, sweeps = excluded.sweeps";

#[derive(serde::Deserialize)]
struct CursorRow {
    #[serde(rename = "cursorRowid")]
    cursor_rowid: f64,
    sweeps: f64,
}

#[async_trait(?Send)]
impl RelatchCursorStore for D1RelatchCursors {
    async fn load(&self, table: &str) -> Result<RelatchCursor, String> {
        let row: Option<CursorRow> = crate::d1::Query::new(RELATCH_CURSOR_LOAD_SQL)
            .bind(table)
            .fetch_optional(&self.0)
            .await?;
        // No row = never swept = start at the head. Absence is a POSITION, not
        // a fault: the pass must run on a database that has never seen it.
        Ok(row
            .map(|r| RelatchCursor {
                cursor: r.cursor_rowid as i64,
                sweeps: r.sweeps as u64,
            })
            .unwrap_or_default())
    }

    async fn store(&self, table: &str, cursor: RelatchCursor) -> Result<(), String> {
        crate::d1::Query::new(RELATCH_CURSOR_STORE_SQL)
            .bind(table)
            .bind(cursor.cursor)
            .bind(cursor.sweeps as i64)
            .execute(&self.0)
            .await
    }
}

/// ONE tick of the re-latch fixpoint over BOTH latched tables (#355 + #367).
///
/// Deliberately one entry point rather than two call sites: the two tables
/// share a cursor pattern, a bound and a summary shape, and a caller that could
/// run one and forget the other is a caller that will (the hopparty tier is the
/// one with no other repair path at all).
pub async fn run_relatch(
    db: std::rc::Rc<worker::D1Database>,
    limit: u64,
) -> (RelatchSummary, RelatchSummary) {
    let cursors = D1RelatchCursors::new(db.clone());
    let potparty = crate::d1_discovery::D1PotpartyStorage::new(db.clone());
    let hopparty = crate::d1_discovery::D1HoppartyStorage::new(db);
    let pp = relatch_pass(&potparty, &cursors, limit).await;
    let hp = relatch_pass(&hopparty, &cursors, limit).await;
    (pp, hp)
}

/// Log one table's tick in the shape the other scheduled passes use.
pub fn log_relatch_summary(s: &RelatchSummary) {
    push_log(&format!(
        "Scheduled: relatch ({}) — scanned={} changed={} (latched={} promoted={} demoted={}) \
         remaining={} still_null={} cursor={} sweeps={} wrapped={} errors={}",
        s.table,
        s.scanned,
        s.changed(),
        s.latched,
        s.promoted,
        s.demoted,
        s.remaining,
        s.still_null,
        s.cursor,
        s.sweeps,
        s.wrapped,
        s.errors,
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// A table whose rows are `(rowid, stored, truth)` — `truth` is what the
    /// predicate says NOW. Enough to drive every branch of the pass.
    struct FakeTable {
        rows: RefCell<Vec<(i64, Option<bool>, bool)>>,
        writes: RefCell<Vec<(i64, bool)>>,
        fail_write_at: Option<i64>,
    }

    impl FakeTable {
        fn new(rows: Vec<(i64, Option<bool>, bool)>) -> Self {
            Self {
                rows: RefCell::new(rows),
                writes: RefCell::new(Vec::new()),
                fail_write_at: None,
            }
        }
    }

    #[async_trait(?Send)]
    impl RelatchTable for FakeTable {
        type Row = (i64, Option<bool>, bool);
        type Verdict = bool;
        fn table(&self) -> &'static str {
            "fake_records"
        }
        fn rowid(row: &Self::Row) -> i64 {
            row.0
        }
        fn stored(row: &Self::Row) -> Option<bool> {
            row.1
        }
        async fn scan(&self, after: i64, limit: u64) -> Result<Vec<Self::Row>, String> {
            Ok(self
                .rows
                .borrow()
                .iter()
                .filter(|r| r.0 > after)
                .take(limit as usize)
                .copied()
                .collect())
        }
        async fn relatch_if_changed(&self, row: &Self::Row) -> Result<Option<bool>, String> {
            let v = row.2;
            if row.1 == Some(v) {
                return Ok(None);
            }
            if self.fail_write_at == Some(row.0) {
                return Err("boom".into());
            }
            self.writes.borrow_mut().push((row.0, v));
            for r in self.rows.borrow_mut().iter_mut() {
                if r.0 == row.0 {
                    r.1 = Some(v);
                }
            }
            Ok(Some(v))
        }
        async fn census(&self, after: i64) -> Result<RelatchCensus, String> {
            let rows = self.rows.borrow();
            Ok(RelatchCensus {
                remaining: rows.iter().filter(|r| r.0 > after).count() as u64,
                still_null: rows.iter().filter(|r| r.1.is_none()).count() as u64,
            })
        }
    }

    #[derive(Default)]
    struct MemCursors(RefCell<std::collections::HashMap<String, RelatchCursor>>);

    #[async_trait(?Send)]
    impl RelatchCursorStore for MemCursors {
        async fn load(&self, table: &str) -> Result<RelatchCursor, String> {
            Ok(self.0.borrow().get(table).copied().unwrap_or_default())
        }
        async fn store(&self, table: &str, cursor: RelatchCursor) -> Result<(), String> {
            self.0.borrow_mut().insert(table.to_string(), cursor);
            Ok(())
        }
    }

    /// THE criterion, executed: a NULL-only census would skip the `0` rows, so
    /// the pass must repair a `0` that the predicate now accepts AND a `1` it
    /// now refutes, in the same tick as the `NULL`s it latches.
    #[tokio::test]
    async fn the_pass_repairs_zeros_and_ones_not_just_nulls() {
        let t = FakeTable::new(vec![
            (1, None, true),         // legacy → latched
            (2, Some(false), true),  // transient fault → PROMOTED
            (3, Some(true), false),  // predicate regression → DEMOTED
            (4, Some(true), true),   // converged → untouched
            (5, Some(false), false), // converged (refuted) → untouched
        ]);
        let c = MemCursors::default();
        let s = relatch_pass(&t, &c, 10).await;

        assert_eq!(s.scanned, 5);
        assert_eq!((s.latched, s.promoted, s.demoted), (1, 1, 1));
        assert_eq!(s.changed(), 3);
        assert_eq!(
            t.writes.borrow().as_slice(),
            &[(1, true), (2, true), (3, false)],
            "ONLY the differing rows are written — converged rows cost no write"
        );
        assert_eq!(s.still_null, 0, "the legacy row now carries a verdict");
        assert_eq!(s.errors, 0);
    }

    /// Idempotence: a second tick over a converged table writes NOTHING. This
    /// is what makes the pass safe on every cold start.
    #[tokio::test]
    async fn a_second_tick_over_a_converged_table_writes_nothing() {
        let t = FakeTable::new(vec![(1, None, true), (2, Some(true), false)]);
        let c = MemCursors::default();
        let first = relatch_pass(&t, &c, 10).await;
        assert_eq!(first.changed(), 2);

        let writes_after_first = t.writes.borrow().len();
        let second = relatch_pass(&t, &c, 10).await;
        assert_eq!(second.changed(), 0, "converged");
        assert_eq!(second.scanned, 2, "…but every row was still VISITED");
        assert_eq!(t.writes.borrow().len(), writes_after_first, "no new writes");
    }

    /// Resumability: a page smaller than the table advances the cursor, the
    /// next tick continues from it, and reaching the tail WRAPS so the sweep
    /// starts over — the fixpoint property, not a one-shot backfill.
    #[tokio::test]
    async fn the_cursor_pages_then_wraps_into_a_new_sweep() {
        let rows: Vec<(i64, Option<bool>, bool)> = (1..=5).map(|i| (i, Some(true), true)).collect();
        let t = FakeTable::new(rows);
        let c = MemCursors::default();

        let a = relatch_pass(&t, &c, 2).await;
        assert_eq!((a.scanned, a.cursor, a.wrapped, a.sweeps), (2, 2, false, 0));
        assert_eq!(a.remaining, 3);

        let b = relatch_pass(&t, &c, 2).await;
        assert_eq!((b.scanned, b.cursor, b.wrapped, b.sweeps), (2, 4, false, 0));
        assert_eq!(b.remaining, 1);

        // The tail: a SHORT page wraps the cursor and closes the sweep.
        let d = relatch_pass(&t, &c, 2).await;
        assert_eq!((d.scanned, d.cursor, d.wrapped, d.sweeps), (1, 0, true, 1));
        assert_eq!(d.remaining, 0);

        // …and the next tick starts the NEXT sweep at the head, which is what
        // makes the pass re-runnable after a predicate change.
        let e = relatch_pass(&t, &c, 2).await;
        assert_eq!(e.scanned, 2);
        assert_eq!(e.cursor, 2);
    }

    /// An exactly-full final page does NOT wrap; the following empty page
    /// does. The off-by-one that would otherwise re-scan the tail forever.
    #[tokio::test]
    async fn an_exactly_full_final_page_wraps_on_the_next_tick() {
        let t = FakeTable::new(vec![(1, Some(true), true), (2, Some(true), true)]);
        let c = MemCursors::default();
        let a = relatch_pass(&t, &c, 2).await;
        assert_eq!((a.scanned, a.wrapped, a.cursor), (2, false, 2));
        let b = relatch_pass(&t, &c, 2).await;
        assert_eq!(
            (b.scanned, b.wrapped, b.cursor, b.sweeps),
            (0, true, 0, 1),
            "an empty page is the tail"
        );
    }

    /// A poisoned row cannot wedge the sweep: the write error is COUNTED, the
    /// cursor still advances past it, and the rest of the page is applied.
    #[tokio::test]
    async fn a_write_fault_is_counted_and_never_wedges_the_cursor() {
        let mut t = FakeTable::new(vec![(1, None, true), (2, None, true), (3, None, true)]);
        t.fail_write_at = Some(2);
        let c = MemCursors::default();
        let s = relatch_pass(&t, &c, 10).await;
        assert_eq!(s.errors, 1);
        assert_eq!(s.latched, 2, "the other two rows were repaired");
        assert_eq!(s.still_null, 1, "and the failed row is honestly still NULL");
        assert_eq!(t.writes.borrow().len(), 2);
    }

    /// `changed()` is DERIVED from its parts, so no edit can make the total
    /// and the breakdown disagree (epoch Rule 10).
    #[test]
    fn changed_is_the_sum_of_its_parts() {
        let s = RelatchSummary {
            latched: 3,
            promoted: 5,
            demoted: 7,
            ..Default::default()
        };
        assert_eq!(s.changed(), 15);
    }
}
