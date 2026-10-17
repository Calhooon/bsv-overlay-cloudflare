# DESIGN #674: the leaderboard derived at the write

Status: design only (lane W1-LB-642, 2026-10-17). The read lane #642 shipped the bounded candidate read
(`logic::chain_wins_candidates_sql`, the last 2,000 counted wins); this page is the table that removes the bound.
CAP moves it into bsv-low's `docs/`.

## Why a table

`/leaderboard` ranks owners by a COUNT over every counted win. An aggregate cannot be paged by an index unless
the count is stored, so #642 ranks a window and says so (`candidatesBounded`). #674 stores the count at the write,
per the #638 doctrine (derived lookup tables are the write's, never the read's), and the read becomes a keyset
walk of an index: the top N read N rows, whatever the era holds.

## The derived tables (overlay migrations, appended)

- `board_wins` (one row per counted win): `potTxid TEXT, potVout INTEGER, owner TEXT, identityIsKey INTEGER,
  settleTxid TEXT, verdict TEXT, pubA TEXT, pubB TEXT, settleSigners TEXT, identityA TEXT, identityB TEXT,
  potCreatedAt INTEGER, PRIMARY KEY (potTxid, potVout)`; index `(owner, potCreatedAt DESC, potTxid)` (the page
  owners' pots, `chain_wins_owners_sql`'s order) and `(potCreatedAt, potTxid)` (the era cutoff).
- `board_owners` (one row per owner): `owner TEXT PRIMARY KEY, wins INTEGER NOT NULL, identityIsKey INTEGER`;
  index `(wins DESC, owner ASC)`. The page is `WHERE (wins, owner) < (?, ?)` in that order, `LIMIT page + 1`:
  the #642 cursor `<wins>.<owner>` is already this keyset, so the client's `nextCursor` walk is unchanged.
- `board_hands` (one row per counted win whose winner resolved to an identity and filed a card-bearing claim):
  `potTxid, markerRowid, score, claimCreatedAt, gameId`; index `(score, claimCreatedAt, gameId)` (the era hands).

A row's predicate is `chain_wins_cte`'s verbatim (fresh verdict, decoded keys, the confirmed landing or the
witnessed final spend, the latched slot rule). The era cutoff stays a read filter on `potCreatedAt`; a configured
cutoff makes `board_owners` wrong, so the owners table carries one row per (era, owner) only if a cutoff is ever set
again on a live fleet (today none is); otherwise the read falls back to the candidate read when a cutoff is set.

## The triggers (inside the settle hooks' own statements)

Maintained by SQLite triggers in the source statement, never by a second write in Rust:
`AFTER UPDATE OF verdict, verdictTxid, spent, spentConfirmed, spenderFinal, spendingTxid ON pot_records`,
`AFTER INSERT ON network_seen` (the witness arm), `AFTER INSERT / UPDATE OF sigValid, seatSettlePubkey / DELETE ON
potparty_records` (the slot resolution), `AFTER INSERT / UPDATE OF claimValid ON result_markers_v2` (the hand),
and the eviction move (`evict_txid_everywhere`). Each trigger deletes the pot's `board_wins` row, re-inserts it
when the predicate holds, and adjusts `board_owners` by the difference (`wins = wins - 1` on the old owner,
`+ 1` on the new; a row at 0 is deleted). Every trigger is `CREATE TRIGGER IF NOT EXISTS`; its text is immutable
once shipped, so a change is a `DROP TRIGGER IF EXISTS` + `CREATE` pair APPENDED to the list. No production writer
may use `INSERT OR REPLACE` on the source tables (a REPLACE skips the delete trigger).

## The one-time backfill

Behind a `view_derivation_state` stamp (`board-v1`): `BOARD_WINS_BACKFILL`, `BOARD_OWNERS_BACKFILL`
(`INSERT ... SELECT owner, COUNT(*) ... GROUP BY owner` over `board_wins`), `BOARD_HANDS_BACKFILL`, then
`STATE_DONE`. The guard sits in the OUTER derived table
(`FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM view_derivation_state WHERE name='board-v1')) g CROSS JOIN ...`),
never in the `WHERE`: SQLite does not hoist a constant `NOT EXISTS` above the loops. The stamped replay is pinned at
zero scan steps on the populated fixture (`write_derived_reads.rs`'s shape).

## The pins (all three #638 pins, plus the drift pin)

1. Byte identity: the #642 harness (`tests/support/leaderboard_642.rs`) gains a third reader, the table read,
   held byte-identical to the whole-board read and to the candidate read over the fleet-shaped board
   (`tests/support/d1_board.rs`) at every page and cursor, the #685 decoration included.
2. `EXPLAIN QUERY PLAN`: the page, owners and hands reads in `hot_routes_never_scan_fleet_tables`, plus a
   `board_owners` page that is a SEARCH on its rank index with no temp B-tree for the ORDER BY.
3. `maxReads`: `make ci-d1-board` replays the table read on local D1; the default page's ceiling drops from
   58,574 to the page's own size (rows of `board_owners` read + the page's pots), flat at 1,500 and 3,000 wins.
4. Drift: after every writer pin of the settle path, the derived rows equal the candidate read's ranking of
   the whole fixture (the trigger set is complete, or this is red).

## Migration certification

`ensure_overlay_migrations` compares the persisted count and fingerprint, so the list re-executes once per worker
version and on each old-version cold start in the deploy window: every statement must be benign on rerun
(`IF NOT EXISTS`, the stamped backfills), and no table is dropped or renamed. The app layer reads the tables only
after the overlay's migration count says they exist (the latch-column capability's pattern).

## The money gate

A #638 doctrine change is a money change: the derived count decides what the board shows as a counted win, and
the same predicates mint the chain spine the client counts from. The lane goes through the lens (two consecutive
clean rounds, a from-scratch reviewer among them) before it lands, and its REPORT names the drift pin's RED.
