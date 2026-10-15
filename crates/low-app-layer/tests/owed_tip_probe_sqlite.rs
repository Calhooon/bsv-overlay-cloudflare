//! bsv-low #669: the owed list's hop-stranded `claimable` must be re-probed on a new tip. A non-LOW competitor's
//! confirming block fires no `pot-changed` / `hop-changed` / `/internal/tx-word` hook, so the #469 pot gate
//! (`OWED_STALE_ON_TIP_SQL`) never names a stranded-hop identity and its sweep claim stays frozen at the compute
//! tip. The twin `OWED_STALE_ON_TIP_HOPS_SQL` marks STALE exactly the identities that hold a LIVE claimable
//! stranded-hop row (keyed on the derived row, itself keyed on the hop OUTPOINT), so the next read / the #648
//! deferred alarm re-probes the chain at the new tip.
//!
//! These pins run the EXACT shipped SQL against real SQLite carrying the overlay's PRODUCTION migration list, like
//! `write_derived_reads.rs`. The pure degrade / `should_recompute` / budget pins are inline in `owed.rs`.

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use low_app_layer::owed::{OWED_STALE_ON_TIP_HOPS_SQL, OWED_STALE_ON_TIP_SQL};
use rusqlite::{params, Connection};

fn production_schema_db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            let msg = e.to_string().to_ascii_lowercase();
            assert!(
                msg.contains("duplicate column"),
                "production migration failed under real SQLite: {e}\n{sql}"
            );
        }
    }
    conn
}

/// One owed snapshot + one owed row for an identity, written straight into the shipped tables (no trigger owns
/// `owed_state` / `owed_rows`; the #638 guarded tables are untouched here).
fn seed(conn: &Connection, identity: &str, tip: i64, family: &str, claimable: bool) {
    conn.execute(
        "INSERT INTO owed_state (identity, computedAtMs, tip, rows, stale, truncated) VALUES (?1, ?2, ?3, 1, 0, 0)",
        params![identity, 1_792_000_000_000i64, tip],
    )
    .unwrap();
    let facts = format!(r#"{{"claimable": {claimable}, "chainProbe": "unspent"}}"#);
    conn.execute(
        "INSERT INTO owed_rows (identity, outpoint, family, gameId, sats, facts, updatedAtMs) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            identity,
            format!("{identity}-hop:0"),
            family,
            "game",
            20_190i64,
            facts,
            1_792_000_000_000i64
        ],
    )
    .unwrap();
}

fn stale_of(conn: &Connection, identity: &str) -> i64 {
    conn.query_row(
        "SELECT stale FROM owed_state WHERE identity = ?1",
        params![identity],
        |r| r.get::<_, i64>(0),
    )
    .unwrap()
}

/// PIN A5 (RED on the stub): `OWED_STALE_ON_TIP_HOPS_SQL` marks STALE exactly the identities holding a LIVE
/// claimable hop-stranded row — never a claimable=0 hop row, never another family, never an identity with only a
/// pot. And the #469 twin (`OWED_STALE_ON_TIP_SQL`), the only tip mark that runs today, marks NONE of them.
/// RED line: `assert_eq!(stale_of(&db, "id-claimable-hop"), 1, …)`.
#[test]
fn pin669_a5_the_hops_tip_sql_marks_only_claimable_stranded_identities() {
    let db = production_schema_db();
    seed(&db, "id-claimable-hop", 971_114, "hop-stranded", true);
    seed(&db, "id-swept-hop", 971_114, "hop-stranded", false);
    seed(&db, "id-payout", 971_114, "payout", true);

    // the new twin: marks only the claimable hop-stranded identity
    let marked = db.execute(OWED_STALE_ON_TIP_HOPS_SQL, []).unwrap();
    assert_eq!(
        marked, 1,
        "exactly one identity (the claimable stranded hop) is marked"
    );
    assert_eq!(
        stale_of(&db, "id-claimable-hop"),
        1,
        "a competitor's block must re-probe the identity holding a live claimable stranded hop"
    );
    assert_eq!(
        stale_of(&db, "id-swept-hop"),
        0,
        "a hop-stranded row that is not claimable is already fail-safe: never marked"
    );
    assert_eq!(
        stale_of(&db, "id-payout"),
        0,
        "another family's claimable row is not a stranded hop: never marked"
    );

    // the gap this fixes: the #469 pot gate, the ONLY tip mark that runs today, names none of these identities
    // (they are party to no unspent pot), so block 971115 fired no owed work for the stranded hop.
    let via_469 = db
        .execute(OWED_STALE_ON_TIP_SQL, params![971_115i64])
        .unwrap();
    assert_eq!(
        via_469, 0,
        "the pot-recovery tip gate marks no stranded-hop identity: this is exactly the loop-28 gap"
    );
}

/// PIN A5 byte-pin: the shipped SQL text, so a later edit to its selector (a `lower()` scan, a dropped family
/// guard, a widened predicate) is a RED, parity with the #469 twin's own text pin.
#[test]
fn pin669_a5_owed_stale_on_tip_hops_sql_text_is_pinned() {
    assert_eq!(
        OWED_STALE_ON_TIP_HOPS_SQL,
        "UPDATE owed_state SET stale = 1 WHERE identity IN (SELECT DISTINCT identity FROM owed_rows WHERE family = 'hop-stranded' AND json_extract(facts, '$.claimable') = 1)"
    );
}

/// PIN N5 (the FIX-669 lens's NOTE-5; RED before the overlay index migration lands): the `/internal/tip-changed`
/// twin runs `OWED_STALE_ON_TIP_HOPS_SQL` once per synced block, and its `owed_rows` subquery must not SCAN the
/// table (a cost that grows with every owed row the fleet writes). The overlay's `idx_owed_rows_claimable_hop`
/// (leading `json_extract(facts,'$.claimable')`, then `family`, then `identity`) turns the subquery's owed_rows
/// access into a COVERING index SEARCH on `(json_extract(claimable)=1 AND family='hop-stranded')` — no row fetch, no
/// temp b-tree for the DISTINCT identity. Pinned the way `d1_fleet_plans::hot_routes_never_scan_fleet_tables` pins
/// the hot routes: no line starts with `SCAN owed_rows`, and the subquery seeks THIS index (never the PK autoindex
/// that spans the whole table). The index LEADS with the json predicate so the #662 withheld-payout read
/// (`family='payout'`, no `claimable` predicate) cannot use it and keeps its (identity, outpoint) PK — pinned in
/// `write_derived_reads.rs::the_withheld_payouts_read_searches_owed_rows_by_its_primary_key`. `owed_rows` is the
/// overlay's table and the index lives in the overlay's migration list (the app-layer catch-up is CREATE-TABLE-only
/// by contract), so a cold app-layer isolate ahead of the overlay still RUNS the query (the PK-autoindex scan) until
/// the overlay warms — the index is a performance bound, not a correctness one. RED line:
/// `assert!(!detail.starts_with("SCAN owed_rows"), ...)` — before the index the plan is
/// `SCAN owed_rows USING INDEX sqlite_autoindex_owed_rows_1` (the PK autoindex, the whole owed-rows table).
#[test]
fn pin669_n5_the_hops_tip_sql_seeks_an_index_never_scans_owed_rows() {
    let db = production_schema_db();
    let mut stmt = db
        .prepare(&format!("EXPLAIN QUERY PLAN {OWED_STALE_ON_TIP_HOPS_SQL}"))
        .unwrap();
    // the `detail` column (index 3) of EXPLAIN QUERY PLAN, like d1_fleet_plans::plan.
    let details: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    for detail in &details {
        assert!(
            !detail.starts_with("SCAN owed_rows"),
            "the tip-hops stale mark scans owed_rows per block: {detail}\nfull plan: {details:?}"
        );
    }
    assert!(
        details
            .iter()
            .any(|d| d.contains("idx_owed_rows_claimable_hop")),
        "the subquery must seek the claimable-hop index, never the owed_rows PK autoindex: {details:?}"
    );
}

/// The number of rows `idx_owed_rows_claimable_hop` actually holds, read from `sqlite_stat1` after `ANALYZE`
/// (the first integer of its `stat` string is the index's row count). This is every INSERT's index-write cost:
/// one `owed_rows` INSERT writes one entry into this index for every row the index COVERS.
fn claimable_hop_index_rows(conn: &Connection) -> i64 {
    conn.execute_batch("ANALYZE").unwrap();
    conn.query_row(
        "SELECT stat FROM sqlite_stat1 WHERE idx = 'idx_owed_rows_claimable_hop'",
        [],
        |r| r.get::<_, String>(0),
    )
    .map(|stat| {
        stat.split_whitespace()
            .next()
            .and_then(|n| n.parse::<i64>().ok())
            .unwrap_or(0)
    })
    .unwrap_or(0)
}

/// PIN 669C (RED on main's FULL index): `idx_owed_rows_claimable_hop` must index ONLY hop-stranded rows, never the
/// (abundant) in-progress "rejoin" rows, which are ALSO `claimable = 1`. The FULL index
/// `(json_extract(claimable), family, identity)` covers every claimable row, so every owed snapshot pays one index
/// write per claimable in-progress row it inserts: identity A's 8 in-progress pots pushed the budget tier's owed
/// first read from 18 to 26 writes, over the 20 ceiling (bsv-low #669, the FIX-669C regression; the design promised
/// no read-path cost). The fix makes the index PARTIAL (`WHERE family = 'hop-stranded'`), keeping its three key
/// columns so `OWED_STALE_ON_TIP_HOPS_SQL` still SEEKS a COVERING index (pin669_n5), while an in-progress row writes
/// NOTHING to it. Measured on `sqlite_stat1`: the index holds exactly the hop-stranded-claimable rows, strictly
/// fewer than every claimable row. RED line on the full index: `assert_eq!(indexed, stranded_claimable)` — the full
/// index holds all 9, the partial holds 1.
#[test]
fn pin669c_the_claimable_hop_index_does_not_index_in_progress_rows() {
    let db = production_schema_db();
    // eight in-progress "rejoin" rows (claimable = 1), exactly A's budget-fixture shape, plus one claimable
    // stranded hop. Only the stranded hop belongs in the claimable-hop index.
    for i in 0..8 {
        db.execute(
            "INSERT INTO owed_rows (identity, outpoint, family, gameId, facts, updatedAtMs) VALUES (?1, ?2, 'in-progress', 'g', ?3, 1)",
            params!["id-a", format!("a{i}:0"), r#"{"claimable": true, "claim": "rejoin"}"#],
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO owed_rows (identity, outpoint, family, gameId, facts, updatedAtMs) VALUES ('id-s', 's:0', 'hop-stranded', 'g', ?1, 1)",
        params![r#"{"claimable": true, "chainProbe": "unspent"}"#],
    )
    .unwrap();

    let total_claimable: i64 = db
        .query_row(
            "SELECT count(*) FROM owed_rows WHERE json_extract(facts, '$.claimable') = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let stranded_claimable: i64 = db
        .query_row(
            "SELECT count(*) FROM owed_rows WHERE family = 'hop-stranded' AND json_extract(facts, '$.claimable') = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        total_claimable, 9,
        "precondition: every seeded row is claimable"
    );
    assert_eq!(
        stranded_claimable, 1,
        "precondition: one of them is a stranded hop"
    );

    let indexed = claimable_hop_index_rows(&db);
    assert_eq!(
        indexed, stranded_claimable,
        "the claimable-hop index must hold ONLY the hop-stranded rows ({stranded_claimable}), not every claimable \
         row ({total_claimable}): a FULL index taxes every owed snapshot one write per in-progress rejoin row \
         (the FIX-669C 18->26 budget regression)"
    );
    assert!(
        indexed < total_claimable,
        "the index must exclude the in-progress rejoin rows (indexed {indexed} of {total_claimable} claimable)"
    );
}
