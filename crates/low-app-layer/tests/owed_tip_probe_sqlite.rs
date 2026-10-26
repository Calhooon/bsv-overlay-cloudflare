//! Gate marks use the indexed owed-row columns; the legacy hop index stays partial during rollout.
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use low_app_layer::owed_judge::MARK_TIP_SQL;
use rusqlite::{params, Connection};
fn production_schema_db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(error) = db.execute_batch(sql) {
            assert!(
                error.to_string().contains("duplicate column"),
                "{error}: {sql}"
            );
        }
    }
    db
}

#[test]
fn owed893_tip_marks_only_due_gates_and_advances_the_generation() {
    let db = production_schema_db();
    db.execute_batch("INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs,gateHeight) VALUES ('due','p:0','unbound','g','{}',0,7),('equal','p:0','unbound','g','{}',0,8),('future','p:0','unbound','g','{}',0,9),('absent','p:0','unbound','g','{}',0,NULL)").unwrap();
    assert_eq!(db.execute(MARK_TIP_SQL, params![8]).unwrap(), 2);
    let first: i64 = db
        .query_row("SELECT MAX(generation) FROM owed_dirty", [], |r| r.get(0))
        .unwrap();
    assert_eq!(db.execute(MARK_TIP_SQL, params![8]).unwrap(), 2);
    let last: i64 = db
        .query_row("SELECT MIN(generation) FROM owed_dirty", [], |r| r.get(0))
        .unwrap();
    assert!(
        last > first,
        "a later tip cannot be acknowledged by an earlier judge"
    );
    assert!(!MARK_TIP_SQL.contains("json_extract"));
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
/// columns so the legacy hop lookup keeps its partial index, while an in-progress row writes
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
