//! OWED-1184 (bsv-low #1184): a spender's stored bytes that an INDEX POINTER names (`pot_records.spendingTxid`) are
//! never swept. They are the evidence a judged "spent outside this game" story rests on (#672's rule: a judged owed
//! row never regresses); the eight-day age sweep and the row cap took them, so a fresh judgement of the same record
//! went back to "could not judge". Every pin runs the SHIPPED sweep statements over the overlay's migrations on real
//! SQLite.
//!
//! OWED-1184-FOLD (the lens `OWED-1184-LENS-2026-11-12.md`): the age sweep walks a bounded window of the age index past
//! its cursor (LOW-1), the store has a ceiling again whose named share's trip is counted (LOW-1), the belt's evidence
//! names a spender before the index points at it (LOW-2), and an evicted pointer still names it (LOW-3).
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use low_app_layer::owed::{
    spender_bytes_sweep_pass, OWED_SPENDER_BYTES_AGE_CURSOR_SQL, OWED_SPENDER_BYTES_CAP_SWEEP_SQL,
    OWED_SPENDER_BYTES_RETENTION_MS, OWED_SPENDER_BYTES_STORE_CAP, OWED_SPENDER_BYTES_SWEEP_LIMIT,
    OWED_SPENDER_BYTES_SWEEP_SQL, OWED_SPENDER_BYTES_SWEEP_WINDOW, OWED_SPENDER_BYTES_UPSERT_SQL,
};
use rusqlite::{params, params_from_iter, Connection};

const NOW: i64 = 1_790_000_000_000;

fn migrated() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db
}

fn txid(tag: &str, n: i64) -> String {
    format!("{tag}{n:062x}")
}

/// A spent hop row of the index naming `spender` (confirmed).
fn pointer(db: &Connection, hop: &str, spender: &str) {
    db.execute(
        "INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,spentHeight,createdAt,lockKind) VALUES (?1,0,1,?2,1,899000,1789000000,'p2pkh')",
        params![hop, spender],
    )
    .unwrap();
}

fn bytes(db: &Connection, spender: &str, at: i64) {
    db.execute(
        OWED_SPENDER_BYTES_UPSERT_SQL,
        params![spender, r#"{"inputs":[],"outputs":[]}"#, at],
    )
    .unwrap();
}

/// One shipped statement's first column, with its VM steps.
fn run(db: &Connection, sql: &str, binds: &[i64]) -> (Vec<String>, i64) {
    let mut stmt = db.prepare(sql).unwrap();
    let mut out: Vec<String> = stmt
        .query_map(params_from_iter(binds), |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let steps = stmt.get_status(rusqlite::StatementStatus::VmStep) as i64;
    out.sort();
    (out, steps)
}

/// One pass of the shipped sweeps at `now_ms` (`owed::spender_bytes_sweep_pass`): the age sweep's deleted txids, the
/// ceiling's, and the pass's VM steps.
fn pass(db: &Connection, now_ms: i64) -> (Vec<String>, Vec<String>, i64) {
    let mut gone = Vec::new();
    let mut steps = 0;
    for (sql, binds) in spender_bytes_sweep_pass(now_ms) {
        let (out, s) = run(db, sql, &binds);
        gone.push(out);
        steps += s;
    }
    let capped = gone.pop().unwrap();
    let aged = gone.pop().unwrap();
    (aged, capped, steps)
}

/// The ceiling at `ceiling` rows: the txids it took and, of them, the named ones.
fn cap(db: &Connection, ceiling: i64) -> (Vec<String>, Vec<String>) {
    let mut stmt = db.prepare(OWED_SPENDER_BYTES_CAP_SWEEP_SQL).unwrap();
    let rows: Vec<(String, i64)> = stmt
        .query_map(
            params![
                ceiling,
                OWED_SPENDER_BYTES_SWEEP_LIMIT,
                OWED_SPENDER_BYTES_SWEEP_WINDOW
            ],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut all: Vec<String> = rows.iter().map(|(t, _)| t.clone()).collect();
    let mut named: Vec<String> = rows
        .iter()
        .filter(|(_, n)| *n == 1)
        .map(|(t, _)| t.clone())
        .collect();
    all.sort();
    named.sort();
    (all, named)
}

fn held(db: &Connection) -> Vec<String> {
    let mut out: Vec<String> = db
        .prepare("SELECT txid FROM owed_spender_bytes")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    out.sort();
    out
}

fn counted(db: &Connection) -> i64 {
    db.query_row(
        "SELECT n FROM count_owed_spender_bytes WHERE id=1",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

const LATER: i64 = NOW + OWED_SPENDER_BYTES_RETENTION_MS + 1_000;

/// (a) The age sweep at now plus eight days: a spender an index pointer names is KEPT; a spender no pointer names (a
/// stranger's, an old find no hop row records) is still swept. RED on main: the named spender went too.
#[test]
fn owed1184_a_the_age_sweep_keeps_a_pointer_named_spender_and_sweeps_the_rest() {
    let db = migrated();
    let (named, stranger) = (txid("5e", 1), txid("5f", 2));
    pointer(&db, &txid("b1", 1), &named);
    bytes(&db, &named, NOW);
    bytes(&db, &stranger, NOW);
    let (aged, capped, _) = pass(&db, LATER);
    assert_eq!(
        (aged, capped),
        (vec![stranger.clone()], vec![]),
        "only the unreferenced row is swept"
    );
    assert_eq!(
        held(&db),
        vec![named.clone()],
        "the named spender's bytes are held"
    );
    assert_eq!(counted(&db), 1, "the row count follows the store");
    // a second pass at the same time deletes nothing (the kept row is not re-taken)
    assert!(pass(&db, LATER).0.is_empty());
    // the pointer cleared (the overlay re-points): the bytes are an unreferenced row again, taken by the next lap
    db.execute("UPDATE pot_records SET spendingTxid=NULL, spent=0", [])
        .unwrap();
    assert_eq!(pass(&db, LATER).0, vec![named]);
    assert_eq!(counted(&db), 0);
}

/// (b) THE STORE'S CEILING (OWED-1184-FOLD, LOW-1): past it, the oldest rows go, the unreferenced first, then the
/// oldest NAMED rows, never more than the excess; each named row taken is answered `named` (the trip, counted). Under
/// the production ceiling nothing is taken. RED with the ceiling made inert (`max(0, ..)` taken as 0).
#[test]
fn owed1184_b_the_ceiling_takes_the_unreferenced_first_then_the_oldest_named() {
    let db = migrated();
    let mut named = Vec::new();
    for i in 0..3 {
        let s = txid("5e", i);
        pointer(&db, &txid("b1", i), &s);
        // the named rows are the OLDEST: the ceiling's window is read oldest first
        bytes(&db, &s, 1_000 + i);
        named.push(s);
    }
    for i in 0..5 {
        bytes(&db, &txid("5f", i), 2_000 + i);
    }
    assert_eq!(counted(&db), 8);
    assert_eq!(
        cap(&db, OWED_SPENDER_BYTES_STORE_CAP),
        (vec![], vec![]),
        "under the ceiling nothing is taken"
    );
    // a ceiling of 6: two over, the two oldest UNREFERENCED rows go, every named row stays
    assert_eq!(
        cap(&db, 6),
        (vec![txid("5f", 0), txid("5f", 1)], vec![]),
        "the excess, unreferenced first"
    );
    assert_eq!(counted(&db), 6);
    // a ceiling of 2: four over, the three strangers left, then the OLDEST named row (the trip, answered named)
    assert_eq!(
        cap(&db, 2),
        (
            vec![
                named[0].clone(),
                txid("5f", 2),
                txid("5f", 3),
                txid("5f", 4)
            ],
            vec![named[0].clone()]
        ),
        "the named share's trip takes the oldest named row"
    );
    assert_eq!(held(&db), vec![named[1].clone(), named[2].clone()]);
    assert_eq!(counted(&db), 2);
    assert_eq!(cap(&db, 2), (vec![], vec![]));
}

fn plan(db: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let binds = vec![0i64; stmt.parameter_count()];
    stmt.query_map(params_from_iter(binds), |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The fleet-shaped fixture plus `kept` pointer-named spenders, all older than the cutoff (nothing the age sweep may
/// delete), and one unreferenced old row behind them all.
fn fleet_with(kept: i64) -> Connection {
    let db = migrated();
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_fleet::rows(1_790_000_000) {
        d1_fleet::execute_row(&db, &sql);
    }
    for i in 0..kept {
        let s = txid("5d", i);
        pointer(&db, &txid("b0", i), &s);
        bytes(&db, &s, NOW + i);
    }
    bytes(&db, &txid("5f", 0), NOW + kept);
    db.execute_batch("COMMIT; ANALYZE").unwrap();
    db
}

/// (c) #638 and LOW-1: a pass is BOUNDED whatever the store holds. Neither sweep scans `pot_records`, the evicted rows
/// or the probes (a seek of each partial index per candidate), and `owed_spender_bytes` is read only through its age
/// index or its primary key; the pass's VM steps at 50,000 kept rows are those at 5,000 (the window, not the table).
/// The unreferenced row behind the kept ones is taken within one lap, `ceil(rows / window)` passes. RED on 239c1ae9:
/// `SCAN b` and a temp B-tree over the whole table, the steps linear (70,039 at 5,000 kept, the lens).
#[test]
fn owed1184_c_a_pass_is_bounded_by_the_window_never_the_store() {
    let mut steps_at = Vec::new();
    for kept in [5_000i64, 50_000] {
        let db = fleet_with(kept);
        for sql in [
            OWED_SPENDER_BYTES_AGE_CURSOR_SQL,
            OWED_SPENDER_BYTES_SWEEP_SQL,
            OWED_SPENDER_BYTES_CAP_SWEEP_SQL,
        ] {
            let p = plan(&db, sql);
            eprintln!("owed1184_c plan: {p:?}");
            for d in &p {
                assert!(
                    !(d.starts_with("SCAN b") || d.starts_with("SCAN owed_spender_bytes"))
                        || d.contains("idx_owed_spender_bytes_age"),
                    "the store is scanned whole: {p:?}"
                );
                for t in [
                    "SCAN p",
                    "SCAN e",
                    "SCAN h",
                    "SCAN pot_records",
                    "SCAN hop_chain_probes",
                ] {
                    assert!(
                        !(d == t || d.starts_with(&format!("{t} "))),
                        "a pointer table is scanned: {p:?}"
                    );
                }
            }
        }
        // the first pass: the window's rows read, none deletable (every one is named)
        let (aged, capped, steps) = pass(&db, NOW + kept + OWED_SPENDER_BYTES_RETENTION_MS + 1);
        assert_eq!((aged.len(), capped.len()), (0, 0));
        eprintln!("owed1184_c: one pass over {kept} kept rows: {steps} VM steps");
        steps_at.push(steps);
        // the unreferenced row behind them all goes within one lap
        let laps =
            (kept + 1 + OWED_SPENDER_BYTES_SWEEP_WINDOW - 1) / OWED_SPENDER_BYTES_SWEEP_WINDOW;
        let mut took = None;
        for n in 1..=laps {
            let (aged, _, s) = pass(&db, NOW + kept + OWED_SPENDER_BYTES_RETENTION_MS + 1);
            assert!(
                s <= 2 * steps_at[steps_at.len() - 1] + 1_000,
                "pass {n}: {s} steps"
            );
            if !aged.is_empty() {
                took = Some((n, aged));
                break;
            }
        }
        let (n, aged) = took.expect("the unreferenced row is taken within one lap");
        assert_eq!(aged, vec![txid("5f", 0)]);
        eprintln!(
            "owed1184_c: the unreferenced row taken at pass {} of a lap of {laps}",
            n + 1
        );
    }
    assert!(
        steps_at[1] <= steps_at[0] + steps_at[0] / 20,
        "the pass's steps grow with the store: {steps_at:?}"
    );
    assert!(steps_at[1] < 120_000, "{steps_at:?}");
}

/// (d) LOW-2: a spender the belt FOUND (its evidence `<hop>:<vout>` naming it, its bytes written) is kept before the
/// index points at it; a probe memo `<hop>.<vout>` or the route's `spentany:` cache naming it keeps nothing. RED with
/// the `hop_chain_probes` arm removed.
#[test]
fn owed1184_d_a_belt_found_spend_is_kept_before_the_index_points_at_it() {
    let db = migrated();
    let (found, memo, cache) = (txid("5a", 1), txid("5a", 2), txid("5a", 3));
    let hop = txid("b2", 1);
    // the belt's find, as `owed_belt::Find::statements` writes it: the evidence, then the bytes; no pointer
    db.execute(
        low_app_layer::hops_view::PROBE_MEMO_UPSERT_SQL,
        params![format!("{hop}:0"), NOW, 1, &found, 1, 899_000],
    )
    .unwrap();
    db.execute(
        low_app_layer::hops_view::PROBE_MEMO_UPSERT_SQL,
        params![format!("{hop}.1"), NOW, 1, &memo, 1, 899_000],
    )
    .unwrap();
    db.execute(
        low_app_layer::hops_view::PROBE_MEMO_UPSERT_SQL,
        params![format!("spentany:{hop}.2"), NOW, 1, &cache, 1, 899_000],
    )
    .unwrap();
    for s in [&found, &memo, &cache] {
        bytes(&db, s, NOW);
    }
    let pointers: i64 = db
        .query_row("SELECT COUNT(*) FROM pot_records", [], |r| r.get(0))
        .unwrap();
    assert_eq!(pointers, 0, "the premise: no index pointer");
    let (mut aged, _, _) = pass(&db, LATER);
    aged.sort();
    let mut want = vec![memo, cache];
    want.sort();
    assert_eq!(aged, want, "the memo and the cache name nothing");
    assert_eq!(held(&db), vec![found.clone()], "the belt's find is kept");
    // the evidence retired (an unspent answer): an unreferenced row again
    db.execute(
        low_app_layer::hops_view::PROBE_EVIDENCE_CLEAR_SQL,
        [format!("{hop}:0")],
    )
    .unwrap();
    assert_eq!(pass(&db, LATER).0, vec![found]);
}

/// (e) LOW-3: an EVICTED pointer still names the spender: the hop's row moved to `pot_records_evicted` across the
/// eight-day mark keeps the bytes, and its readmission finds them. RED with the evicted arm removed.
#[test]
fn owed1184_e_an_evicted_pointer_keeps_the_bytes() {
    let db = migrated();
    let (s, hop) = (txid("5b", 1), txid("b3", 1));
    pointer(&db, &hop, &s);
    bytes(&db, &s, NOW);
    // the eviction's shadow move of the hop's row
    db.execute_batch(&format!(
        "INSERT INTO pot_records_evicted(af_evictedAt,af_reason,txid,outputIndex,spent,spendingTxid,spentConfirmed,spentHeight,createdAt,lockKind) SELECT {NOW},'fixture',txid,outputIndex,spent,spendingTxid,spentConfirmed,spentHeight,createdAt,lockKind FROM pot_records WHERE txid='{hop}'; DELETE FROM pot_records WHERE txid='{hop}'"
    ))
    .unwrap();
    assert_eq!(pass(&db, LATER).0, Vec::<String>::new());
    assert_eq!(
        held(&db),
        vec![s.clone()],
        "the evicted pointer keeps the bytes"
    );
    // the readmission moves it back: still named
    db.execute_batch(&format!(
        "INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,spentHeight,createdAt,lockKind) SELECT txid,outputIndex,spent,spendingTxid,spentConfirmed,spentHeight,createdAt,lockKind FROM pot_records_evicted WHERE txid='{hop}'; DELETE FROM pot_records_evicted WHERE txid='{hop}'"
    ))
    .unwrap();
    assert_eq!(pass(&db, LATER).0, Vec::<String>::new());
    assert_eq!(held(&db), vec![s]);
}
