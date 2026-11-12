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
    spender_bytes_sweep_pass, spender_bytes_sweep_pass_under, OWED_SPENDER_BYTES_AGE_CURSOR_SQL,
    OWED_SPENDER_BYTES_CAP_SWEEP_SQL, OWED_SPENDER_BYTES_CEILING_COUNT_SQL,
    OWED_SPENDER_BYTES_CEILING_CURSOR_SQL, OWED_SPENDER_BYTES_NAMED_SWEPT_COUNTER,
    OWED_SPENDER_BYTES_RETENTION_MS, OWED_SPENDER_BYTES_STORE_CAP, OWED_SPENDER_BYTES_SWEEP_SQL,
    OWED_SPENDER_BYTES_SWEEP_WINDOW, OWED_SPENDER_BYTES_UPSERT_SQL,
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
    (gone[1].clone(), gone[4].clone(), steps)
}

/// One pass of the store's ceiling at `ceiling` rows, its three shipped statements in order (one D1 batch on the
/// Worker): the txids it took and, of them, the named ones.
fn cap(db: &Connection, ceiling: i64) -> (Vec<String>, Vec<String>) {
    cap_steps(db, ceiling).0
}

/// [`cap`] with the three statements' VM steps.
fn cap_steps(db: &Connection, ceiling: i64) -> ((Vec<String>, Vec<String>), i64) {
    let pass = spender_bytes_sweep_pass_under(NOW, ceiling);
    let mut steps = run(db, pass[2].0, &pass[2].1).1 + run(db, pass[3].0, &pass[3].1).1;
    let mut stmt = db.prepare(pass[4].0).unwrap();
    let rows: Vec<(String, i64)> = stmt
        .query_map(params_from_iter(&pass[4].1), |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    steps += stmt.get_status(rusqlite::StatementStatus::VmStep) as i64;
    let mut all: Vec<String> = rows.iter().map(|(t, _)| t.clone()).collect();
    let mut named: Vec<String> = rows
        .iter()
        .filter(|(_, n)| *n == 1)
        .map(|(t, _)| t.clone())
        .collect();
    all.sort();
    named.sort();
    ((all, named), steps)
}

/// The durable count of the named rows the ceiling took.
fn named_swept(db: &Connection) -> i64 {
    db.query_row(
        "SELECT COALESCE((SELECT value FROM ops_counters WHERE name=?1),0)",
        [OWED_SPENDER_BYTES_NAMED_SWEPT_COUNTER],
        |r| r.get(0),
    )
    .unwrap()
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

/// (b) THE STORE'S CEILING (OWED-1184-FOLD, LOW-1; the rule since fold 2): past it the unreferenced rows go, never
/// more than the excess; a named row goes only after a whole lap of the store found no unreferenced row, and each
/// named row taken is answered `named` and counted durably. Under the production ceiling nothing is taken. RED with
/// the ceiling made inert (`max(0, ..)` taken as 0).
#[test]
fn owed1184_b_the_ceiling_takes_the_unreferenced_first_then_the_oldest_named() {
    let db = migrated();
    let mut named = Vec::new();
    for i in 0..3 {
        let s = txid("5e", i);
        pointer(&db, &txid("b1", i), &s);
        // the named rows are the OLDEST
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
    // a ceiling of 2: four over; this lap found unreferenced rows, so only they go (three), no named row
    assert_eq!(
        cap(&db, 2),
        (vec![txid("5f", 2), txid("5f", 3), txid("5f", 4)], vec![]),
        "no named row while the lap found an unreferenced one"
    );
    assert_eq!(named_swept(&db), 0);
    // the next lap finds none: then the OLDEST named row (the trip, answered named, counted)
    assert_eq!(
        cap(&db, 2),
        (vec![named[0].clone()], vec![named[0].clone()]),
        "a clean lap: the oldest named row"
    );
    assert_eq!(named_swept(&db), 1);
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
    let mut tripped_at = Vec::new();
    for kept in [5_000i64, 50_000] {
        let db = fleet_with(kept);
        for sql in [
            OWED_SPENDER_BYTES_AGE_CURSOR_SQL,
            OWED_SPENDER_BYTES_SWEEP_SQL,
            OWED_SPENDER_BYTES_CEILING_CURSOR_SQL,
            OWED_SPENDER_BYTES_CEILING_COUNT_SQL,
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
        // OWED-1184 fold 2: the ceiling TRIPPED is bounded too, its cursor's window and, clean, its newest and oldest
        // windows: the steps at 50,000 kept rows are those at 5,000
        let over = counted(&db) - 1;
        let ((took, _), walk) = cap_steps(&db, over);
        assert!(took.is_empty(), "a lap not yet clean takes no named row");
        db.execute(
            "UPDATE owed_belt_state SET cursor=json_set(cursor,'$.clean',1) WHERE name='owed-spender-bytes:ceiling'",
            [],
        )
        .unwrap();
        let ((took, named), clean) = cap_steps(&db, over);
        assert_eq!(
            (took.len(), named.len()),
            (1, 1),
            "clean: the oldest named row"
        );
        eprintln!("owed1184_c: the ceiling tripped over {kept} kept rows: {walk} VM steps walking, {clean} clean");
        tripped_at.push((walk, clean));
    }
    assert!(
        steps_at[1] <= steps_at[0] + steps_at[0] / 20,
        "the pass's steps grow with the store: {steps_at:?}"
    );
    assert!(steps_at[1] < 120_000, "{steps_at:?}");
    for (a, b) in [
        (tripped_at[0].0, tripped_at[1].0),
        (tripped_at[0].1, tripped_at[1].1),
    ] {
        assert!(
            b <= a + a / 20,
            "the tripped ceiling's steps grow with the store: {tripped_at:?}"
        );
    }
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

// =============================================================================
// OWED-1184 fold 2 (the delta lens `OWED-1184-DELTA-LENS-2026-11-12.md`): LOW-A, LOW-B, NOTE-2, NOTE-3.
// =============================================================================

/// The rows the ceiling takes, pass after pass at `ceiling`, until a pass takes nothing: every txid taken and, of
/// them, the named ones.
fn cap_until_quiet(db: &Connection, ceiling: i64) -> (Vec<String>, Vec<String>) {
    let (mut all, mut named) = (Vec::new(), Vec::new());
    for _ in 0..16 {
        let (a, n) = cap(db, ceiling);
        if a.is_empty() && counted(db) <= ceiling {
            break;
        }
        all.extend(a);
        named.extend(n);
    }
    all.sort();
    named.sort();
    (all, named)
}

/// (f2_a) LOW-A, THE LENS'S CASE: 600 old pointer-named rows and 10 young unreferenced ones, the ceiling 5 over. The 5
/// taken are UNREFERENCED (the oldest five of them); no named row goes, none is counted. RED under bbcb9e79's rule
/// (unreferenced first within the 512 oldest only): the 5 oldest NAMED rows taken, answered named.
#[test]
fn owed1184f2_a_the_ceiling_takes_unreferenced_rows_anywhere_before_a_named_one() {
    let db = migrated();
    for i in 0..600 {
        let s = txid("5e", i);
        pointer(&db, &txid("b1", i), &s);
        bytes(&db, &s, 1_000 + i);
    }
    for i in 0..10 {
        bytes(&db, &txid("5f", i), 10_000 + i);
    }
    assert_eq!(counted(&db), 610);
    let (all, named) = cap_until_quiet(&db, 605);
    assert_eq!(
        (all, named),
        ((0..5).map(|i| txid("5f", i)).collect::<Vec<_>>(), vec![]),
        "the five taken are unreferenced"
    );
    assert_eq!(counted(&db), 605);
    assert_eq!(named_swept(&db), 0);
    let left: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM owed_spender_bytes WHERE txid LIKE '5e%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(left, 600, "every named row stands");
}

/// (f2_b) The rule's other edges: a store of named rows only gives up its OLDEST named rows only after a whole lap
/// found no unreferenced row (the first pass, a window of 512 of 600, takes nothing); once clean, an unreferenced row
/// written since (the newest) goes before a named one; under the ceiling the cursor is reset, so the next trip judges
/// a whole lap again. Each named row is counted durably.
#[test]
fn owed1184f2_b_named_rows_go_only_after_a_clean_lap_and_after_the_newest_unreferenced() {
    let db = migrated();
    let named: Vec<String> = (0..600).map(|i| txid("5e", i)).collect();
    for (i, s) in named.iter().enumerate() {
        pointer(&db, &txid("b1", i as i64), s);
        bytes(&db, s, 1_000 + i as i64);
    }
    // the first window: 512 rows, all named, the lap not over: nothing taken
    assert_eq!(cap(&db, 595), (vec![], vec![]));
    // the lap ends clean: the five oldest named rows, counted
    let five: Vec<String> = named[..5].to_vec();
    assert_eq!(cap(&db, 595), (five.clone(), five));
    assert_eq!(named_swept(&db), 5);
    // a young unreferenced row written after the clean lap: it goes first, no named row
    let young = txid("5f", 1);
    bytes(&db, &young, 50_000);
    assert_eq!(cap(&db, 595), (vec![young], vec![]));
    assert_eq!(named_swept(&db), 5);
    // under the ceiling: nothing taken and the cursor reset (wrap 1, clean 0)
    assert_eq!(cap(&db, 595), (vec![], vec![]));
    let (wrap, clean): (i64, i64) = db
        .query_row(
            "SELECT json_extract(cursor,'$.wrap'), json_extract(cursor,'$.clean') FROM owed_belt_state WHERE name='owed-spender-bytes:ceiling'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (wrap, clean),
        (1, 0),
        "the trip's cursor reset under the ceiling"
    );
    // the next trip walks a whole lap again before a named row goes
    assert_eq!(cap(&db, 590), (vec![], vec![]));
    let next: Vec<String> = named[5..10].to_vec();
    assert_eq!(cap(&db, 590), (next.clone(), next));
    assert_eq!(named_swept(&db), 10);
}

/// (f2_c) LOW-B: in Phase C (no `owed_rows`, no marks) the ceiling's delete of a spender kept for EACH reason marks
/// its seat: the belt's evidence (alice's index-unspent hop), an evicted pointer (dave's hop moved to
/// `pot_records_evicted`) and an index pointer (carol's). RED with either new lookup of
/// `owed_dirty_owed_spender_bytes_delete_pointer` removed: alice (or dave) gets no mark.
#[test]
fn owed1184f2_c_every_ceiling_delete_of_a_kept_row_marks_its_seat_in_phase_c() {
    let db = migrated();
    let marker = |hop: &str, id: &str| {
        db.execute(
            "INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES (?1,'bob','game',0,1000,'k','s','i',2,?2,1,1,1)",
            params![id, hop],
        )
        .unwrap();
    };
    let (ev, ptr, evicted) = (txid("5a", 1), txid("5a", 2), txid("5a", 3));
    let (hop_e, hop_p, hop_v) = (txid("b2", 1), txid("b2", 2), txid("b2", 3));
    db.execute(
        "INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES (?1,0,0,1,'p2pkh')",
        [&hop_e],
    )
    .unwrap();
    pointer(&db, &hop_p, &ptr);
    db.execute(
        "INSERT INTO pot_records_evicted(af_evictedAt,af_reason,txid,outputIndex,spent,spendingTxid,spentConfirmed,createdAt,lockKind) VALUES (1,'fixture',?1,0,1,?2,1,1,'p2pkh')",
        params![hop_v, evicted],
    )
    .unwrap();
    marker(&hop_e, "alice");
    marker(&hop_p, "carol");
    marker(&hop_v, "dave");
    db.execute(
        low_app_layer::hops_view::PROBE_MEMO_UPSERT_SQL,
        params![format!("{hop_e}:0"), 1, 1, &ev, 1, 899_000],
    )
    .unwrap();
    bytes(&db, &ev, 1);
    bytes(&db, &ptr, 2);
    bytes(&db, &evicted, 3);
    db.execute("DELETE FROM owed_dirty", []).unwrap();
    let rows: i64 = db
        .query_row("SELECT COUNT(*) FROM owed_rows", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 0, "the premise: Phase C, D1 holds no owed row");
    let (all, named) = cap(&db, 0);
    assert_eq!(all.len(), 3);
    assert_eq!(named.len(), 3, "each is a kept row");
    let marks: Vec<(String, String, String, i64)> = db
        .prepare("SELECT identity, source, sourceKey, downgrade FROM owed_dirty ORDER BY identity")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        marks,
        vec![
            ("alice".into(), "bytes".into(), ev, 1),
            ("carol".into(), "bytes".into(), ptr, 1),
            ("dave".into(), "bytes".into(), evicted, 1),
        ],
        "each seat marked, a downgrade"
    );
}

/// (f2_d) NOTE-2: the count's backfill over a POPULATED store (the path prod takes: `owed_spender_bytes` holds rows
/// before migration 504). The list up to 503, rows written, then the rest: the count is the rows; a re-run of the
/// whole list keeps it (the guard: one probe, no recount, even over a count planted wrong); the triggers keep it.
#[test]
fn owed1184f2_d_the_count_backfill_over_a_populated_store() {
    let db = Connection::open_in_memory().unwrap();
    let apply = |list: &[&str]| {
        for sql in list {
            if let Err(e) = db.execute_batch(sql) {
                assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
            }
        }
    };
    apply(&OVERLAY_MIGRATIONS[..503]);
    let present: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='count_owed_spender_bytes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(present, 0, "the premise: the count is migration 504's");
    for i in 0..37 {
        bytes(&db, &txid("5c", i), NOW + i);
    }
    apply(&OVERLAY_MIGRATIONS[503..]);
    assert_eq!(counted(&db), 37, "the backfill counted the populated store");
    // the guard: a re-run of the whole list does not recount, even a count planted wrong
    db.execute("UPDATE count_owed_spender_bytes SET n=999 WHERE id=1", [])
        .unwrap();
    apply(OVERLAY_MIGRATIONS);
    assert_eq!(counted(&db), 999, "the re-run's guard holds");
    db.execute("UPDATE count_owed_spender_bytes SET n=37 WHERE id=1", [])
        .unwrap();
    bytes(&db, &txid("5c", 99), NOW);
    assert_eq!(counted(&db), 38);
    db.execute(
        "DELETE FROM owed_spender_bytes WHERE txid=?1",
        [txid("5c", 0)],
    )
    .unwrap();
    assert_eq!(counted(&db), 37);
}

/// (f2_e) NOTE-3: the named count rides the delete's batch: the ceiling is three statements in one pass's order
/// (cursor, count, delete), the count's statement names the durable counter and reads the same take as the delete,
/// and over a run of trips the counter equals the named rows answered. The route runs them in one D1 batch
/// (`routes::sweep_owed_spender_bytes`, `counted_batch(&db, vec![cursor, count, delete])`).
#[test]
fn owed1184f2_e_the_named_count_rides_the_deletes_batch() {
    let pass = spender_bytes_sweep_pass(NOW);
    assert_eq!(
        [pass[2].0, pass[3].0, pass[4].0],
        [
            OWED_SPENDER_BYTES_CEILING_CURSOR_SQL,
            OWED_SPENDER_BYTES_CEILING_COUNT_SQL,
            OWED_SPENDER_BYTES_CAP_SWEEP_SQL
        ]
    );
    assert!(OWED_SPENDER_BYTES_CEILING_COUNT_SQL
        .contains(&format!("'{OWED_SPENDER_BYTES_NAMED_SWEPT_COUNTER}'")));
    let take = |sql: &str| sql[..sql.find("INSERT").or(sql.find("DELETE")).unwrap()].to_string();
    assert_eq!(
        take(OWED_SPENDER_BYTES_CEILING_COUNT_SQL),
        take(OWED_SPENDER_BYTES_CAP_SWEEP_SQL),
        "the count and the delete read one take"
    );
    let routes = include_str!("../src/routes.rs");
    let body = &routes[routes
        .find("pub async fn sweep_owed_spender_bytes(")
        .unwrap()..];
    let body: String = body[..body.find("\n}\n").unwrap()]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    assert!(
        body.contains("counted_batch(&db,vec![cursor,count,delete])"),
        "the route batches the ceiling's three statements"
    );
    assert!(
        !body.contains("BUMP_COUNTER_SQL"),
        "no count outside the batch"
    );
    // a run of trips: the counter is the named rows answered
    let db = migrated();
    for i in 0..20 {
        let s = txid("5e", i);
        pointer(&db, &txid("b1", i), &s);
        bytes(&db, &s, 1_000 + i);
    }
    for i in 0..4 {
        bytes(&db, &txid("5f", i), 5_000 + i);
    }
    let (all, named) = cap_until_quiet(&db, 10);
    assert_eq!((all.len(), named.len()), (14, 10));
    assert_eq!(named_swept(&db), 10);
}
