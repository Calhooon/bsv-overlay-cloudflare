//! bsv-low #825 (B-FLEET-4): the conflict pass's fault memo, the SHIPPED statements under real SQLite over the
//! overlay's production migrations.
use bsv_overlay_cloudflare::{admit_fast::*, d1::OVERLAY_MIGRATIONS};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeSet;

const NOW: i64 = 1_800_000_000_000;

fn db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    conn
}

fn pot(conn: &Connection, txid: &str) {
    conn.execute(
        "INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES (?1,0,0,1)",
        [txid],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO pot_beefs(txid,beef,createdAt,has_proof,proof_verified,structurally_unprovable) VALUES (?1, x'00', 1, 0, 0, 0)",
        [txid],
    )
    .unwrap();
}

fn drawn(conn: &Connection, now: i64) -> BTreeSet<String> {
    let mut q = conn.prepare(CONFLICT_CANDIDATES_SQL).unwrap();
    let rows = q
        .query_map([now], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<BTreeSet<_>, _>>()
        .unwrap();
    rows
}

/// One faulted re-check, as the worker writes it: the hold this write armed, if any.
fn fault(conn: &Connection, txid: &str, now: i64) -> Option<i64> {
    let (faults, held): (i64, Option<i64>) = conn
        .query_row(
            CONFLICT_FAULT_MEMO_PUT_SQL,
            params![
                txid,
                now,
                CONFLICT_FAULT_MEMO_BOUND,
                CONFLICT_FAULT_MEMO_HOLD_MS,
                conflict_fault_words(&"courier exhausted ".repeat(20)),
            ],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert!(faults >= 1);
    conflict_hold_armed(now, held)
}

fn clean(conn: &Connection, txid: &str) {
    conn.execute(CONFLICT_FAULT_MEMO_CLEAR_SQL, [txid]).unwrap();
}

fn memo(conn: &Connection, txid: &str) -> Option<(i64, Option<i64>, i64, String)> {
    conn.query_row(
        "SELECT faults, held_until, holds, last_fault FROM conflict_fault_memo WHERE txid = ?1",
        [txid],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
    .optional()
    .unwrap()
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

/// bsv-low #825 (B-FLEET-4): A VOID POT IS NOT RE-DRAWN FOREVER. A pot whose re-check faults three times in a row
/// is held out of the draw for 24 h, so the eight places go to pots the couriers can answer and
/// `admit_fast_conflict_faults_total` stops climbing on the same void pots; a pot the index holds SEEN or funded is
/// never held (neither at the write nor at the draw, a latch landing after the hold included); the hold ends at its
/// time and one more fault re-arms it; a clean re-check ends the run; the sweep takes only old, unheld rows; the
/// pots' own rows are never touched. RED on `edcc72e4` with this file's statements grafted and the draw's exclusion
/// removed: the void pot is drawn on every pass.
#[test]
fn b_fleet_4_a_void_pot_is_held_after_three_consecutive_faults() {
    let conn = db();
    let (void, seen, funded, quiet, late) = (
        "a1".repeat(32),
        "b2".repeat(32),
        "c3".repeat(32),
        "d4".repeat(32),
        "e5".repeat(32),
    );
    for t in [&void, &seen, &funded, &quiet, &late] {
        pot(&conn, t);
    }
    conn.execute(
        "INSERT INTO network_seen(txid, seenAt) VALUES (?1, 1)",
        [&seen],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO transactions(txid, beef, has_proof) VALUES (?1, x'00', 1)",
        [&funded],
    )
    .unwrap();
    let all = set(&[&void, &seen, &funded, &quiet, &late]);
    assert_eq!(drawn(&conn, NOW), all, "no memo: every pot is drawn");

    // two faults are not the bound
    for t in [&void, &seen, &funded, &late] {
        assert_eq!(fault(&conn, t, NOW), None);
        assert_eq!(fault(&conn, t, NOW + 1), None);
    }
    assert_eq!(drawn(&conn, NOW + 2), all, "two faults hold nothing");
    // the third arms the hold for the void pot alone
    assert_eq!(
        fault(&conn, &void, NOW + 2),
        Some(NOW + 2 + CONFLICT_FAULT_MEMO_HOLD_MS),
        "the third consecutive fault arms the hold"
    );
    assert_eq!(
        fault(&conn, &seen, NOW + 2),
        None,
        "a SEEN pot is never held"
    );
    assert_eq!(
        fault(&conn, &funded, NOW + 2),
        None,
        "a funded pot is never held"
    );
    assert_eq!(
        fault(&conn, &late, NOW + 2),
        Some(NOW + 2 + CONFLICT_FAULT_MEMO_HOLD_MS)
    );
    let (faults, held, holds, words) = memo(&conn, &void).unwrap();
    assert_eq!(
        (faults, held, holds),
        (3, Some(NOW + 2 + CONFLICT_FAULT_MEMO_HOLD_MS), 1)
    );
    assert_eq!(words.chars().count(), 160, "the fault's words are clipped");
    assert_eq!(memo(&conn, &seen).unwrap().1, None);

    // the draw: the void pot is out; the SEEN, funded and quiet pots stay
    let after = set(&[&seen, &funded, &quiet]);
    for pass in 0..8 {
        assert_eq!(
            drawn(&conn, NOW + 3 + pass * 900_000),
            after,
            "the held pots are never re-drawn while their hold runs (pass {pass})"
        );
    }
    // a SEEN latch landing AFTER the hold puts the pot back in the draw at once
    conn.execute(
        "INSERT INTO network_seen(txid, seenAt) VALUES (?1, 2)",
        [&late],
    )
    .unwrap();
    assert!(
        drawn(&conn, NOW + 3).contains(&late),
        "a pot latched SEEN is drawn whatever the memo says"
    );

    // the health: two rows hold (void; late is SEEN now, so it is not counted held), the soonest release
    let (rows, held, next, max): (i64, i64, Option<i64>, i64) = conn
        .query_row(CONFLICT_FAULT_MEMO_HEALTH_SQL, [NOW + 3], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })
        .unwrap();
    assert_eq!(
        (rows, held, next, max),
        (4, 1, Some(NOW + 2 + CONFLICT_FAULT_MEMO_HOLD_MS), 3)
    );

    // the hold ends at its time; one more fault re-arms it at once
    let end = NOW + 2 + CONFLICT_FAULT_MEMO_HOLD_MS;
    assert!(
        !drawn(&conn, end - 1).contains(&void),
        "held to its last ms"
    );
    assert!(
        drawn(&conn, end).contains(&void),
        "drawn again at the hold's end"
    );
    assert_eq!(
        fault(&conn, &void, end + 1),
        Some(end + 1 + CONFLICT_FAULT_MEMO_HOLD_MS),
        "a fault past the hold re-arms it"
    );
    assert_eq!(memo(&conn, &void).unwrap().2, 2, "two holds armed");
    assert!(!drawn(&conn, end + 2).contains(&void));

    // a clean re-check ends the run: the memo's row goes, the pot is drawn, and the count restarts
    clean(&conn, &void);
    assert_eq!(memo(&conn, &void), None);
    assert!(drawn(&conn, end + 2).contains(&void));
    assert_eq!(
        fault(&conn, &void, end + 3),
        None,
        "consecutive: the run restarted at one"
    );

    // the sweep: a week-old row whose hold ended goes; a held row and a recent one stay
    let old = "f6".repeat(32);
    pot(&conn, &old);
    for i in 0..3 {
        fault(&conn, &old, NOW + i);
    }
    let sweep_at = NOW + CONFLICT_FAULT_MEMO_SWEEP_AFTER_MS + 10;
    conn.execute(
        CONFLICT_FAULT_MEMO_SWEEP_SQL,
        params![
            sweep_at - CONFLICT_FAULT_MEMO_SWEEP_AFTER_MS,
            sweep_at,
            CONFLICT_FAULT_MEMO_SWEEP_LIMIT
        ],
    )
    .unwrap();
    assert_eq!(
        memo(&conn, &old),
        None,
        "a week-old row whose hold ended is swept"
    );
    assert!(memo(&conn, &void).is_some(), "a recent row stays");

    // never a pot: every pot row and every BEEF row is where it was
    for table in ["pot_records", "pot_beefs"] {
        let n: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 6, "{table}: the memo deletes no pot");
    }
}
