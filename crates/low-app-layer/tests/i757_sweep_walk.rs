//! bsv-low #757, the FIX-757 lens's MEDIUM-1: the stranger sweep (`stored::SWEEP`) and the scheduled refresh
//! (`stored::DUE`) spare a recorded pot or hop spender (`pot_records.spendingTxid`) only while its word is not yet
//! mined or terminal. `pot_records` rows are never deleted, so a spender arm with no word filter (as built at
//! `a7f8d44`) made every settled pot's word row permanent, and each 15-minute pass walked all of them from the old end
//! of `tx_words_age`: the lens measured `SWEEP` at 22,023 VM steps over 1,000 mined spared settles, deleting none,
//! where main's walked 2,070 and deleted 64.
//!
//! These pins run the shipped statements over SQLite under the overlay's real migrations: (1) at N = 1,000 mined
//! settles each named by a `pot_records` pointer, `SWEEP` deletes them as main's would (64 a pass, the same passes),
//! its VM steps a pass within twice main's and its plan an index walk with two index probes; (2) the spender arm
//! still spares a spender whose word still matters, and the `transactions` arm is main's byte for byte.
use low_app_layer::tracker_host::stored;
use rusqlite::{params, Connection, StatementStatus};

/// `e86c168` (main), verbatim.
const MAIN_SWEEP: &str = "DELETE FROM tx_words WHERE txid IN (SELECT w.txid FROM tx_words w WHERE w.updated_at < ?1 AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.txid=w.txid) ORDER BY w.updated_at,w.txid LIMIT 64)";
const MAIN_DUE: &str = "SELECT w.txid FROM tx_words w WHERE w.word IN ('unknown','built','announced','seen') AND w.updated_at <= ?1 AND EXISTS (SELECT 1 FROM transactions t WHERE t.txid=w.txid) AND COALESCE(json_extract(w.legacy_body,'$.trackerTerminal'),0) = 0 ORDER BY w.updated_at,w.txid LIMIT 2";

const N: u64 = 1_000;
const CUTOFF: u64 = 10;

fn db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{sql}: {e}");
        }
    }
    db.execute_batch(stored::CREATE).unwrap();
    db.execute_batch(stored::AGE_INDEX).unwrap();
    db
}

/// `n` mined settles, aged past the cutoff, each the recorded spender of its own pot coin.
fn mined_settles(db: &Connection, n: u64) {
    for i in 0..n {
        let settle = format!("{i:064x}");
        db.execute(
            "INSERT INTO tx_words(txid,word,tier,updated_at) VALUES (?1,'mined','chain',1)",
            [&settle],
        )
        .unwrap();
        db.execute(
            "INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed) VALUES (?1,0,1,?2,1)",
            params![format!("{:064x}", i + 1_000_000), settle],
        )
        .unwrap();
    }
}

/// Rows a statement touched and its VM steps.
fn run(db: &Connection, sql: &str, arg: u64) -> (usize, i32) {
    let mut st = db.prepare(sql).unwrap();
    let n = if sql.starts_with("DELETE") {
        st.execute([arg]).unwrap()
    } else {
        st.query_map([arg], |r| r.get::<_, String>(0))
            .unwrap()
            .count()
    };
    (n, st.get_status(StatementStatus::VmStep))
}

fn plan(db: &Connection, sql: &str) -> Vec<String> {
    db.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap()
        .query_map([CUTOFF], |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn count(db: &Connection) -> u64 {
    db.query_row("SELECT COUNT(*) FROM tx_words", [], |r| r.get(0))
        .unwrap()
}

#[test]
fn i757_sweep_walk_mined_spared_settles_age_out_as_on_main() {
    let (ours, main) = (db(), db());
    mined_settles(&ours, N);
    mined_settles(&main, N);
    // the plan: the age index walked, both holds index probes, no table scan
    let p = plan(&ours, stored::SWEEP);
    assert!(p.iter().any(|l| l.contains("tx_words_age")), "{p:?}");
    assert!(p.iter().any(|l| l.contains("idx_pot_spending")), "{p:?}");
    assert!(
        !p.iter()
            .any(|l| l.starts_with("SCAN t") || l.starts_with("SCAN p")),
        "{p:?}"
    );
    // the first pass at N = 1,000: deletes what main's deletes, within twice main's walk
    let (deleted, steps) = run(&ours, stored::SWEEP, CUTOFF);
    let (main_deleted, main_steps) = run(&main, MAIN_SWEEP, CUTOFF);
    println!(
        "N={N}: SWEEP deleted={deleted} vm_steps={steps}; main deleted={main_deleted} vm_steps={main_steps}"
    );
    assert_eq!(
        (deleted, main_deleted),
        (64, 64),
        "a mined spared settle is not swept as on main (vm_steps {steps}, main {main_steps})"
    );
    assert!(
        steps <= 2 * main_steps,
        "SWEEP walked {steps} VM steps over {N} mined spared settles, main {main_steps}"
    );
    // every pass after it, to an empty table: the same passes as main, each within twice main's walk
    let mut passes = 1;
    loop {
        let (d, s) = run(&ours, stored::SWEEP, CUTOFF);
        let (md, ms) = run(&main, MAIN_SWEEP, CUTOFF);
        assert_eq!(d, md, "pass {passes}");
        assert!(
            s <= 2 * ms.max(20),
            "pass {passes}: {s} against main's {ms}"
        );
        if d == 0 {
            break;
        }
        passes += 1;
    }
    assert_eq!((count(&ours), count(&main), passes), (0, 0, 16));
    // DUE over what is left walks nothing, as main's
    let (due, due_steps) = run(&ours, stored::DUE, CUTOFF);
    let (_, main_due_steps) = run(&main, MAIN_DUE, CUTOFF);
    println!("after the sweep: DUE vm_steps={due_steps}; main's {main_due_steps}");
    assert_eq!(due, 0);
    assert!(due_steps <= 2 * main_due_steps.max(20));
}

/// The spender arm spares a spender only while its word still matters, and the `transactions` arm is main's.
#[test]
fn i757_sweep_walk_the_spender_arm_spares_only_a_word_that_still_matters() {
    let db = db();
    let word = |txid: &str, word: &str, legacy: Option<&str>| {
        db.execute(
            "INSERT INTO tx_words(txid,word,tier,updated_at,legacy_body) VALUES (?1,?2,'host',1,?3)",
            params![txid, word, legacy],
        )
        .unwrap();
    };
    let spends = |pot: &str, spender: &str| {
        db.execute(
            "INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed) VALUES (?1,0,1,?2,0)",
            params![pot, spender],
        )
        .unwrap();
    };
    let kept = ["unknown", "built", "announced", "seen"];
    let gone = ["mined", "stale", "abandoned"];
    let mut n = 0u8;
    let mut id = || {
        n += 1;
        (
            format!("{n:02x}").repeat(32),
            format!("{:02x}", n + 100).repeat(32),
        )
    };
    let mut spared = Vec::new();
    let mut swept = Vec::new();
    for w in kept {
        let (spender, pot) = id();
        word(&spender, w, None);
        spends(&pot, &spender);
        spared.push(spender);
        // the same word, terminal (an Arcade refusal), is not spared
        let (spender, pot) = id();
        word(&spender, w, Some(r#"{"trackerTerminal":true}"#));
        spends(&pot, &spender);
        swept.push(spender);
    }
    for w in gone {
        let (spender, pot) = id();
        word(&spender, w, None);
        spends(&pot, &spender);
        swept.push(spender);
    }
    // the transactions arm, main's: every word spared, mined included
    for w in kept.iter().chain(gone.iter()) {
        let (txid, _) = id();
        word(&txid, w, None);
        db.execute("INSERT INTO transactions(txid) VALUES (?1)", [&txid])
            .unwrap();
        spared.push(txid);
    }
    // a stranger: no hold at all
    let (stranger, _) = id();
    word(&stranger, "built", None);
    swept.push(stranger);
    // DUE refreshes exactly the spared spenders the spender arm holds (and main's transactions arm)
    let due: Vec<String> = db
        .prepare(&stored::DUE.replace("LIMIT 2", "LIMIT 100"))
        .unwrap()
        .query_map([CUTOFF], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    for s in &spared[..kept.len()] {
        assert!(due.contains(s), "{s} not due");
    }
    for s in &swept {
        assert!(!due.contains(s), "{s} due");
    }
    while db.execute(stored::SWEEP, [CUTOFF]).unwrap() > 0 {}
    let left: Vec<String> = db
        .prepare("SELECT txid FROM tx_words ORDER BY txid")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut want = spared.clone();
    want.sort();
    assert_eq!(left, want, "the sweep spared the wrong rows");
    // the transactions arm and DUE's text are main's; the spender arm's word filter is DUE's own
    assert!(stored::SWEEP.contains(
        &MAIN_SWEEP
            ["DELETE FROM tx_words WHERE txid IN (".len()..MAIN_SWEEP.find(" ORDER BY").unwrap()]
    ));
    let filter = "w.word IN ('unknown','built','announced','seen')";
    let terminal = "COALESCE(json_extract(w.legacy_body,'$.trackerTerminal'),0) = 0";
    for sql in [stored::SWEEP, stored::DUE] {
        assert!(sql.contains(filter) && sql.contains(terminal), "{sql}");
    }
}
