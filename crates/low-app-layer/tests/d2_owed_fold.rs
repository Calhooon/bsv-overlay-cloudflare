//! The D2-OWED lens's folds (bsv-low `docs/audit/D2-OWED-LENS-2026-10-25.md`), each on the producer path: the
//! shipped writers, judge, drain, read and belt over the overlay's migrations on real SQLite.
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;
use low_app_layer::owed_judge as judge;
use owed_db::*;
use rusqlite::types::Value as SqlValue;
use serde_json::Value;

fn row_of<'a>(body: &'a Value, outpoint: &str) -> &'a Value {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == outpoint)
        .unwrap_or_else(|| panic!("no row {outpoint} in {body}"))
}

/// HIGH-1: the #669 freshness gate at the read. A sweep the judge found claimable at tip T on a COURIER probe alone
/// (the index does not hold the hop) is served claimable at T, degraded to a pending chain-wait at T+1, and withheld
/// when the present tip cannot be read. Re-aimed by the delta lens's D-H1: an index-UNSPENT sweep keeps its press
/// across a block (`d2_owed_delta.rs`, `d2delta_h1_*`), so the gate is pinned where it still applies.
#[tokio::test]
async fn d2fold_high1_a_stored_sweep_degrades_at_the_read_across_a_block() {
    let db = Db::new();
    seed_courier_unspent_hop(&db, "owner", "g00", &hop(0), 900_000);
    drain_all(&db).await;
    let op = format!("{}:0", hop(0));
    let fresh = served(&db, "owner", Some(900_000)).await;
    let row = row_of(&fresh, &op);
    assert_eq!(row["family"], "hop-stranded");
    assert_eq!(row["facts"]["claim"], "sweep-hop");
    assert_eq!(row["facts"]["claimable"], true, "tip-current: the press stands");

    let moved = served(&db, "owner", Some(900_001)).await;
    let row = row_of(&moved, &op);
    assert_eq!(row["facts"]["claim"], Value::Null, "{row}");
    assert_eq!(row["facts"]["claimable"], false);
    assert_eq!(row["facts"]["chainWait"], "block");
    assert_eq!(row["facts"]["tipMoved"], true);
    assert_eq!(row["facts"]["servedTip"], 900_001);
    assert_eq!(
        row["reason"],
        low_app_layer::owed::SWEEP_TIP_MOVED_REASON,
        "{row}"
    );

    let unknown = served(&db, "owner", None).await;
    let row = row_of(&unknown, &op);
    assert_eq!(row["facts"]["claimable"], false, "{row}");
    assert_eq!(row["facts"]["claim"], Value::Null);
    assert_eq!(row["facts"]["tipUnknown"], true);

    // A judge with no tip (TIP-877) stores a claim no read serves as a press.
    db.tip.set(None);
    judge::judge_subjects(&db, "owner", &["g00".into()], "test")
        .await
        .unwrap();
    assert_eq!(
        db.opt(&format!(
            "SELECT probeTip FROM owed_rows WHERE outpoint='{op}'"
        )),
        None
    );
    for tip in [Some(900_000), Some(900_001), None] {
        let body = served(&db, "owner", tip).await;
        assert_eq!(row_of(&body, &op)["facts"]["claimable"], false, "{tip:?}");
    }
}

/// HIGH-1: on a tip the courier-dependent claimable sweeps judged below it are dated due now (the design's tip row,
/// narrowed by the delta lens's D-H1), over a partial index, so the belt re-probes them first; a sweep judged at the
/// new tip, and every other row, keep their date.
#[tokio::test]
async fn d2fold_high1_b_a_tip_dates_the_claimable_sweeps_due_now() {
    let db = Db::new();
    seed_courier_unspent_hop(&db, "owner", "g00", &hop(0), 900_000);
    seed_courier_unspent_hop(&db, "owner", "g01", &hop(1), 900_000);
    drain_all(&db).await;
    let later = db.now.get() as i64 + 3 * HOUR;
    db.exec(&format!("UPDATE owed_rows SET beltDueAtMs={later}"));
    db.exec(&format!(
        "UPDATE owed_rows SET probeTip=900001 WHERE outpoint='{}:0'",
        hop(1)
    ));
    db.now.set(db.now.get() + 1_000);
    judge::mark_tip(&db, 900_001).await.unwrap();
    let due = |n: usize| {
        db.opt(&format!(
            "SELECT beltDueAtMs FROM owed_rows WHERE outpoint='{}:0'",
            hop(n)
        ))
    };
    assert_eq!(due(0), Some(db.now.get() as i64), "judged below the tip: due now");
    assert_eq!(due(1), Some(later), "judged at the tip: its date stands");
    let plan = db.plan(
        judge::DATE_SWEEPS_SQL,
        &[SqlValue::Integer(900_001), SqlValue::Integer(1)],
    );
    assert!(
        plan.iter()
            .any(|d| d.contains("USING INDEX idx_owed_rows_sweep")),
        "{plan:?}"
    );
    assert!(!plan.iter().any(|d| d.starts_with("SCAN")), "{plan:?}");
}

/// HIGH-2: the tip marks only the design's window, `gateHeight BETWEEN tip - 6 AND tip`; a row past its gate long
/// ago is not re-marked at every block.
#[tokio::test]
async fn d2fold_high2_the_tip_marks_the_seven_block_window_only() {
    let db = Db::new();
    for (n, gate) in [
        (0, 899_000),
        (1, 899_993),
        (2, 899_994),
        (3, 899_999),
        (4, 900_000),
        (5, 900_001),
    ] {
        db.exec(&format!("INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs,gateHeight) VALUES('owner','{}:0','unbound','g{n}','{{}}',0,{gate})", hop(n)));
    }
    db.exec("DELETE FROM owed_dirty");
    db.run_marks_only(900_000).await;
    let marked: Vec<String> = {
        let conn = db.conn.borrow();
        let mut s = conn
            .prepare("SELECT gameId FROM owed_dirty WHERE source='tip' AND sourceKey='gate' ORDER BY gameId")
            .unwrap();
        s.query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    assert_eq!(marked, ["g2", "g3", "g4"], "the window is [tip - 6, tip]");
}

trait MarkOnly {
    async fn run_marks_only(&self, tip: u64);
}
impl MarkOnly for Db {
    async fn run_marks_only(&self, tip: u64) {
        use low_app_layer::pot_hook::{Bind, SqlPort, Stmt};
        self.run_batch(vec![Stmt {
            sql: judge::MARK_TIP_SQL.into(),
            binds: vec![Bind::Int(tip as i64)],
        }])
        .await
        .unwrap();
    }
}

use low_app_layer::owed_belt::{self as belt, Courier};
use std::cell::RefCell;

type Answer = Option<(u16, Vec<u8>)>;
/// Answers by URL fragment; counts every call (a refused ask never reaches it).
#[derive(Default)]
struct Fake {
    calls: RefCell<Vec<String>>,
    answers: RefCell<Vec<(String, Answer)>>,
}
impl Fake {
    fn on(&self, fragment: &str, answer: Answer) {
        self.answers.borrow_mut().push((fragment.into(), answer));
    }
}
impl Courier for Fake {
    async fn get(&self, url: String) -> Option<(u16, Vec<u8>)> {
        self.calls.borrow_mut().push(url.clone());
        for (fragment, answer) in self.answers.borrow().iter().rev() {
            if url.contains(fragment.as_str()) {
                return answer.clone();
            }
        }
        if url.ends_with("/spend") {
            return Some((200, br#"{"spent":false}"#.to_vec()));
        }
        Some((404, b"{}".to_vec()))
    }
}
fn due(db: &Db, outpoint: &str) -> Option<i64> {
    db.opt(&format!(
        "SELECT beltDueAtMs FROM owed_rows WHERE outpoint='{outpoint}'"
    ))
}
fn spender_paying_elsewhere(spent: &str) -> (String, Vec<u8>) {
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
    let mut tx = Transaction::new();
    tx.add_input(TransactionInput::new(spent.to_string(), 0))
        .unwrap();
    tx.add_output(TransactionOutput {
        satoshis: Some(20_000),
        locking_script: LockingScript::from_hex(&format!("76a914{}88ac", "11".repeat(20))).unwrap(),
        change: false,
    })
    .unwrap();
    (tx.id(), tx.to_binary())
}

/// HIGH-3: the tick takes its eight round-robin by identity. A stranger holding 1,000 due rows, every one due before
/// the honest seat's, does not delay the honest row's look past the first tick.
#[tokio::test]
async fn d2fold_high3_a_a_thousand_stranger_rows_leave_the_honest_look_in_the_first_tick() {
    let db = Db::new();
    seed_stranded_hop(&db, "owner", "g00", &hop(0));
    drain_all(&db).await;
    db.exec(&format!(
        "UPDATE owed_rows SET beltDueAtMs={} WHERE identity='owner'",
        NOW as i64 - 1
    ));
    {
        let conn = db.conn.borrow();
        let tx = conn.unchecked_transaction().unwrap();
        for n in 0..1_000 {
            tx.execute("INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs,judgedAtMs,beltDueAtMs,probeTip) VALUES('stranger',?1,'hop-stranded',?2,'{}',0,0,?3,900000)", rusqlite::params![format!("{}:0", hop(10_000 + n)), format!("s{n}"), NOW as i64 - 1_000_000 + n as i64]).unwrap();
        }
        tx.commit().unwrap();
    }
    let fake = Fake::default();
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(report.candidates, belt::OWED_BELT_OUTPOINTS_PER_TICK);
    assert!(
        fake.calls.borrow().iter().any(|u| u.contains(&hop(0))),
        "the honest row is looked at in the first tick: {:?}",
        fake.calls.borrow()
    );
    // the walk and the per-identity due read are covering-index searches: the tick never reads the due set
    let plan = db.plan(
        belt::BELT_DUE_SQL,
        &[SqlValue::Text("owner".into()), SqlValue::Integer(NOW as i64), SqlValue::Integer(8)],
    );
    assert!(
        plan.iter().any(|d| d.contains(
            "USING COVERING INDEX idx_owed_rows_belt_turn (identity=? AND beltDueAtMs<?)"
        )),
        "{plan:?}"
    );
    let walk = db.plan(
        belt::BELT_IDENTITIES_SQL,
        &[SqlValue::Text(String::new()), SqlValue::Integer(64)],
    );
    assert!(
        walk.iter().any(|d| d.contains("USING COVERING INDEX idx_owed_rows_belt_turn (identity>?)")),
        "{walk:?}"
    );
    for p in plan.iter().chain(&walk) {
        assert!(!p.starts_with("SCAN owed_rows"), "{p}");
    }
}

/// HIGH-3: the judge dates only a row that owes the chain a question it can answer. A hop whose marker does not verify
/// (anyone's byte-format row) is never dated; a spender whose bytes are held is never asked for again.
#[tokio::test]
async fn d2fold_high3_b_no_date_for_an_unverified_marker_or_a_held_spender() {
    let db = Db::new();
    let made = (NOW as i64 - 2 * HOUR) / 1000;
    let (planted_spender, _) = spender_paying_elsewhere(&hop(1));
    db.exec(&format!("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,createdAt,lockKind) VALUES ('{h}',0,1,'{planted_spender}',{made},'p2pkh'); INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('victim','other','g01',0,20190,'seat','00','00',2,'{h}',1,{made},0);", h = hop(1)));
    drain_all(&db).await;
    let planted = format!("{}:0", hop(1));
    assert_eq!(
        db.text(&format!(
            "SELECT family FROM owed_rows WHERE outpoint='{planted}'"
        ))
        .as_deref(),
        Some("unbound")
    );
    assert_eq!(due(&db, &planted), None, "an unverified marker is never dated");

    // a verified hop the belt finds spent by a transaction it then holds the bytes of: no further look is owed
    seed_stranded_hop(&db, "owner", "g00", &hop(0));
    drain_all(&db).await;
    let op = format!("{}:0", hop(0));
    db.exec(&format!(
        "UPDATE owed_rows SET beltDueAtMs={} WHERE outpoint='{op}'",
        NOW as i64 - 1
    ));
    let (spender, raw) = spender_paying_elsewhere(&hop(0));
    let fake = Fake::default();
    fake.on(
        &format!("{}/0/spent", hop(0)),
        Some((
            200,
            serde_json::json!({"txid": spender, "vin": 0, "status": "unconfirmed"})
                .to_string()
                .into_bytes(),
        )),
    );
    fake.on(
        &format!("tx/{spender}/hex"),
        Some((200, hex::encode(&raw).into_bytes())),
    );
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(report.found, 1, "{report:?}");
    assert_eq!(
        db.int(&format!(
            "SELECT COUNT(*) FROM owed_spender_bytes WHERE txid='{spender}'"
        )),
        1
    );
    assert_eq!(
        due(&db, &op),
        None,
        "the spender's bytes are held: {:?}",
        db.text(&format!("SELECT facts FROM owed_rows WHERE outpoint='{op}'"))
    );
}

/// HIGH-3: a look that changed nothing backs its row off, doubling, to `OWED_BELT_BACKOFF_MAX_MS`.
#[tokio::test]
async fn d2fold_high3_c_a_look_that_changed_nothing_doubles_its_period_to_a_ceiling() {
    let db = Db::new();
    seed_stranded_hop(&db, "owner", "g00", &hop(0));
    drain_all(&db).await;
    let op = format!("{}:0", hop(0));
    let fake = Fake::default();
    // each look runs at the row's own due time, so the period the backoff reads is the one the last look left
    let look = |at: u64| {
        db.now.set(at);
        belt::tick(&db, &fake)
    };
    let mut at = due(&db, &op).unwrap() as u64;
    look(at).await.unwrap();
    let first = due(&db, &op).unwrap() - at as i64;
    assert_eq!(first, 600_000, "the first look changed the probe: the base period");
    let mut period = first;
    for _ in 0..8 {
        at += period as u64;
        look(at).await.unwrap();
        let next = due(&db, &op).unwrap() - at as i64;
        assert_eq!(
            next,
            (2 * period).min(belt::OWED_BELT_BACKOFF_MAX_MS),
            "an unchanged look doubles to the ceiling"
        );
        period = next;
    }
    assert_eq!(period, belt::OWED_BELT_BACKOFF_MAX_MS);
}

fn plant_parties(db: &Db, identity: &str, games: usize) {
    let conn = db.conn.borrow();
    let tx = conn.unchecked_transaction().unwrap();
    for n in 0..games {
        tx.execute("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES (?1,'other',?2,?3,0,1,'00',?3,1,?4,0)", rusqlite::params![identity, format!("s{n:05}"), format!("{:064x}", 0x5000_0000u64 + n as u64), 1_000 + n as i64]).unwrap();
    }
    tx.commit().unwrap();
}

/// MEDIUM-1: one stranger's planted games and one read of `/owed` no longer buy the fleet's drain. The wanted
/// envelope expands to the identity's newest 32 games (the rest marked `backfill`), and the drain takes its subjects
/// round-robin across identities by age, so an honest residual is judged in the first pass.
#[tokio::test]
async fn d2fold_medium1_a_a_planted_identity_does_not_preempt_the_drain() {
    let db = Db::new();
    plant_parties(&db, "stranger", 300);
    db.exec("DELETE FROM owed_dirty");
    seed_stranded_hop(&db, "owner", "g00", &hop(0));
    // the stranger's one read: no snapshot, the identity probe, one wanted envelope
    let _ = served(&db, "stranger", Some(900_000)).await;
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty WHERE source='wanted' AND gameId=''"),
        1
    );
    let r = judge::drain_dirty(&db, judge::OWED_DIRTY_SUBJECTS_PER_PASS)
        .await
        .unwrap();
    assert!(r.faults.is_empty(), "{:?}", r.faults);
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_rows WHERE identity='owner'"),
        1,
        "the honest residual is judged in the first pass"
    );
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty WHERE identity='owner'"),
        0
    );
    let wanted = db.int(
        "SELECT COUNT(DISTINCT gameId) FROM owed_dirty WHERE identity='stranger' AND source='wanted'",
    ) + db.int("SELECT COUNT(*) FROM owed_rows WHERE identity='stranger' AND gameId>='s00268'");
    assert!(wanted <= judge::OWED_WANTED_GAMES as i64, "{wanted}");
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty WHERE identity='stranger' AND source='wanted' AND gameId<'s00268'"),
        0,
        "only the newest 32 are wanted"
    );
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty WHERE identity='stranger' AND source='backfill'"),
        300 - judge::OWED_WANTED_GAMES as i64,
        "the rest wait as backfill marks"
    );
}

/// MEDIUM-1: the SHIPPED drain statements, never proxies: none scans `owed_dirty`.
#[test]
fn d2fold_medium1_b_the_shipped_drain_statements_never_scan_owed_dirty() {
    assert!(judge::EXPAND_WANTED_SQL.contains(&format!(" LIMIT {})", judge::OWED_WANTED_GAMES)));
    assert!(judge::EXPAND_BACKFILL_SQL.contains(&format!(" OFFSET {})", judge::OWED_WANTED_GAMES)));
    let db = Db::new();
    let int = SqlValue::Integer;
    let text = |s: &str| SqlValue::Text(s.into());
    for (name, sql, binds) in [
        ("wanted envelopes", judge::WANTED_ENVELOPES_SQL, vec![int(64)]),
        ("identity walk", judge::DRAIN_IDENTITIES_SQL, vec![text(""), int(64)]),
        ("identity marks", judge::DRAIN_MARKS_SQL, vec![text("owner"), int(64), int(0)]),
        ("identity age marks", judge::DRAIN_MARKS_SQL, vec![text("owner"), int(64), int(1)]),
        ("hook subjects", judge::CHANGED_SQL, vec![text("[\"owner\"]"), text("[\"gate\"]"), int(16)]),
        ("judge marks", judge::MARKS_SQL, vec![text("owner"), text("[\"g\"]")]),
        ("expand wanted", judge::EXPAND_WANTED_SQL, vec![text("owner")]),
        ("expand backfill", judge::EXPAND_BACKFILL_SQL, vec![text("owner")]),
        ("age marks", low_app_layer::owed_backfill::AGE_MARK_SQL, vec![int(1), int(3)]),
        ("read pending", low_app_layer::owed_read::PENDING_SQL, vec![text("owner")]),
    ] {
        let plan = db.plan(sql, &binds);
        assert!(
            !plan.iter().any(|d| d.starts_with("SCAN owed_dirty") || d.starts_with("SCAN d ") || d == "SCAN d"),
            "{name}: {plan:?}"
        );
    }
}

/// MEDIUM-2: the filing-fault counter is the overlay's seeded name, served from 0.
#[test]
fn d2fold_medium2_c_the_filing_fault_counter_is_seeded_by_the_overlay() {
    let names: Vec<String> = bsv_overlay_cloudflare::applayer_counters::OWED_JUDGE
        .iter()
        .map(|n| n.to_string())
        .collect();
    assert_eq!(judge::counter_names(), names);
    let seeded = bsv_overlay_cloudflare::applayer_counters::seeded_names();
    assert!(names.iter().all(|n| seeded.contains(n)));
}

/// MEDIUM-5: a keyless row (an old worker's write, or a rollback's: `judgedAtMs` NULL) is repaired by the age re-mark
/// after the stamp too, bounded by the same 128 pending age marks, over its own partial index.
#[tokio::test]
async fn d2fold_medium5_the_age_repair_reaches_keyless_rows() {
    let db = Db::new();
    for n in 0..200usize {
        seed_stranded_hop(&db, "legacy", &format!("k{n:03}"), &hop(20_000 + n));
    }
    db.exec("DELETE FROM owed_dirty");
    // the old walk's whole-identity write: every row keyless
    db.exec("INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs) SELECT identity,txid||':0','hop-stranded',gameId,'{\"claim\":\"sweep-hop\",\"claimable\":true}',1 FROM hopparty_records WHERE identity='legacy'");
    assert_eq!(
        low_app_layer::owed_backfill::mark_aged(&db).await.unwrap(),
        128,
        "the bound holds"
    );
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty WHERE source='age' AND identity='legacy'"),
        128
    );
    drain_all(&db).await;
    assert_eq!(low_app_layer::owed_backfill::mark_aged(&db).await.unwrap(), 72);
    drain_all(&db).await;
    assert_eq!(low_app_layer::owed_backfill::mark_aged(&db).await.unwrap(), 0);
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_rows WHERE identity='legacy' AND judgedAtMs IS NULL"),
        0,
        "every keyless row rewritten with its keys"
    );
    let plan = db.plan(
        low_app_layer::owed_backfill::AGE_MARK_UNJUDGED_SQL,
        &[SqlValue::Integer(128)],
    );
    assert!(
        plan.iter().any(|d| d.contains("idx_owed_rows_unjudged")),
        "{plan:?}"
    );
    assert!(!plan.iter().any(|d| d.starts_with("SCAN owed_rows") || d.starts_with("SCAN owed_dirty")), "{plan:?}");
}

fn open_deadline() -> low_app_layer::pot_hook::Deadline {
    low_app_layer::pot_hook::Deadline::new(
        low_app_layer::owed_backfill::OWED_BACKFILL_WALL_MS,
        low_app_layer::owed_backfill::OWED_BACKFILL_SAVE_RESERVE_MS,
        Box::new(|| 0),
        Box::new(|_| Box::pin(std::future::pending())),
    )
}

/// LOW-1: a judge that faults on the backfill's open identity for three passes is recorded, its marks left for the
/// drain, and the cursor moves past it, so one identity cannot hold the stamp (and every later identity) back.
#[tokio::test]
async fn d2fold_low1_a_wedged_identity_is_recorded_and_passed_after_three_passes() {
    use low_app_layer::owed_backfill::{self as backfill, Stop};
    let db = Db::new();
    seed_stranded_hop(&db, "a-wedged", "g00", &hop(1));
    seed_stranded_hop(&db, "b-honest", "g00", &hop(2));
    db.exec("DELETE FROM owed_dirty");
    db.exec(&format!("INSERT INTO ops_counters(name,value) VALUES ('overlay_migration_count',{})", backfill::OWED_TRIGGERS_MIGRATION_COUNT));
    db.exec("CREATE TRIGGER wedge BEFORE INSERT ON owed_rows WHEN NEW.identity='a-wedged' BEGIN SELECT RAISE(ABORT,'wedged'); END;");
    assert_eq!(backfill::backfill_pass(&db, &open_deadline()).await.unwrap().stop, Stop::Armed);
    for pass in 1..backfill::OWED_BACKFILL_IDENTITY_FAULTS_MAX {
        assert!(
            backfill::backfill_pass(&db, &open_deadline()).await.is_err(),
            "pass {pass}: the fault is the pass's"
        );
    }
    let report = backfill::backfill_pass(&db, &open_deadline()).await.unwrap();
    assert_eq!(report.stop, Stop::Finished, "{report:?}");
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_rows WHERE identity='b-honest'"),
        1,
        "the identities after it are backfilled"
    );
    assert!(
        db.int("SELECT COUNT(*) FROM owed_dirty WHERE identity='a-wedged'") > 0,
        "its marks stand for the drain"
    );
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_belt_state WHERE name='backfill:owed-v2:skipped:a-wedged'"),
        1,
        "recorded"
    );
    assert_eq!(db.counters.borrow()[backfill::COUNTER_SKIPPED], 1);
    assert_eq!(db.int("SELECT COUNT(*) FROM view_derivation_state WHERE name='owed-v2'"), 1);
}

#[allow(dead_code)]
#[path = "support/d1_fleet.rs"]
mod d1_fleet;

/// LOW-2, measured: on the lived-in seat (250 pots, 270 hops, 500 stored rows) one subject's commit reads one game's
/// rows and one state row. The base's per-game delete read the identity by its primary key and its state write
/// COUNTed every row; both are run here beside the shipped statements, on the same data, and printed.
#[tokio::test]
async fn d2fold_low2_one_subjects_commit_reads_its_game_not_the_identity() {
    let db = Db::new();
    {
        let conn = db.conn.borrow();
        conn.execute_batch("BEGIN").unwrap();
        for row in d1_fleet::owed_rows(NOW / 1000) {
            d1_fleet::execute_row(&conn, &row);
        }
        conn.execute_batch("COMMIT").unwrap();
    }
    let seat = d1_fleet::OWED_SEAT;
    let games: Vec<String> = {
        let conn = db.conn.borrow();
        let mut s = conn
            .prepare("SELECT DISTINCT gameId FROM hopparty_records WHERE identity=?1 ORDER BY gameId")
            .unwrap();
        s.query_map([seat], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
    };
    for chunk in games.chunks(16) {
        judge::judge_subjects(&db, seat, chunk, "measure").await.unwrap();
    }
    assert_eq!(db.int(&format!("SELECT COUNT(*) FROM owed_rows WHERE identity='{seat}'")), 500);
    assert_eq!(
        db.int(&format!("SELECT rows FROM owed_state WHERE identity='{seat}'")),
        500,
        "the kept count equals the rows"
    );
    db.steps.borrow_mut().clear();
    judge::judge_subjects(&db, seat, &games[..1], "measure").await.unwrap();
    let step = |needle: &str| -> i64 {
        db.steps
            .borrow()
            .iter()
            .filter(|(sql, _)| sql == needle)
            .map(|(_, n)| *n)
            .sum()
    };
    let now: i64 = [
        judge::GAME_DELETE_SQL,
        judge::STATE_CREATE_SQL,
        judge::CEILING_DELETE_SQL,
        judge::STATE_UPDATE_SQL,
    ]
    .iter()
    .map(|s| step(s))
    .sum();
    // the base's statements on the same 500 rows, run in a rolled-back transaction
    let base: i64 = {
        let conn = db.conn.borrow();
        conn.execute_batch("BEGIN").unwrap();
        let mut total = 0;
        for sql in [
            "DELETE FROM owed_rows INDEXED BY sqlite_autoindex_owed_rows_1 WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2))",
            "DELETE FROM owed_rows WHERE identity=?1 AND outpoint IN (SELECT outpoint FROM owed_rows WHERE identity=?1 ORDER BY CASE family WHEN 'payout' THEN 0 WHEN 'refund-due' THEN 1 WHEN 'hop-stranded' THEN 2 WHEN 'in-progress' THEN 3 ELSE 4 END,COALESCE(atHeight,0) DESC,outpoint LIMIT -1 OFFSET 500)",
            "INSERT INTO owed_state(identity,computedAtMs,tip,rows,stale,truncated,recomputeSeq) SELECT ?1,1,1,COUNT(*),0,0,1 FROM owed_rows WHERE identity=?1 ON CONFLICT(identity) DO UPDATE SET rows=excluded.rows",
        ] {
            let mut s = conn.prepare(sql).unwrap();
            let n = s.parameter_count();
            let binds: Vec<SqlValue> = [SqlValue::Text(seat.into()), SqlValue::Text(format!("[\"{}\"]", games[0]))]
                .into_iter()
                .take(n)
                .collect();
            s.execute(rusqlite::params_from_iter(binds)).unwrap();
            total += i64::from(s.get_status(rusqlite::StatementStatus::VmStep));
        }
        conn.execute_batch("ROLLBACK").unwrap();
        total
    };
    for (sql, n) in db.steps.borrow().iter() {
        eprintln!("  {n:>6} {}", &sql[..sql.len().min(90)]);
    }
    eprintln!("LOW-2: one subject's commit statements on a 500-row identity: {now} VM steps (the base's: {base})");
    assert!(now * 5 < base, "{now} against {base}");
    // an old worker's keyless rows (its column set; the walk that wrote them retired, bsv-low #893 section 6.2): the
    // row-count triggers keep the figure exact through them
    {
        let conn = db.conn.borrow();
        for n in 0..3 {
            conn.execute(
                "INSERT INTO owed_rows (identity, outpoint, family, gameId, facts, updatedAtMs) VALUES (?1, ?2, 'unbound', 'g', '{}', ?3)",
                rusqlite::params![seat, format!("{}:0", hop(30_000 + n)), NOW as i64 + 1],
            )
            .unwrap();
        }
    }
    assert_eq!(
        db.int(&format!("SELECT rows FROM owed_state WHERE identity='{seat}'")),
        db.int(&format!("SELECT COUNT(*) FROM owed_rows WHERE identity='{seat}'")),
        "the kept count through an old worker's write"
    );
    judge::judge_subjects(&db, seat, &games[..2], "measure").await.unwrap();
    assert_eq!(
        db.int(&format!("SELECT rows FROM owed_state WHERE identity='{seat}'")),
        db.int(&format!("SELECT COUNT(*) FROM owed_rows WHERE identity='{seat}'")),
        "and through the next judge"
    );
}
