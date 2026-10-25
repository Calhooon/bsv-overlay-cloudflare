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

/// HIGH-1: the #669 freshness gate at the read. A sweep the judge found claimable at tip T is served claimable at T,
/// degraded to a pending chain-wait at T+1, and withheld when the present tip cannot be read.
#[tokio::test]
async fn d2fold_high1_a_stored_sweep_degrades_at_the_read_across_a_block() {
    let db = Db::new();
    seed_stranded_hop(&db, "owner", "g00", &hop(0));
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

/// HIGH-1: on a tip the claimable sweeps judged below it are dated due now (the design's tip row), over a partial
/// index, so the belt re-probes them first; a sweep judged at the new tip, and every other row, keep their date.
#[tokio::test]
async fn d2fold_high1_b_a_tip_dates_the_claimable_sweeps_due_now() {
    let db = Db::new();
    seed_stranded_hop(&db, "owner", "g00", &hop(0));
    seed_stranded_hop(&db, "owner", "g01", &hop(1));
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
            binds: vec![Bind::Int(tip as i64), Bind::Int(1)],
        }])
        .await
        .unwrap();
    }
}
