//! D2-OWED-FOLD-3: the tip's sweep date reads only the rows it writes. The shipped tip handler over the overlay's
//! migrations on real SQLite, its statements' VM steps read as `write_derived_reads.rs` reads them.
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;
use low_app_layer::owed_judge as judge;
use owed_db::*;

/// Copy `from`'s stored row `n` times under fresh identities and outpoints, every other column as judged.
fn clone_row(db: &Db, from: &str, prefix: &str, n: usize, start: usize) {
    let cols: Vec<String> = {
        let conn = db.conn.borrow();
        let mut s = conn
            .prepare("SELECT name FROM pragma_table_info('owed_rows')")
            .unwrap();
        s.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    let list = cols.join(",");
    for k in 0..n {
        let picked: Vec<String> = cols
            .iter()
            .map(|c| match c.as_str() {
                "identity" => format!("'{prefix}{k:04}'"),
                "outpoint" => format!("'{}:0'", hop(start + k)),
                "beltDueAtMs" => "NULL".into(),
                other => other.into(),
            })
            .collect();
        db.exec(&format!(
            "INSERT INTO owed_rows({list}) SELECT {} FROM owed_rows WHERE outpoint='{from}'",
            picked.join(",")
        ));
    }
}

/// The fleet at a block: 40 courier-dependent claimable sweeps and `unspent` index-unspent claimable sweeps, all
/// judged at 900,000; the tip handler at 900,001. Answers the date statement's VM steps and the rows it dated.
async fn date_sweeps_at(unspent: usize) -> (i64, i64) {
    let db = Db::new();
    seed_stranded_hop(&db, "owner", "g00", &hop(0));
    seed_courier_unspent_hop(&db, "courier", "c00", &hop(1), 900_000);
    drain_all(&db).await;
    let honest = format!("{}:0", hop(0));
    let leaning = format!("{}:0", hop(1));
    assert_eq!(
        db.int(&format!(
            "SELECT claimable FROM owed_rows WHERE outpoint='{honest}'"
        )),
        1
    );
    assert_eq!(
        db.int(&format!(
            "SELECT claimable FROM owed_rows WHERE outpoint='{leaning}'"
        )),
        1
    );
    db.exec(&format!(
        "UPDATE owed_rows SET beltDueAtMs=NULL WHERE outpoint='{leaning}'"
    ));
    clone_row(&db, &leaning, "courier", 39, 10_000);
    clone_row(&db, &honest, "stranger", unspent - 1, 100_000);
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_rows WHERE family='hop-stranded' AND claimable=1 AND probeTip<900001"),
        40 + unspent as i64
    );
    db.now.set(db.now.get() + 1_000);
    db.tip.set(Some(900_001));
    db.steps.borrow_mut().clear();
    judge::mark_tip(&db, 900_001).await.unwrap();
    let now = db.now.get() as i64;
    let steps = db
        .steps
        .borrow()
        .iter()
        .find(|(sql, _)| sql == judge::DATE_SWEEPS_SQL)
        .map(|(_, n)| *n)
        .expect("the date statement ran");
    let dated = db.int(&format!(
        "SELECT COUNT(*) FROM owed_rows WHERE beltDueAtMs={now}"
    ));
    (steps, dated)
}

/// The ruling: a write statement reads the rows it writes and nothing a stranger grows. The date statement's steps
/// at 1,000 index-unspent claimable sweeps equal its steps at 10, within the 40 dated rows' own share. RED on
/// 8396f8b2, where `idx_owed_rows_sweep` holds every claimable sweep and the statement filters the index-unspent
/// ones out one by one, so its steps grow with N.
#[tokio::test]
async fn d2fold3_the_tip_sweep_date_reads_only_the_rows_it_writes() {
    let (few, dated_few) = date_sweeps_at(10).await;
    let (many, dated_many) = date_sweeps_at(1_000).await;
    eprintln!("DATE_SWEEPS_SQL: {few} VM steps at N=10, {many} at N=1000; dated {dated_few} and {dated_many}");
    assert_eq!(
        (dated_few, dated_many),
        (40, 40),
        "the 40 courier-dependent sweeps dated, no index-unspent one"
    );
    let share = few / 40;
    assert!(
        (many - few).abs() <= share,
        "the statement's reads grow with the index-unspent sweeps: {few} at N=10, {many} at N=1000 (one row's share {share})"
    );
}
