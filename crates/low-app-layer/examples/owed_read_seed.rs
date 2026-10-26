//! Local D1 fixture for the three-statement read, independent of the retired walk.
#[allow(dead_code)]
#[path = "../tests/support/d1_fleet.rs"]
mod d1_fleet;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let now: u64 = std::env::var("OWED_SEED_TIME")
        .expect("date -u stamp")
        .parse()
        .unwrap();
    let db = rusqlite::Connection::open_in_memory().unwrap();
    for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(bsv_overlay_cloudflare::d1::migration_error_is_benign(
                sql,
                &e.to_string()
            ));
        }
    }
    let mut schema = db.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid").unwrap();
    for sql in schema.query_map([], |r| r.get::<_, String>(0)).unwrap() {
        println!("{};", sql.unwrap());
    }
    drop(schema);
    let id = d1_fleet::OWED_SEAT;
    // The full unrelated fleet is covered by d1_fleet_plans. This local D1
    // scenario needs the lived-in identity and its complete source history.
    let mut rows = d1_fleet::owed_rows(now);
    let source_rows = rows.len();
    let wanted = format!("02{}", "ab".repeat(32));
    rows.push(format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt) VALUES ('{wanted}','{wanted}','{}','{}',0,900100,'00','{}',1,{now})", "ac".repeat(32),"ad".repeat(32),"ad".repeat(32)));
    rows.push(format!("DELETE FROM owed_dirty WHERE identity='{wanted}'"));
    let mut source_sql = Vec::new();
    for (index, sql) in rows.into_iter().enumerate() {
        if index < source_rows {
            d1_fleet::execute_row(&db, &sql);
        } else {
            db.execute_batch(&sql).unwrap();
        }
        source_sql.push(sql);
    }
    for sql in source_sql {
        println!("{sql};");
    }
    let port = SeedDb {
        db: std::cell::RefCell::new(db),
        now: now * 1000,
        tip: Some(900_000),
        out: std::cell::RefCell::new(String::new()),
    };
    let games: Vec<_> = (0..270).map(|n| format!("a1{n:062x}")).collect();
    for games in games.chunks(16) {
        low_app_layer::owed_judge::judge_subjects(&port, id, games, "budget-fixture")
            .await
            .unwrap();
    }
    print!("{}", port.out.borrow());
    let counts: (u64, u64, u64, u64) = port.db.borrow().query_row(
        "SELECT (SELECT COUNT(*) FROM potparty_records WHERE identity=?1),(SELECT COUNT(*) FROM hopparty_records WHERE identity=?1),(SELECT COUNT(*) FROM owed_rows WHERE identity=?1),(SELECT COUNT(*) FROM owed_dirty WHERE identity=?1)",
        [id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
    ).unwrap();
    assert_eq!(counts, (250, 270, 500, 0));
    println!(
        "-- judged fixture: {} results, {} hops, {} rows, {} pending",
        counts.0, counts.1, counts.2, counts.3
    );
    // D2-OWED-FOLD-8: `OWED_SEED_PAIR13=1` (the backfill tier) adds the captured beta pair 13, an eviction made
    // before the eviction writer, and its synthetic legs (the budget seed's rows), for the evidence phase's cost.
    if std::env::var("OWED_SEED_PAIR13").is_ok_and(|v| v == "1") {
        for sql in l23_pair13::l23_r2_route_rows(now) {
            println!("{sql};");
        }
    }
}

#[path = "../tests/support/l23_pair13.rs"]
mod l23_pair13;

#[path = "../tests/support/seed_judge.rs"]
mod seed_judge;
use seed_judge::SeedDb;
