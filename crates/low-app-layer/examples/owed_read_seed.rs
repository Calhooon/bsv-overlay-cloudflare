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
    };
    let games: Vec<_> = (0..270).map(|n| format!("a1{n:062x}")).collect();
    for games in games.chunks(16) {
        low_app_layer::owed_judge::judge_subjects(&port, id, games, "budget-fixture")
            .await
            .unwrap();
    }
    let counts: (u64, u64, u64, u64) = port.db.borrow().query_row(
        "SELECT (SELECT COUNT(*) FROM potparty_records WHERE identity=?1),(SELECT COUNT(*) FROM hopparty_records WHERE identity=?1),(SELECT COUNT(*) FROM owed_rows WHERE identity=?1),(SELECT COUNT(*) FROM owed_dirty WHERE identity=?1)",
        [id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
    ).unwrap();
    assert_eq!(counts, (250, 270, 500, 0));
    println!(
        "-- judged fixture: {} results, {} hops, {} rows, {} pending",
        counts.0, counts.1, counts.2, counts.3
    );
}

struct SeedDb {
    db: std::cell::RefCell<rusqlite::Connection>,
    now: u64,
}
impl low_app_layer::pot_hook::SqlPort for SeedDb {
    async fn run_batch(
        &self,
        statements: Vec<low_app_layer::pot_hook::Stmt>,
    ) -> Result<Vec<low_app_layer::pot_hook::StmtOut>, String> {
        use low_app_layer::pot_hook::Bind;
        use rusqlite::types::{Value, ValueRef};
        let mut db = self.db.borrow_mut();
        let tx = db.transaction().map_err(|e| e.to_string())?;
        let mut results = Vec::new();
        for statement in statements {
            let mut query = tx
                .prepare_cached(&statement.sql)
                .map_err(|e| e.to_string())?;
            let readonly = query.readonly();
            let names: Vec<_> = query
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            let values = statement.binds.iter().map(|bind| match bind {
                Bind::Text(s) => Value::Text(s.clone()),
                Bind::Int(n) => Value::Integer(*n),
                Bind::Null => Value::Null,
            });
            let rows = query
                .query_map(rusqlite::params_from_iter(values), |row| {
                    let mut object = serde_json::Map::new();
                    for (index, name) in names.iter().enumerate() {
                        let value = match row.get_ref(index)? {
                            ValueRef::Null => serde_json::Value::Null,
                            ValueRef::Integer(n) => serde_json::json!(n),
                            ValueRef::Real(n) => serde_json::json!(n),
                            ValueRef::Text(s) => serde_json::json!(std::str::from_utf8(s).unwrap()),
                            ValueRef::Blob(b) => serde_json::json!(hex::encode(b)),
                        };
                        object.insert(name.clone(), value);
                    }
                    Ok(serde_json::Value::Object(object))
                })
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            if !readonly {
                let literals: Vec<_> = statement
                    .binds
                    .iter()
                    .map(|bind| match bind {
                        Bind::Text(s) => format!("'{}'", s.replace('\'', "''")),
                        Bind::Int(n) => n.to_string(),
                        Bind::Null => "NULL".to_string(),
                    })
                    .collect();
                let mut expanded = String::new();
                let mut chars = statement.sql.chars().peekable();
                while let Some(ch) = chars.next() {
                    if ch == '?' {
                        let mut number = String::new();
                        while chars.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                            number.push(chars.next().unwrap());
                        }
                        expanded.push_str(&literals[number.parse::<usize>().unwrap() - 1]);
                    } else {
                        expanded.push(ch);
                    }
                }
                println!("{expanded};");
            }
            results.push(low_app_layer::pot_hook::StmtOut {
                rows,
                changes: Some(tx.changes()),
            });
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(results)
    }
    async fn counters(&self, _: Vec<(String, u64)>) {}
    fn now_ms(&self) -> u64 {
        self.now
    }
    fn log(&self, _: &str) {}
}
impl low_app_layer::owed_judge::JudgeDb for SeedDb {
    fn tip(&self) -> Option<u64> {
        Some(900_000)
    }
    async fn word_views(
        &self,
        rows: &std::collections::BTreeMap<String, low_app_layer::tracker_host::stored::Row>,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>, String> {
        Ok(rows
            .iter()
            .map(|(id, row)| (id.clone(), row.view()))
            .collect())
    }
    async fn canonical_root(&self, _: u64) -> Option<String> {
        None
    }
    async fn push(&self, _: &str, _: serde_json::Value) {}
}
