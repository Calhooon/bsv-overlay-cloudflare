// The seed generators' judge port (shared by examples/owed_read_seed.rs and examples/d1_budget_seed.rs).
/// A judge port over a native SQLite connection that RECORDS each write statement it runs, its binds expanded as SQL
/// literals, into `out`, so a seed generator replays the shipped judge's writes on the local D1 (the owed read tier's
/// lived-in seat; the budget tier's identity C, D2-OWED-FOLD-8).
pub struct SeedDb {
    pub db: std::cell::RefCell<rusqlite::Connection>,
    pub now: u64,
    pub tip: Option<u64>,
    pub out: std::cell::RefCell<String>,
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
                // wrangler 4.54's `--file` splitter opens a frame at a whitespace-`CASE` and closes it only at an `END`
                // followed by whitespace or `;`: the judge's ceiling DELETE's `END,COALESCE` left it open, so the
                // statements after it merged into one past D1's 100 KB statement limit ("statement too long:
                // SQLITE_TOOBIG", measured, D2-OWED-FOLD-8). The replayed text spaces it; SQLite reads it the same.
                let expanded = expanded.replace(" END,", " END ,");
                self.out.borrow_mut().push_str(&format!("{expanded};\n"));
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
        self.tip
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
