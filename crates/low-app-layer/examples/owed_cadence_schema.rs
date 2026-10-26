//! Minimal workerd cadence fixture: the production schema, no unrelated fleet history.
fn main() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
        if let Err(error) = db.execute_batch(sql) {
            assert!(bsv_overlay_cloudflare::d1::migration_error_is_benign(sql, &error.to_string()));
        }
    }
    let mut query = db.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid").unwrap();
    let schema: Vec<String> = query.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    println!("{}", serde_json::to_string(&schema).unwrap());
}
