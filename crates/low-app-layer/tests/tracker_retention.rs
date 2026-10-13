#[test]
fn ns3_l1_public_reads_create_zero_rows_and_projection_is_reaskable() {
    let manifest: serde_json::Value =
        serde_json::from_str(include_str!("../../../storage-ownership.json")).unwrap();
    let text = manifest.to_string();
    let row = manifest["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "tx_words")
        .unwrap_or_else(|| panic!("manifest shape: {text}"));
    assert_eq!(
        row["rebuild_class"], "re-askable",
        "tx_words is still permanent stranger-fillable storage"
    );
    assert_eq!(row["never_wipe"], false);
    let runtime = include_str!("../src/tracker_host/runtime.rs");
    assert!(runtime.contains("pub const PUBLIC_ROWS_PER_MINUTE: usize = 0;"));
    assert!(runtime.contains("stored::SWEEP"));
}
#[test]
fn ns3_l1_sweep_is_eight_days_bounded_and_spares_first_party() {
    use low_app_layer::tracker_host::stored;
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::CREATE).unwrap();
    db.execute_batch("CREATE TABLE transactions(txid TEXT PRIMARY KEY); CREATE TABLE pot_beefs(txid TEXT PRIMARY KEY)").unwrap();
    for n in 0..100 {
        db.execute(
            "INSERT INTO tx_words(txid,word,tier,updated_at) VALUES (?,'unknown','host',1)",
            [format!("{n:064x}")],
        )
        .unwrap();
    }
    db.execute("INSERT INTO pot_beefs VALUES (?)", [format!("{:064x}", 0)])
        .unwrap();
    db.execute(
        "UPDATE tx_words SET hint_source='arcade' WHERE txid=?",
        [format!("{:064x}", 1)],
    )
    .unwrap();
    db.execute(
        "UPDATE tx_words SET updated_at=700000 WHERE txid=?",
        [format!("{:064x}", 2)],
    )
    .unwrap();
    assert_eq!(db.execute(stored::SWEEP, [700001 - 8 * 86400]).unwrap(), 64);
    assert_eq!(db.execute(stored::SWEEP, [700001 - 8 * 86400]).unwrap(), 33);
    let remaining: i64 = db
        .query_row("SELECT count(*) FROM tx_words", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remaining, 3);
}
