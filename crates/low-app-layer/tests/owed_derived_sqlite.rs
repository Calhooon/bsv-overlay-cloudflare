//! #893: the owed write's additive schema and source-statement dirty marks.
use bsv_overlay_cloudflare::d1::{self, OVERLAY_MIGRATIONS};
use rusqlite::Connection;

const ALTERS: [&str; 5] = [
    "ALTER TABLE owed_rows ADD COLUMN judgedAtMs INTEGER",
    "ALTER TABLE owed_rows ADD COLUMN claimable INTEGER",
    "ALTER TABLE owed_rows ADD COLUMN gateHeight INTEGER",
    "ALTER TABLE owed_rows ADD COLUMN beltDueAtMs INTEGER",
    "ALTER TABLE owed_rows ADD COLUMN probeTip INTEGER",
];
const TABLES: [&str; 4] = [
    "CREATE TABLE IF NOT EXISTS owed_row_words (identity TEXT, outpoint TEXT, role TEXT, txid TEXT, PRIMARY KEY (identity, outpoint, role))",
    "CREATE TABLE IF NOT EXISTS owed_dirty (identity TEXT NOT NULL, gameId TEXT NOT NULL, source TEXT NOT NULL, sourceKey TEXT NOT NULL, markedAtMs INTEGER NOT NULL, UNIQUE (identity, gameId, source, sourceKey))",
    "CREATE TABLE IF NOT EXISTS released_hop_evidence (hopTxid TEXT NOT NULL, hopVout INTEGER NOT NULL, identity TEXT NOT NULL, gameId TEXT NOT NULL, potTxid TEXT NOT NULL, seatSettlePubkey TEXT NOT NULL, hopSats INTEGER NOT NULL, evictedAt INTEGER NOT NULL, PRIMARY KEY (hopTxid, hopVout, identity))",
    "CREATE TABLE IF NOT EXISTS owed_belt_state (name TEXT PRIMARY KEY, cursor TEXT, tickedAtMs INTEGER)",
];
const INDEXES: [&str; 6] = [
    "CREATE INDEX IF NOT EXISTS idx_owed_row_words_txid ON owed_row_words(txid, identity)",
    "CREATE INDEX IF NOT EXISTS idx_owed_dirty_identity ON owed_dirty(identity)",
    "CREATE INDEX IF NOT EXISTS idx_owed_dirty_marked ON owed_dirty(markedAtMs)",
    "CREATE INDEX IF NOT EXISTS idx_released_hop_evidence_identity_game ON released_hop_evidence(identity, gameId)",
    "CREATE INDEX IF NOT EXISTS idx_owed_rows_gate ON owed_rows(gateHeight) WHERE gateHeight IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_owed_rows_belt ON owed_rows(beltDueAtMs) WHERE beltDueAtMs IS NOT NULL",
];

fn migrate(db: &Connection, statements: &[&str]) {
    for sql in statements {
        if let Err(e) = db.execute_batch(sql) {
            assert!(
                d1::migration_error_is_benign(sql, &e.to_string()),
                "{e}: {sql}"
            );
        }
    }
}

fn schema() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    migrate(&db, OVERLAY_MIGRATIONS);
    db
}

#[test]
fn owed893_schema_is_additive_replayable_and_keeps_the_old_walks_rows() {
    println!(
        "migration count {}, fingerprint {}",
        OVERLAY_MIGRATIONS.len(),
        d1::migration_list_fingerprint()
    );
    for sql in ALTERS.into_iter().chain(TABLES).chain(INDEXES) {
        assert!(
            OVERLAY_MIGRATIONS[349..].contains(&sql),
            "missing additive migration: {sql}"
        );
    }
    let db = schema();
    db.execute(
        low_app_layer::owed::OWED_ROW_INSERT_SQL,
        rusqlite::params![
            "owner",
            "pot:0",
            "in-progress",
            "game",
            100,
            "other",
            20,
            "{\"claimable\":true}",
            1_000,
            "waiting"
        ],
    )
    .unwrap();
    migrate(&db, OVERLAY_MIGRATIONS);
    let old: (String, Option<i64>, Option<i64>, Option<i64>, Option<i64>, Option<i64>) = db.query_row(
        "SELECT facts, judgedAtMs, claimable, gateHeight, beltDueAtMs, probeTip FROM owed_rows WHERE identity='owner' AND outpoint='pot:0'", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?))).unwrap();
    assert_eq!(
        old,
        ("{\"claimable\":true}".into(), None, None, None, None, None)
    );
    let backfill: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM view_derivation_state WHERE name='owed-v2'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(backfill, 0, "only the scheduled backfill may stamp owed-v2");
}

#[test]
fn owed893_app_layer_schema_text_is_byte_identical_to_the_overlay() {
    for sql in ALTERS {
        assert!(
            low_app_layer::schema::LATCH_COLUMN_ALTERS.contains(&sql),
            "missing app-layer column: {sql}"
        );
    }
    for sql in TABLES {
        assert!(
            low_app_layer::schema::CREATE_TABLE_CATCHUPS.contains(&sql),
            "missing app-layer table: {sql}"
        );
    }
}

#[test]
fn owed893_old_walk_delete_clears_words_in_the_same_statement() {
    let db = schema();
    for id in ["owner", "bystander"] {
        db.execute(
            low_app_layer::owed::OWED_ROW_INSERT_SQL,
            rusqlite::params![id, "pot:0", "payout", "game", 100, "other", 20, "{}", 1_000, "paid"],
        )
        .unwrap();
        db.execute("INSERT INTO owed_row_words (identity,outpoint,role,txid) VALUES (?1,'pot:0','funding','pot')", [id]).unwrap();
    }
    db.execute_batch("CREATE TEMP TRIGGER fail_word_delete AFTER DELETE ON owed_row_words BEGIN SELECT RAISE(ABORT,'word delete failed'); END;").unwrap();
    assert!(db
        .execute(
            low_app_layer::owed::OWED_ROWS_DELETE_SQL,
            rusqlite::params!["owner", 1_000]
        )
        .is_err());
    for table in ["owed_rows", "owed_row_words"] {
        assert_eq!(
            db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
    db.execute_batch("DROP TRIGGER fail_word_delete").unwrap();
    db.execute(
        low_app_layer::owed::OWED_ROWS_DELETE_SQL,
        rusqlite::params!["owner", 1_000],
    )
    .unwrap();
    assert_eq!(
        db.query_row("SELECT identity FROM owed_row_words", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "bystander"
    );
}
