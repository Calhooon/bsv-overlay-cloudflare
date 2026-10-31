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
    "CREATE TABLE IF NOT EXISTS owed_dirty (generation INTEGER PRIMARY KEY AUTOINCREMENT, identity TEXT NOT NULL, gameId TEXT NOT NULL, source TEXT NOT NULL, sourceKey TEXT NOT NULL, markedAtMs INTEGER NOT NULL, downgrade INTEGER NOT NULL DEFAULT 0, UNIQUE (identity, gameId, source, sourceKey))",
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
fn owed893_judge_game_indexes_are_shipped_by_the_overlay() {
    let required = [
        "CREATE INDEX IF NOT EXISTS idx_potparty_game_identity ON potparty_records(gameId,identity)",
        "CREATE INDEX IF NOT EXISTS idx_evicted_party_identity_game ON potparty_records_evicted(identity,gameId,potTxid)",
    ];
    for sql in required {
        assert!(
            OVERLAY_MIGRATIONS.contains(&sql),
            "missing migration: {sql}"
        );
    }
}

fn fingerprint(list: &[&str]) -> u32 {
    list.iter().fold(0u32, |total, sql| {
        total.wrapping_add(sql.as_bytes().iter().fold(0x811c_9dc5u32, |hash, byte| {
            (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193)
        }))
    })
}

#[test]
fn owed893_migrations_preserve_the_shipped_prefix_and_pin_the_new_count() {
    assert_eq!(fingerprint(&OVERLAY_MIGRATIONS[..349]), 428_073_086);
    // The count and fingerprint the overlay certifies (moved by lane D2-OWED-FOLD's appended indexes, then by
    // D2-OWED-FOLD-3's in-place scope of `idx_owed_rows_sweep`, then by D2-OWED-FOLD-5's in-place `owed_dirty.downgrade`
    // and the triggers' classes, then the sweep index's key, then D2-OWED-FOLD-6's in-place classes of a pot's
    // admission and a refusing word, then D2-OWED-FOLD-8's appended missing-evidence table and its dirty trigger).
    // FIX-CADENCE-GAUGES appends 29 DROP/CREATE pairs and two covering indexes.
    assert_eq!(fingerprint(&OVERLAY_MIGRATIONS[..409]), 740_971_678);
    // bsv-low #911 (D2-ACTOR-A) appends four: the owed events' counter, their dead letters, the hop facts' index and
    // the cost bar's queue column; the 469 before them are untouched. The D2-ACTOR-A lens fold re-keyed the dead
    // letters by pair in place (MEDIUM-2, LOW-4; the four shipped nowhere): 1,223,941,910 then 751,808,187; the delta lens fold gave them
    // `redrivenAt` in place (LOW-C): 1,823,919,051.
    assert_eq!(fingerprint(&OVERLAY_MIGRATIONS[..469]), 889_593_182);
    // bsv-low #986 (FIX-986-COMPARE-2) appends one: `owed_actor_compare`; the 473 before it are untouched.
    assert_eq!(fingerprint(&OVERLAY_MIGRATIONS[..473]), 1_823_919_051);
    // bsv-low #986 (FIX-986, READ-986 item 2(b)) appends one: `idx_pot_spent_unclassified`, the params backfill's
    // second verdict candidate set; the 474 before it are untouched.
    assert_eq!(fingerprint(&OVERLAY_MIGRATIONS[..474]), 3_293_095_810);
    // bsv-low #1001 (FIX-1001) appends four: `idx_hopsweep_sweep` and three `owed_dirty` triggers (a transaction's
    // proof inserted and updated, a hop row's markerless filers); the 475 before them are untouched.
    assert_eq!(fingerprint(&OVERLAY_MIGRATIONS[..475]), 4_238_311_408);
    // bsv-low #1010 (FIX-1010) appends fourteen `owed_dirty` triggers (the actor's families written by statements no
    // trigger marked); the 479 before them are untouched.
    assert_eq!(fingerprint(&OVERLAY_MIGRATIONS[..479]), 3_349_521_383);
    const COUNT: usize = 493;
    assert_eq!(OVERLAY_MIGRATIONS.len(), COUNT);
    assert_eq!(d1::OVERLAY_MIGRATION_COUNT, COUNT);
    const FINGERPRINT: u32 = 2_350_949_467;
    assert_eq!(
        d1::migration_list_fingerprint(),
        FINGERPRINT,
        "the certified fingerprint"
    );
    assert_eq!(d1::migration_list_fingerprint(), FINGERPRINT);
    assert_eq!(fingerprint(OVERLAY_MIGRATIONS), FINGERPRINT);
    for missing in 349..OVERLAY_MIGRATIONS.len() {
        let mut without = OVERLAY_MIGRATIONS.to_vec();
        without.remove(missing);
        assert_ne!(
            fingerprint(&without),
            FINGERPRINT,
            "missing migration {} must not certify",
            missing + 1
        );
    }
    assert!(d1::migration_count_matches(COUNT as f64));
    assert!(!d1::migration_count_matches(COUNT as f64 - 1.0));
    assert!(!d1::migration_count_matches(396.0));
    assert!(!d1::migration_count_matches(395.0));
    assert!(!d1::migration_count_matches(391.0));
    assert!(!d1::migration_count_matches(349.0));
    assert!(!d1::migration_count_matches(393.0));
    assert_eq!(bsv_overlay_cloudflare::derived_owed::OWED_MAX_ROWS, 500);
    assert_eq!(
        bsv_overlay_cloudflare::derived_owed::OWED_MAX_ROWS,
        low_app_layer::owed::OWED_MAX_ROWS
    );
    let db = schema();
    db.execute(
        d1::MIGRATION_CERTIFY_SQL,
        rusqlite::params![395, FINGERPRINT],
    )
    .unwrap();
    for (name, expected) in [
        ("overlay_migration_count", 395.0),
        ("overlay_migration_fp", f64::from(FINGERPRINT)),
    ] {
        assert_eq!(
            db.query_row(
                "SELECT value FROM ops_counters WHERE name=?1",
                [name],
                |r| r.get::<_, f64>(0)
            )
            .unwrap(),
            expected
        );
    }
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
        // an old worker's row (its column set, before the #893 columns)
        "INSERT INTO owed_rows (identity, outpoint, family, gameId, sats, opponentIdentity, atHeight, facts, updatedAtMs, reason) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
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
    let old: (String, [Option<i64>; 5]) = db.query_row(
        "SELECT facts, judgedAtMs, claimable, gateHeight, beltDueAtMs, probeTip FROM owed_rows WHERE identity='owner' AND outpoint='pot:0'", [],
        |r| Ok((r.get(0)?, [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?]))).unwrap();
    assert_eq!(old, ("{\"claimable\":true}".into(), [None; 5]));
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
fn owed893_a_row_delete_clears_its_words_in_the_same_statement() {
    let db = schema();
    for id in ["owner", "bystander"] {
        db.execute(
            "INSERT INTO owed_rows (identity, outpoint, family, gameId, sats, opponentIdentity, atHeight, facts, updatedAtMs, reason) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![id, "pot:0", "payout", "game", 100, "other", 20, "{}", 1_000, "paid"],
        )
        .unwrap();
        db.execute("INSERT INTO owed_row_words (identity,outpoint,role,txid) VALUES (?1,'pot:0','funding','pot')", [id]).unwrap();
    }
    db.execute_batch("CREATE TEMP TRIGGER fail_word_delete AFTER DELETE ON owed_row_words BEGIN SELECT RAISE(ABORT,'word delete failed'); END;").unwrap();
    assert!(db
        .execute(
            "DELETE FROM owed_rows WHERE identity = ?1",
            rusqlite::params!["owner"]
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
        "DELETE FROM owed_rows WHERE identity = ?1",
        rusqlite::params!["owner"],
    )
    .unwrap();
    assert_eq!(
        db.query_row("SELECT identity FROM owed_row_words", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "bystander"
    );
}
