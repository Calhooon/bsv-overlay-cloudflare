//! bsv-low #638: the read's inputs must already exist when the write lands.
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use rusqlite::Connection;

#[test]
fn hop_and_pot_writes_derive_links_and_seat_lookups_in_both_arrival_orders() {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db.execute_batch("INSERT INTO pot_records (txid,outputIndex,spent,spendingTxid) VALUES ('hop',0,1,'pot');
        INSERT INTO hopparty_records (identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('me','them','game',0,1,'03aa','00','00',2,'hop',1,1,1);
        INSERT INTO pot_records (txid,outputIndex,paramsDecoded,recoveryHeight,pubA,pubB) VALUES ('pot',0,1,99,'03aa','03bb');").unwrap();
    let count = |table: &str| {
        db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
    };
    assert_eq!(count("hop_pot_links"), 1);
    assert_eq!(count("pot_hop_seats"), 1);
    db.execute_batch("INSERT INTO potparty_records (identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex) VALUES ('me','them','game','pot',0,99,'00','party',1);").unwrap();
    assert_eq!(
        count("hop_pot_links"),
        0,
        "the stronger party row suppresses the derived candidate"
    );
    db.execute_batch("DELETE FROM potparty_records WHERE txid='party';")
        .unwrap();
    assert_eq!(
        count("hop_pot_links"),
        1,
        "losing the last party row restores the same evidence"
    );
    db.execute_batch("UPDATE pot_records SET spent=0,spendingTxid=NULL WHERE txid='hop';")
        .unwrap();
    assert_eq!(
        count("hop_pot_links"),
        0,
        "released hop spends reverse the link"
    );
    assert_eq!(
        count("pot_hop_seats"),
        1,
        "seat facts depend on committed keys, not the spend"
    );
    db.execute_batch("DELETE FROM hopparty_records WHERE txid='hop';")
        .unwrap();
    assert_eq!(count("pot_hop_seats"), 0);
    db.execute_batch("INSERT INTO hopparty_records (identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('me','them','game',0,1,'03aa','00','00',2,'hop',1,1,1);
        UPDATE pot_records SET spent=1,spendingTxid='pot' WHERE txid='hop';").unwrap();
    assert_eq!(count("hop_pot_links"), 1);
    assert_eq!(count("pot_hop_seats"), 1);
    db.execute_batch("DELETE FROM pot_records WHERE txid='pot';")
        .unwrap();
    assert_eq!((count("hop_pot_links"), count("pot_hop_seats")), (0, 0));
}

#[test]
fn a_cold_start_does_not_rewalk_the_fleet_after_the_backfill() {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"));
        }
    }
    for sql in [
        bsv_overlay_cloudflare::derived_hops::LINKS_BACKFILL,
        bsv_overlay_cloudflare::derived_hops::SEATS_BACKFILL,
    ] {
        let mut stmt = db.prepare(sql).unwrap();
        stmt.execute([]).unwrap();
        assert_eq!(stmt.get_status(rusqlite::StatementStatus::FullscanStep), 0);
    }
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
}

#[test]
fn a_changed_pot_drives_its_links_from_the_spender_index() {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"));
        }
    }
    let trigger = bsv_overlay_cloudflare::derived_hops::POT_INSERT;
    let start = trigger.find("FROM pot_records pot CROSS JOIN").unwrap();
    let end = trigger[start..].find(';').unwrap() + start;
    let query = format!(
        "EXPLAIN QUERY PLAN SELECT hp.rowid {}",
        trigger[start..end]
            .replace("NEW.txid", "'pot'")
            .replace("NEW.outputIndex", "0")
    );
    let mut stmt = db.prepare(&query).unwrap();
    let details = stmt
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    assert!(
        details
            .iter()
            .any(|s| s.contains("SEARCH hop USING INDEX idx_pot_spending_lower")),
        "{details:?}"
    );
    assert!(
        !details.iter().any(|s| s.starts_with("SCAN ")),
        "{details:?}"
    );
}
