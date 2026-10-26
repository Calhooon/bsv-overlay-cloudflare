//! The D2-OWED second delta lens's folds (bsv-low `docs/audit/D2-OWED-DELTA-2-2026-10-26.md`, the captain's ruling in
//! `docs/lanes/D2-OWED-FOLD-4-2026-10-26.md`), each on the producer path: the shipped writers, judge, drain, read and
//! belt over the overlay's migrations on real SQLite.
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;
use low_app_layer::owed_belt::{self as belt, Courier};
use low_app_layer::owed_judge as judge;
use owed_db::*;
use serde_json::Value;
use std::cell::RefCell;

fn row_of<'a>(body: &'a Value, outpoint: &str) -> Option<&'a Value> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == outpoint)
}

/// Every hop unspent at every rung; counts every call (a refused ask never reaches it).
#[derive(Default)]
struct Fake {
    calls: RefCell<Vec<String>>,
}
impl Courier for Fake {
    async fn get(&self, url: String) -> Option<(u16, Vec<u8>)> {
        self.calls.borrow_mut().push(url.clone());
        if url.ends_with("/spend") {
            return Some((200, br#"{"spent":false}"#.to_vec()));
        }
        Some((404, b"{}".to_vec()))
    }
}

/// D2-M1 (the captain's ruling: a look a PRESS WAITS ON comes before a look that can only downgrade). S = 640
/// stranger identities, each holding one index-UNSPENT stranded hop freshly looked at (dated, not due), walked BEFORE
/// the honest identity (the cursor at the start); the honest identity holds one courier-only sweep (index status
/// unknown, a courier said unspent) that a block dated due now and the read withholds until its look. The first tick
/// after the block looks at the honest hop. RED on 6ec6eb9a: the walk took 64 dated identities a tick, none due, so the
/// honest identity was not reached on the first tick (the lens's probe: tick 46).
#[tokio::test]
async fn d2delta2_m1_a_courier_only_sweep_a_block_dated_is_looked_at_first_whatever_the_strangers_dated_rows(
) {
    const S: usize = 640;
    let db = Db::new();
    for n in 0..S {
        seed_stranded_hop(&db, &format!("s{n:04}"), &format!("s{n}"), &hop(1_000 + n));
    }
    seed_courier_unspent_hop(&db, "zz-honest", "h00", &hop(0), 900_000);
    drain_all(&db).await;
    let honest = format!("{}:0", hop(0));
    // the strangers' rows were looked at a moment ago and backed off: dated, not due
    let later = db.now.get() as i64 + 6 * HOUR;
    db.exec(&format!(
        "UPDATE owed_rows SET beltDueAtMs={later} WHERE identity LIKE 's%'"
    ));
    let row = served(&db, "zz-honest", Some(900_000)).await;
    assert_eq!(
        row_of(&row, &honest).unwrap()["facts"]["claimable"],
        true,
        "{row}"
    );

    // a block: the tip handler dates the courier-only sweep due now; the read withholds its press until its look
    db.now.set(db.now.get() + 1_000);
    db.tip.set(Some(900_001));
    judge::mark_tip(&db, 900_001).await.unwrap();
    let withheld = served(&db, "zz-honest", Some(900_001)).await;
    let row = row_of(&withheld, &honest).unwrap();
    assert_eq!(row["facts"]["claimable"], false, "{row}");
    assert_eq!(row["facts"]["chainWait"], "block", "{row}");

    let fake = Fake::default();
    let report = belt::tick(&db, &fake).await.unwrap();
    assert!(
        fake.calls.borrow().iter().any(|u| u.contains(&hop(0))),
        "the honest courier-only sweep is looked at on the first tick: {report:?} {:?}",
        fake.calls.borrow()
    );
    let looked = served(&db, "zz-honest", Some(900_001)).await;
    let row = row_of(&looked, &honest).unwrap();
    assert_eq!(row["facts"]["claimable"], true, "after its look: {row}");
}

/// D2-M1 (b): inside the due set, the courier-only sweeps a block dated come before the back-off re-looks of
/// index-unspent rows, and each class is round-robin by identity: 12 strangers' index-unspent rows due an hour ago (the
/// oldest due) and two honest identities' courier-only sweeps dated by the block take the first places in the tick.
/// RED on 6ec6eb9a: the walk served the strangers' older rows first.
#[tokio::test]
async fn d2delta2_m1_b_the_block_dated_class_precedes_the_backoff_relooks() {
    let db = Db::new();
    for n in 0..12 {
        seed_stranded_hop(&db, &format!("s{n:02}"), &format!("s{n}"), &hop(1_000 + n));
    }
    seed_courier_unspent_hop(&db, "zz-a", "a00", &hop(0), 900_000);
    seed_courier_unspent_hop(&db, "zz-b", "b00", &hop(1), 900_000);
    seed_courier_unspent_hop(&db, "zz-b", "b01", &hop(2), 900_000);
    drain_all(&db).await;
    let an_hour_ago = db.now.get() as i64 - HOUR;
    db.exec(&format!(
        "UPDATE owed_rows SET beltDueAtMs={an_hour_ago} WHERE identity LIKE 's%'"
    ));
    db.now.set(db.now.get() + 1_000);
    db.tip.set(Some(900_001));
    judge::mark_tip(&db, 900_001).await.unwrap();
    let fake = Fake::default();
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        report.candidates,
        belt::OWED_BELT_OUTPOINTS_PER_TICK,
        "{report:?}"
    );
    let asked = |n: usize| fake.calls.borrow().iter().any(|u| u.contains(&hop(n)));
    assert!(
        asked(0) && asked(1) && asked(2),
        "the block-dated sweeps first: {:?}",
        fake.calls.borrow()
    );
    let strangers = (0..12).filter(|n| asked(1_000 + n)).count();
    assert_eq!(
        strangers, 5,
        "the rest of the tick to the back-off re-looks"
    );
}
/// D2-L1 (the captain's ruling: a read never does work, but never serves a press over a pending downgrade). The index
/// records a COMPETING spend of the owner's index-unspent hop (`pot_records.spent = 1`, the trigger's `pot` mark) while
/// the game's mark waits for a drain: the read serves the row with its press withheld as re-judging (`refresh:
/// "pending"`, the mark's source stated); the drain then downgrades it. An eviction's mark withholds the same way; a
/// filing's, a tip's or a wanted mark does not. RED on 6ec6eb9a: the stale press was served (`claimable: true`).
#[tokio::test]
async fn d2delta2_l1_a_pending_downgrade_withholds_the_press_until_the_drain() {
    let db = Db::new();
    seed_stranded_hop(&db, "owner", "g00", &hop(0));
    seed_stranded_hop(&db, "other", "g01", &hop(1));
    drain_all(&db).await;
    let op = format!("{}:0", hop(0));
    let body = served(&db, "owner", Some(900_000)).await;
    assert_eq!(
        row_of(&body, &op).unwrap()["facts"]["claimable"],
        true,
        "{body}"
    );

    // a filing's, a tip's and a wanted mark withhold nothing
    for (source, key) in [("sweep", "aa"), ("tip", "gate"), ("wanted", "wanted")] {
        db.exec(&format!("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner','g00','{source}','{key}',{})", db.now.get()));
        let body = served(&db, "owner", Some(900_000)).await;
        assert_eq!(
            row_of(&body, &op).unwrap()["facts"]["claimable"],
            true,
            "{source}: {body}"
        );
        db.exec("DELETE FROM owed_dirty");
    }

    // the competitor recorded by the index; the mark waits
    let rival = "cc".repeat(32);
    db.exec(&format!(
        "UPDATE pot_records SET spent=1,spendingTxid='{rival}' WHERE txid='{}' AND outputIndex=0",
        hop(0)
    ));
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty WHERE identity='owner' AND source='pot'"),
        1
    );
    let body = served(&db, "owner", Some(900_000)).await;
    let row = row_of(&body, &op).unwrap();
    assert_eq!(
        row["facts"]["claimable"], false,
        "no press over a pending downgrade: {row}"
    );
    assert_eq!(row["facts"]["refresh"], "pending", "{row}");
    assert_eq!(row["facts"]["pendingSource"], "pot", "{row}");
    assert_eq!(row["facts"]["claimableBeforePending"], true, "{row}");
    // across a block too
    let row = row_of(&served(&db, "owner", Some(900_001)).await, &op)
        .unwrap()
        .clone();
    assert_eq!(row["facts"]["claimable"], false, "{row}");
    // another identity's game is untouched
    let other = format!("{}:0", hop(1));
    let body = served(&db, "other", Some(900_000)).await;
    assert_eq!(
        row_of(&body, &other).unwrap()["facts"]["claimable"],
        true,
        "{body}"
    );

    // the drain lifts it: the row is downgraded
    drain_all(&db).await;
    let body = served(&db, "owner", Some(900_000)).await;
    if let Some(row) = row_of(&body, &op) {
        assert_ne!(row["facts"]["claimable"], true, "{row}");
        assert!(row["facts"].get("refresh").is_none(), "{row}");
    }

    // an eviction's mark withholds the same way
    seed_stranded_hop(&db, "owner", "g02", &hop(2));
    drain_all(&db).await;
    let op2 = format!("{}:0", hop(2));
    db.exec(&format!("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner','g02','eviction','{}',1)", "dd".repeat(32)));
    let body = served(&db, "owner", Some(900_000)).await;
    let row = row_of(&body, &op2).unwrap();
    assert_eq!(row["facts"]["claimable"], false, "{row}");
    assert_eq!(row["facts"]["pendingSource"], "eviction", "{row}");
}
