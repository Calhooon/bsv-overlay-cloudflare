//! The D2-OWED third delta lens's folds (bsv-low `docs/audit/D2-OWED-DELTA-3-2026-10-26.md`), each on the producer
//! path: the shipped writers, judge, drain, read and belt over the overlay's migrations on real SQLite.
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

/// Every hop unspent at every rung; counts every call.
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

/// An index-unspent hop whose verified marker is `age_ms` old: under 30 minutes the judge holds it `in-progress`,
/// dated for its look at the marker + 30 min.
fn seed_young_hop(db: &Db, identity: &str, game: &str, hop_txid: &str, age_ms: i64) {
    let made = (db.now.get() as i64 - age_ms) / 1000;
    db.exec(&format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{hop_txid}',0,0,{made},'p2pkh'); INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('{identity}','other','{game}',0,20190,'seat','00','00',2,'{hop_txid}',1,{made},1);"));
}

/// D3-M1 (the ruling: no class owns the whole tick). Strangers plant 96 courier-dependent rows over 24 identities
/// (each sorting before the honest one), every one dated due by each block; the honest identity holds one young
/// index-unspent hop whose look, due now, is what turns it into a press. The first tick after the block gives the
/// second class its reserved places: the honest hop is looked at and served as a press; the first class keeps the
/// rest of the tick (7 of 8: the second class's other reserved place yields). RED on e53c1406: the first class took all 8 places while it held 40 or more due rows, so
/// the honest look waited ceil(96 / 8) = 12 ticks.
#[tokio::test]
async fn d3m1_a_a_flood_of_block_dated_rows_never_starves_the_second_class() {
    const C: usize = 96;
    let db = Db::new();
    for n in 0..C {
        seed_courier_unspent_hop(
            &db,
            &format!("a{:02}", n % 24),
            &format!("s{n}"),
            &hop(1_000 + n),
            900_000,
        );
    }
    seed_young_hop(&db, "zz-honest", "h00", &hop(0), 25 * 60_000);
    drain_all(&db).await;
    let honest = format!("{}:0", hop(0));
    let body = served(&db, "zz-honest", Some(900_000)).await;
    assert_eq!(
        row_of(&body, &honest).unwrap()["family"],
        "in-progress",
        "{body}"
    );

    // six minutes later the honest look is due; a block dates every stranger row due now
    db.now.set(db.now.get() + 6 * 60_000);
    db.tip.set(Some(900_001));
    judge::mark_tip(&db, 900_001).await.unwrap();
    assert_eq!(
        db.int(&format!(
            "SELECT COUNT(*) FROM owed_rows WHERE identity LIKE 'a%' AND beltDueAtMs<={}",
            db.now.get()
        )),
        C as i64
    );

    let fake = Fake::default();
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        report.candidates,
        belt::OWED_BELT_OUTPOINTS_PER_TICK,
        "{report:?}"
    );
    assert!(
        fake.calls.borrow().iter().any(|u| u.contains(&hop(0))),
        "the second class's reserved places reach the honest look on the first tick: {report:?} {:?}",
        fake.calls.borrow()
    );
    let strangers = (0..C)
        .filter(|n| {
            fake.calls
                .borrow()
                .iter()
                .any(|u| u.contains(&hop(1_000 + n)))
        })
        .count();
    assert_eq!(
        strangers,
        belt::OWED_BELT_OUTPOINTS_PER_TICK - 1,
        "the first class keeps the rest of the tick (the second class holds one due row; its other place yields)"
    );
    let body = served(&db, "zz-honest", Some(900_001)).await;
    let row = row_of(&body, &honest).unwrap();
    assert_eq!(
        row["family"], "hop-stranded",
        "the look created the press: {row}"
    );
    assert_eq!(row["facts"]["claimable"], true, "{row}");
}

/// D3-M1, the yield: a class with nothing due yields its places. With no second-class row due the first class takes
/// all 8; with no first-class row due the second class takes all 8.
#[tokio::test]
async fn d3m1_b_a_class_with_nothing_due_yields_its_places() {
    assert_eq!(belt::class_places(8, 8), (6, 2));
    assert_eq!(belt::class_places(8, 1), (7, 1));
    assert_eq!(belt::class_places(3, 8), (3, 5));
    assert_eq!(belt::class_places(8, 0), (8, 0));
    assert_eq!(belt::class_places(0, 8), (0, 8));
    let db = Db::new();
    for n in 0..20 {
        seed_courier_unspent_hop(
            &db,
            &format!("a{n:02}"),
            &format!("s{n}"),
            &hop(1_000 + n),
            900_000,
        );
    }
    drain_all(&db).await;
    db.now.set(db.now.get() + 1_000);
    db.tip.set(Some(900_001));
    judge::mark_tip(&db, 900_001).await.unwrap();
    let fake = Fake::default();
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        report.candidates,
        belt::OWED_BELT_OUTPOINTS_PER_TICK,
        "first class alone: {report:?}"
    );

    let db = Db::new();
    for n in 0..20 {
        seed_stranded_hop(&db, &format!("s{n:02}"), &format!("s{n}"), &hop(1_000 + n));
    }
    drain_all(&db).await;
    db.now.set(db.now.get() + HOUR as u64);
    let fake = Fake::default();
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        report.candidates,
        belt::OWED_BELT_OUTPOINTS_PER_TICK,
        "second class alone: {report:?}"
    );
}
