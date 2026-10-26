//! The D2-OWED delta lens's folds (bsv-low `docs/audit/D2-OWED-DELTA-2026-10-25.md`), each on the producer path: the
//! shipped writers, judge, drain, read and belt over the overlay's migrations on real SQLite.
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;
use low_app_layer::owed_belt::{self as belt, Courier};
use low_app_layer::owed_judge as judge;
use owed_db::*;
use serde_json::Value;
use std::cell::RefCell;

fn row_of<'a>(body: &'a Value, outpoint: &str) -> &'a Value {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == outpoint)
        .unwrap_or_else(|| panic!("no row {outpoint} in {body}"))
}

type Answer = Option<(u16, Vec<u8>)>;
/// Answers by URL fragment; counts every call (a refused ask never reaches it).
#[derive(Default)]
struct Fake {
    calls: RefCell<Vec<String>>,
    answers: RefCell<Vec<(String, Answer)>>,
}
impl Fake {
    fn on(&self, fragment: &str, answer: Answer) {
        self.answers.borrow_mut().push((fragment.into(), answer));
    }
}
impl Courier for Fake {
    async fn get(&self, url: String) -> Option<(u16, Vec<u8>)> {
        self.calls.borrow_mut().push(url.clone());
        for (fragment, answer) in self.answers.borrow().iter().rev() {
            if url.contains(fragment.as_str()) {
                return answer.clone();
            }
        }
        if url.ends_with("/spend") {
            return Some((200, br#"{"spent":false}"#.to_vec()));
        }
        Some((404, b"{}".to_vec()))
    }
}

fn due(db: &Db, outpoint: &str) -> Option<i64> {
    db.opt(&format!(
        "SELECT beltDueAtMs FROM owed_rows WHERE outpoint='{outpoint}'"
    ))
}

fn spender_paying_elsewhere(spent: &str) -> (String, Vec<u8>) {
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
    let mut tx = Transaction::new();
    tx.add_input(TransactionInput::new(spent.to_string(), 0))
        .unwrap();
    tx.add_output(TransactionOutput {
        satoshis: Some(20_000),
        locking_script: LockingScript::from_hex(&format!("76a914{}88ac", "11".repeat(20))).unwrap(),
        change: false,
    })
    .unwrap();
    (tx.id(), tx.to_binary())
}

/// D-H1 (the captain's ruling): the owner's own index-UNSPENT stranded hop is pressable on the index's word, so a
/// block neither withholds it at the read nor dates it due at the belt; only a claimable sweep that rests on a courier
/// probe alone (index status not `unspent`) is withheld at the block and re-dated for its look. K strangers' own
/// index-unspent sweeps therefore buy no place ahead of anyone after a block. RED on d521888f: the honest press is
/// withheld at the moved read and every one of the K + 2 sweeps is dated due now.
#[tokio::test]
async fn d2delta_h1_an_index_unspent_sweep_keeps_its_press_across_a_block_and_no_stranger_is_dated() {
    const K: usize = 64;
    let db = Db::new();
    seed_stranded_hop(&db, "owner", "g00", &hop(0));
    seed_courier_unspent_hop(&db, "courier", "c00", &hop(1), 900_000);
    for n in 0..K {
        seed_stranded_hop(&db, &format!("stranger{n:03}"), &format!("s{n}"), &hop(1_000 + n));
    }
    drain_all(&db).await;
    let honest = format!("{}:0", hop(0));
    let leaning = format!("{}:0", hop(1));
    let at_tip = served(&db, "owner", Some(900_000)).await;
    let row = row_of(&at_tip, &honest);
    assert_eq!(row["facts"]["claimable"], true, "{row}");
    assert_eq!(row["facts"]["chainProbe"], "index-unspent", "{row}");
    assert_eq!(row["facts"]["status"], "unspent", "the index's word is a stored fact");
    let row = row_of(&served(&db, "courier", Some(900_000)).await, &leaning).clone();
    assert_eq!(row["facts"]["claimable"], true, "{row}");
    assert_eq!(row["facts"]["chainProbe"], "unspent", "{row}");
    assert_ne!(row["facts"]["status"], "unspent", "{row}");

    // a block: the tip handler runs, no belt tick runs
    let before: Vec<Option<i64>> = (0..K).map(|n| due(&db, &format!("{}:0", hop(1_000 + n)))).collect();
    let honest_due = due(&db, &honest);
    db.now.set(db.now.get() + 1_000);
    db.tip.set(Some(900_001));
    judge::mark_tip(&db, 900_001).await.unwrap();
    let now = db.now.get() as i64;
    assert_eq!(
        db.int(&format!("SELECT COUNT(*) FROM owed_rows WHERE beltDueAtMs={now}")),
        1,
        "the block dates the courier-dependent sweep alone"
    );
    assert_eq!(due(&db, &leaning), Some(now));
    assert_eq!(due(&db, &honest), honest_due, "the index-unspent sweep keeps its own cadence");
    let after: Vec<Option<i64>> = (0..K).map(|n| due(&db, &format!("{}:0", hop(1_000 + n)))).collect();
    assert_eq!(before, after, "no stranger's sweep is dated by the block");

    // the read: the honest press stands with the move stated; the courier-only press is withheld until its look
    let moved = served(&db, "owner", Some(900_001)).await;
    let row = row_of(&moved, &honest);
    assert_eq!(row["facts"]["claim"], "sweep-hop", "{row}");
    assert_eq!(row["facts"]["claimable"], true, "{row}");
    assert_eq!(row["facts"]["chainProbe"], "index-unspent");
    assert_eq!(row["facts"]["tipMoved"], true);
    assert_eq!(row["facts"]["snapshotTip"], 900_000);
    assert_eq!(row["facts"]["chainProbeTip"], 900_000);
    assert_eq!(row["facts"]["servedTip"], 900_001);
    assert!(row["facts"].get("chainWait").is_none(), "{row}");
    let unknown = served(&db, "owner", None).await;
    let row = row_of(&unknown, &honest);
    assert_eq!(row["facts"]["claimable"], true, "{row}");
    assert_eq!(row["facts"]["tipUnknown"], true, "{row}");
    let row = row_of(&served(&db, "courier", Some(900_001)).await, &leaning).clone();
    assert_eq!(row["facts"]["claimable"], false, "{row}");
    assert_eq!(row["facts"]["chainWait"], "block");
    assert_eq!(row["facts"]["tipMoved"], true);

    // the belt's tick: the courier-dependent row is the one due; its look (still unspent) restores its press
    let fake = Fake::default();
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(report.candidates, 1, "{report:?}");
    assert!(fake.calls.borrow().iter().all(|u| u.contains(&hop(1))), "{:?}", fake.calls.borrow());
    let row = row_of(&served(&db, "courier", Some(900_001)).await, &leaning).clone();
    assert_eq!(row["facts"]["claimable"], true, "after its look: {row}");

    // a KNOWN competitor the belt finds on the honest hop downgrades it at the write
    db.exec(&format!(
        "UPDATE owed_rows SET beltDueAtMs={} WHERE outpoint='{honest}'",
        now - 1
    ));
    let (spender, raw) = spender_paying_elsewhere(&hop(0));
    let fake = Fake::default();
    fake.on(
        &format!("{}/0/spent", hop(0)),
        Some((
            200,
            serde_json::json!({"txid": spender, "vin": 0, "status": "unconfirmed"})
                .to_string()
                .into_bytes(),
        )),
    );
    fake.on(&format!("tx/{spender}/hex"), Some((200, hex::encode(&raw).into_bytes())));
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(report.found, 1, "{report:?}");
    let row = row_of(&served(&db, "owner", Some(900_001)).await, &honest).clone();
    assert_ne!(row["family"], "hop-stranded", "{row}");
    assert_ne!(row["facts"]["claimable"], true, "{row}");
}

/// The outpoints the fake was asked about, by hop index, in first-asked order.
fn asked(fake: &Fake, hops: std::ops::Range<usize>) -> Vec<usize> {
    let mut seen = Vec::new();
    for url in fake.calls.borrow().iter() {
        if let Some(n) = hops.clone().find(|n| url.contains(&hop(*n))) {
            if !seen.contains(&n) {
                seen.push(n);
            }
        }
    }
    seen.sort();
    seen
}

/// D-L1: the belt's cursor saves the last identity GIVEN A TURN, and each turn is taken in walk order (the drain's
/// rule), so no identity holding a due row is passed unserved. 80 dated identities, every fourth one due, the due
/// times falling along the walk: three ticks serve them eight at a time in walk order. RED on d521888f: the first tick
/// took the eight OLDEST due of the 64 walked (`left: [32, .., 60]`), its cursor at the 64th, passing seven unserved.
#[tokio::test]
async fn d2delta_l1_the_belt_cursor_passes_no_identity_it_did_not_serve() {
    let db = Db::new();
    for n in 0..80 {
        seed_stranded_hop(&db, &format!("id{n:03}"), &format!("g{n:03}"), &hop(n));
    }
    drain_all(&db).await;
    let now = db.now.get() as i64;
    for n in 0..80 {
        let at = if n % 4 == 0 { now - 1_000 - n as i64 } else { now + 3 * HOUR };
        db.exec(&format!("UPDATE owed_rows SET beltDueAtMs={at} WHERE outpoint='{}:0'", hop(n)));
    }
    let mut served_by_tick = Vec::new();
    for _ in 0..3 {
        let fake = Fake::default();
        belt::tick(&db, &fake).await.unwrap();
        served_by_tick.push(asked(&fake, 0..80));
    }
    assert_eq!(served_by_tick[0], [0, 4, 8, 12, 16, 20, 24, 28], "the first eight due, in walk order");
    assert_eq!(served_by_tick[1], [32, 36, 40, 44, 48, 52, 56, 60]);
    assert_eq!(served_by_tick[2], [64, 68, 72, 76]);
    assert_eq!(
        db.text("SELECT cursor FROM owed_belt_state WHERE name='belt:identities'").as_deref(),
        Some("id076"),
        "the cursor rests on the last identity given a turn"
    );
}
