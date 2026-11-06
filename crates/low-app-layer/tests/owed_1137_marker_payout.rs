//! OWED-1137 (bsv-low #1137, EPIC #1134): a pot spend that pays a seat's home is owed to the identity whose VERIFIED
//! `LOW/hopparty/v1` marker names the hop that funded the pot, with or without that identity's filings. Every pin
//! runs over the overlay's migrations on real SQLite: the overlay's own rows, the dirty triggers, the shipped drain
//! and the shipped read.
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;
use bsv_rs::primitives::hash::hash160;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Beef, Transaction as Tx, TransactionInput, TransactionOutput};
use owed_db::*;
use serde_json::Value;

const X: &str = "02aa00000000000000000000000000000000000000000000000000000000000001";
const OTHER: &str = "03bb00000000000000000000000000000000000000000000000000000000000002";
/// X's seat settle key K and the opponent's.
const K: &str = "021111111111111111111111111111111111111111111111111111111111111111";
const K2: &str = "032222222222222222222222222222222222222222222222222222222222222222";
const GAME: &str = "1137000000000000000000000000000000000000000000000000000000000001";
const RECOVERY: u64 = 899_000;

fn pkh(key: &str) -> String {
    hex::encode(hash160(&hex::decode(key).unwrap()))
}
fn p2pkh(pkh_hex: &str) -> LockingScript {
    let mut lock = vec![0x76, 0xa9, 0x14];
    lock.extend_from_slice(&hex::decode(pkh_hex).unwrap());
    lock.extend_from_slice(&[0x88, 0xac]);
    LockingScript::from_binary(&lock).unwrap()
}
fn out(sats: u64, lock: LockingScript) -> TransactionOutput {
    TransactionOutput {
        satoshis: Some(sats),
        locking_script: lock,
        change: false,
    }
}

/// The hop (P2PKH to hash160(K) at vout 0), the JOIN spending it into the pot at vout 0, and the pot's spend R
/// paying 19,800 to `my_home` and 19,800 to the opponent's home: (hop, join, spend, the spend's atomic BEEF).
fn chain(my_home: &str) -> (Tx, Tx, Tx, Vec<u8>) {
    let mut hop = Tx::new();
    hop.add_input(TransactionInput::new("41".repeat(32), 0))
        .unwrap();
    hop.add_output(out(20_000, p2pkh(&pkh(K)))).unwrap();
    let mut join = Tx::new();
    let mut input = TransactionInput::new(hop.id(), 0);
    input.source_transaction = Some(Box::new(hop.clone()));
    join.inputs.push(input);
    join.outputs
        .push(out(40_000, LockingScript::from_binary(&[0x51]).unwrap()));
    let mut spend = Tx::new();
    let mut input = TransactionInput::new(join.id(), 0);
    input.source_transaction = Some(Box::new(join.clone()));
    spend.inputs.push(input);
    spend.outputs.push(out(19_800, p2pkh(my_home)));
    spend.outputs.push(out(19_800, p2pkh(&pkh(K2))));
    let beef = Beef::from_binary(&spend.to_beef(true).unwrap())
        .unwrap()
        .to_binary_atomic(&spend.id())
        .unwrap();
    (hop, join, spend, beef)
}

struct Hand {
    hop: String,
    pot: String,
    spend: String,
    beef: Vec<u8>,
}

/// The overlay's rows of one hand: the hop's row spent by the JOIN, X's hop marker (`verified`), the pot's covenant
/// row (decoded, committing K and K2, homes hash160(K) and hash160(K2)), spent by R (`spent`), R's stored BEEF
/// (`bytes`) and the index's proof latch on the spend. No potparty, potrefund or result row for X.
fn seed(db: &Db, my_home: &str, verified: bool, spent: bool, bytes: bool) -> Hand {
    let (hop, join, spend, beef) = chain(my_home);
    let (hop_txid, pot, sp) = (hop.id(), join.id(), spend.id());
    let made = (db.now.get() as i64 - 2 * HOUR) / 1000;
    db.exec(&format!("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,createdAt,lockKind) VALUES ('{hop_txid}',0,1,'{pot}',1,{made},'p2pkh');"));
    db.exec(&format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('{X}','{OTHER}','{GAME}',0,20000,'{K}','00','00',2,'{hop_txid}',1,{made},{v});", v = i64::from(verified)));
    db.exec(&format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind,pubA,pubB,payPkhA,payPkhB,paramsDecoded,recoveryHeight,stakeA,stakeB,potSats) VALUES ('{pot}',0,0,{made},'covenant','{K}','{K2}','{a}','{b}',1,{RECOVERY},20000,20000,40000);", a = pkh(K), b = pkh(K2)));
    let hand = Hand {
        hop: hop_txid,
        pot,
        spend: sp,
        beef,
    };
    if spent {
        record_spend(db, &hand, bytes);
    }
    hand
}

/// The pot's spend as the index records it: R's bytes (when held), then the pointer and the proof latch.
fn record_spend(db: &Db, hand: &Hand, bytes: bool) {
    if bytes {
        db.conn
            .borrow()
            .execute(
                "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof, proof_verified) VALUES (?1, ?2, ?3, 0, 0)",
                rusqlite::params![hand.spend, hand.beef, (db.now.get() / 1000) as i64],
            )
            .unwrap();
    }
    db.exec(&format!("UPDATE pot_records SET spent=1,spendingTxid='{}',spentConfirmed=1,spentHeight=899500 WHERE txid='{}' AND outputIndex=0;", hand.spend, hand.pot));
}

fn rows_of<'a>(body: &'a Value, outpoint: &str) -> Vec<&'a Value> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["outpoint"] == outpoint)
        .collect()
}

async fn judged(db: &Db) -> Value {
    drain_all(db).await;
    served(db, X, Some(900_000)).await
}

/// Pin 1: the verified marker alone attributes the landed spend to X: one claimable `internalize` payout keyed on the
/// pot's outpoint, the spend R its paying transaction, 19,800 sats to hash160(K), `source` chain-marker.
#[tokio::test]
async fn owed1137_a_the_marker_alone_owes_the_landed_spend_to_its_seat() {
    let db = Db::new();
    let h = seed(&db, &pkh(K), true, true, true);
    let body = judged(&db).await;
    let op = format!("{}:0", h.pot);
    let rows = rows_of(&body, &op);
    assert_eq!(rows.len(), 1, "one row for the pot: {body}");
    let r = rows[0];
    assert_eq!(r["family"], "payout", "{r}");
    assert_eq!(r["facts"]["claim"], "internalize", "{r}");
    assert_eq!(r["facts"]["claimable"], true, "{r}");
    assert_eq!(r["facts"]["settleTxid"], h.spend, "{r}");
    assert_eq!(r["facts"]["payTxid"], h.spend, "{r}");
    assert_eq!(r["facts"]["payPkh"], pkh(K), "{r}");
    assert_eq!(r["facts"]["payVout"], 0, "{r}");
    assert_eq!(r["facts"]["source"], "chain-marker", "{r}");
    assert_eq!(r["sats"], 19_800, "{r}");
    assert_eq!(r["gameId"], GAME, "{r}");
    // the hop is the pot's: no stranded or payout row on the hop itself
    assert!(rows_of(&body, &format!("{}:0", h.hop)).is_empty(), "{body}");
}

/// Pin 2 (N1): an UNVERIFIED marker binds nothing: no row for the pot.
#[tokio::test]
async fn owed1137_b_an_unverified_marker_binds_nothing() {
    let db = Db::new();
    let h = seed(&db, &pkh(K), false, true, true);
    let body = judged(&db).await;
    assert!(rows_of(&body, &format!("{}:0", h.pot)).is_empty(), "{body}");
}

/// Pin 3: a spend paying another key is not X's: no chain-marker row and nothing claimable on the pot (the results'
/// own "could not conclude" sentence, which main serves, is left as it was).
#[tokio::test]
async fn owed1137_c_a_spend_paying_another_key_yields_nothing() {
    let db = Db::new();
    let h = seed(&db, &"99".repeat(20), true, true, true);
    let body = judged(&db).await;
    for r in rows_of(&body, &format!("{}:0", h.pot)) {
        assert_ne!(r["facts"]["source"], "chain-marker", "{r}");
        assert_ne!(r["facts"]["claimable"], true, "{r}");
        assert_ne!(r["family"], "payout", "{r}");
    }
}

/// Pin 1b (a guard, green on main): the same seat with the pot's verdict latched (`refund`, the janitor's
/// classification) and still no filing of X's: the link-derived results row already concludes the payout, and the
/// chain marker adds no second row.
#[tokio::test]
async fn owed1137_a2_a_latched_verdict_without_a_filing_is_owed_by_the_marker() {
    let db = Db::new();
    let h = seed(&db, &pkh(K), true, true, true);
    db.exec(&format!("UPDATE pot_records SET verdict='refund',verdictTxid='{sp}' WHERE txid='{pot}' AND outputIndex=0;", sp = h.spend, pot = h.pot));
    let body = judged(&db).await;
    let rows = rows_of(&body, &format!("{}:0", h.pot));
    assert_eq!(rows.len(), 1, "{body}");
    assert_eq!(rows[0]["family"], "payout", "{}", rows[0]);
    assert_eq!(rows[0]["facts"]["claimable"], true, "{}", rows[0]);
}

/// Pin 7's plan: the chain marker's one statement is a primary-key probe of `pot_records`, never a scan.
#[test]
fn owed1137_g_the_marker_pots_read_is_a_key_search() {
    let db = Db::new();
    let plan = db.plan(
        low_app_layer::owed::MARKER_POT_SPENDS_SQL,
        &[rusqlite::types::Value::Text(format!(
            "[\"{}\"]",
            "ab".repeat(32)
        ))],
    );
    assert!(
        plan.iter().all(|l| !l.starts_with("SCAN pot_records")),
        "{plan:?}"
    );
    assert!(
        plan.iter().any(|l| l.contains("pot_records USING")),
        "{plan:?}"
    );
}

/// Pin 4: X's own filings for the pot (a verified party row, the latched refund verdict, a filed refund backup) make
/// the FILED row, and it is the one row: never a second, never the chain-marker's.
#[tokio::test]
async fn owed1137_d_the_filed_row_wins() {
    let db = Db::new();
    let h = seed(&db, &pkh(K), true, true, true);
    let made = (db.now.get() as i64 - HOUR) / 1000;
    db.exec(&format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,seatSettlePubkey,seatSigHex,sigValid) VALUES ('{X}','{OTHER}','{GAME}','{pot}',0,{RECOVERY},'3045ab','{}',0,{made},'{K}','00',1);", "d4".repeat(32), pot = h.pot));
    db.exec(&format!("INSERT INTO potrefund_records(identity,gameId,potTxid,potVout,refundRawHex,sigHex,txid,outputIndex,createdAt,refundValid) VALUES ('{X}','{GAME}','{pot}',0,'00','3045ab','{}',0,{made},1);", "d5".repeat(32), pot = h.pot));
    db.exec(&format!("UPDATE pot_records SET verdict='refund',verdictTxid='{sp}' WHERE txid='{pot}' AND outputIndex=0;", sp = h.spend, pot = h.pot));
    let body = judged(&db).await;
    let rows = rows_of(&body, &format!("{}:0", h.pot));
    assert_eq!(rows.len(), 1, "{body}");
    assert_eq!(rows[0]["family"], "payout", "{}", rows[0]);
    assert_ne!(rows[0]["facts"]["source"], "chain-marker", "{}", rows[0]);
}

/// Pin 5: the pot's spend recorded but R's bytes absent from the index: a pending row with its reason, no press.
#[tokio::test]
async fn owed1137_e_a_spend_whose_bytes_the_index_lacks_is_pending() {
    let db = Db::new();
    let h = seed(&db, &pkh(K), true, true, false);
    let body = judged(&db).await;
    let rows = rows_of(&body, &format!("{}:0", h.pot));
    assert_eq!(rows.len(), 1, "{body}");
    let r = rows[0];
    assert_eq!(r["facts"]["claimable"], false, "{r}");
    assert!(r["facts"]["claim"].is_null(), "{r}");
    assert_eq!(r["facts"]["source"], "chain-marker", "{r}");
    assert_eq!(r["facts"]["settleTxid"], h.spend, "{r}");
    assert_eq!(
        r["reason"],
        low_app_layer::owed::CHAIN_MARKER_BYTES_PENDING_REASON,
        "{r}"
    );
}

/// Pin 6, the write path: the pot judged unspent first, then its spend recorded (the index's pointer write): the
/// write itself marks X's game dirty, a downgrade, and the drain's next judgement produces the payout.
#[tokio::test]
async fn owed1137_f_recording_the_spend_marks_the_marker_seat_and_the_next_judge_owes_it() {
    let db = Db::new();
    let h = seed(&db, &pkh(K), true, false, true);
    drain_all(&db).await;
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), 0);
    record_spend(&db, &h, true);
    assert!(
        db.int(&format!(
            "SELECT COUNT(*) FROM owed_dirty WHERE identity='{X}' AND gameId='{GAME}'"
        )) >= 1,
        "recording the pot's spend marks the marker's seat"
    );
    let body = judged(&db).await;
    let rows = rows_of(&body, &format!("{}:0", h.pot));
    assert_eq!(rows.len(), 1, "{body}");
    assert_eq!(rows[0]["facts"]["source"], "chain-marker", "{}", rows[0]);
    assert_eq!(rows[0]["facts"]["claimable"], true, "{}", rows[0]);
}
