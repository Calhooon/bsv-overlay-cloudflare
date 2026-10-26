//! The D2-OWED fourth delta lens's folds (bsv-low `docs/audit/D2-OWED-DELTA-4-2026-10-26.md`), on the producer path:
//! the overlay's shipped admission statement, the dirty triggers, the drain and the read over the overlay's
//! migrations on real SQLite.
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Beef, Transaction as Tx, TransactionInput, TransactionOutput};
use owed_db::*;
use serde_json::Value;

fn row_of<'a>(body: &'a Value, outpoint: &str) -> Option<&'a Value> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == outpoint)
}

/// A P2PKH hop and the JOIN that spends its output 0 into a pot output, the JOIN's atomic BEEF as the pot stores it.
fn hop_and_join() -> (Tx, Tx, Vec<u8>) {
    let mut hop = Tx::new();
    hop.add_input(TransactionInput::new("41".repeat(32), 0))
        .unwrap();
    let mut lock = vec![0x76, 0xa9, 0x14];
    lock.extend_from_slice(&[0x41; 20]);
    lock.extend_from_slice(&[0x88, 0xac]);
    hop.add_output(TransactionOutput {
        satoshis: Some(20_190),
        locking_script: LockingScript::from_binary(&lock).unwrap(),
        change: false,
    })
    .unwrap();
    let mut join = Tx::new();
    let mut input = TransactionInput::new(hop.id(), 0);
    input.source_transaction = Some(Box::new(hop.clone()));
    join.inputs.push(input);
    join.outputs.push(TransactionOutput {
        satoshis: Some(40_000),
        locking_script: LockingScript::from_binary(&[0x51]).unwrap(),
        change: false,
    });
    let beef = Beef::from_binary(&join.to_beef(true).unwrap())
        .unwrap()
        .to_binary_atomic(&join.id())
        .unwrap();
    (hop, join, beef)
}

/// D4-L1 (the ruling: every pot admission is a DOWNGRADE mark; a new pot row means hops consumed). The seat's hop is
/// one the index does not hold (claimable on the courier's "unspent" alone), the seat's party filing for the pot is
/// already on record, then the JOIN is admitted through the overlay's own `store_record_sql` with its stored bytes:
/// the read withholds the hop's sweep press until the drain, whose judgement then reads the hop spent by the held
/// JOIN (#837's belt) and serves no press. RED on ae41223c: the pot arrived UNSPENT, so its mark was plain and the
/// read served the press of a hop the admitted JOIN spends.
#[tokio::test]
async fn d4l1_a_join_admitted_withholds_its_seats_sweep_press_until_the_drain() {
    let db = Db::new();
    let (hop, join, beef) = hop_and_join();
    let (hop_txid, pot) = (hop.id(), join.id());
    seed_courier_unspent_hop(&db, "owner", "g00", &hop_txid, 900_000);
    let made = (db.now.get() as i64 - HOUR) / 1000;
    db.exec(&format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,seatSettlePubkey,seatSigHex,sigValid) VALUES ('owner','other','g00','{pot}',0,958846,'3045ab','{}',0,{made},'seat','00',1)", "d4".repeat(32)));
    drain_all(&db).await;
    let op = format!("{hop_txid}:0");
    let body = served(&db, "owner", Some(900_000)).await;
    let row = row_of(&body, &op).unwrap_or_else(|| panic!("{body}"));
    assert_eq!(row["family"], "hop-stranded", "{row}");
    assert_eq!(
        row["facts"]["claimable"], true,
        "the courier's word presses before the JOIN: {row}"
    );

    // the JOIN admitted: the pot row through the shipped upsert, then its stored bytes
    db.conn
        .borrow()
        .execute(
            bsv_overlay_cloudflare::d1_discovery::store_record_sql(),
            rusqlite::params![
                pot,
                0,
                0,
                None::<String>,
                0,
                made,
                "covenant",
                "seat",
                "rival",
                "03cc",
                None::<String>,
                None::<String>,
                None::<String>,
                None::<i64>,
                None::<i64>,
                None::<i64>,
                958_846,
                40_000,
                1
            ],
        )
        .unwrap();
    db.conn
        .borrow()
        .execute(
            "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof, proof_verified) VALUES (?1, ?2, ?3, 0, 0)",
            rusqlite::params![pot, beef, made],
        )
        .unwrap();
    let body = served(&db, "owner", Some(900_000)).await;
    let row = row_of(&body, &op).unwrap_or_else(|| panic!("{body}"));
    assert_eq!(
        row["facts"]["claimable"], false,
        "the JOIN's admission withholds the press until the drain: {row}"
    );
    assert_eq!(row["facts"]["pendingSource"], "pot", "{row}");
    assert_eq!(
        db.int("SELECT MAX(downgrade) FROM owed_dirty WHERE identity='owner' AND gameId='g00' AND source='pot'"),
        1,
        "a pot's admission is a downgrade mark"
    );

    drain_all(&db).await;
    let body = served(&db, "owner", Some(900_000)).await;
    if let Some(row) = row_of(&body, &op) {
        assert_ne!(
            row["facts"]["claimable"], true,
            "the drain judges the hop spent by the held JOIN: {row}"
        );
        assert!(row["facts"].get("refresh").is_none(), "{row}");
    }
}
