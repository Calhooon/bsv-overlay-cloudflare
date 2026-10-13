use bsv_rs::transaction::{MerklePath, Transaction};
use bsv_tracker::Header;
use low_app_layer::tracker_host::{runtime, stored, HeaderSnapshot};
use std::{cell::RefCell, collections::BTreeMap};

#[tokio::test]
async fn ns3d_m2_bad_middle_proof_advances_cursor_and_writes_next_subject() {
    let mut subjects = Vec::new();
    let mut roots = BTreeMap::new();
    for n in 1..=3 {
        let mut tx = Transaction::from_hex(
            include_str!(
                "fixtures/176885049985a858244993ab5591bfeb61b9368ac12fc7f12540d0824e5f891e.hex"
            )
            .trim(),
        )
        .unwrap();
        tx.lock_time = n;
        let id = tx.id();
        tx.merkle_path = Some(MerklePath::from_coinbase_txid(&id, n));
        roots.insert(
            n,
            Ok(Some(Header {
                hash: "02".repeat(32),
                merkle_root: if n == 2 { "ff".repeat(32) } else { id.clone() },
            })),
        );
        subjects.push(runtime::BackfillSubject {
            txid: id,
            beef: Some(tx.to_beef(true).unwrap()),
            has_proof: 1.0,
            tracked: None,
        });
    }
    let last = subjects[2].txid.clone();
    let written = RefCell::new(Vec::new());
    let cursor = runtime::walk_backfill_page(
        subjects,
        String::new(),
        100,
        |_| {
            std::future::ready(HeaderSnapshot {
                tip: Ok(3),
                headers: roots.clone(),
            })
        },
        |row| {
            written.borrow_mut().push(row.txid);
            std::future::ready(Ok(()))
        },
    )
    .await
    .expect("bad held proof must not stall the page");
    assert_eq!(cursor, last);
    assert_eq!(written.borrow().len(), 2);
    assert_eq!(written.borrow().last(), Some(&last));
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::BACKFILL_CREATE).unwrap();
    db.execute(
        stored::BACKFILL_SAVE,
        (stored::BACKFILL_VERSION, format!("0:{cursor}"), 0),
    )
    .unwrap();
    assert_eq!(
        db.query_row("SELECT cursor FROM tracker_backfill", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        format!("0:{last}")
    );
}
