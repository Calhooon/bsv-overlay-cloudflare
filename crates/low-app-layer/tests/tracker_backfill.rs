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
        // Public field mutation must not reuse the SDK cached transaction hash.
        tx.invalidate_caches();
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

#[tokio::test]
async fn ns3d2_l1_header_fault_retries_middle_subject_next_pass() {
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
        // Public field mutation must not reuse the SDK cached transaction hash.
        tx.invalidate_caches();
        let id = tx.id();
        tx.merkle_path = Some(MerklePath::from_coinbase_txid(&id, n));
        roots.insert(
            n,
            Ok(Some(Header {
                hash: "02".repeat(32),
                merkle_root: id.clone(),
            })),
        );
        subjects.push(runtime::BackfillSubject {
            txid: id,
            beef: Some(tx.to_beef(true).unwrap()),
            has_proof: 1.0,
            tracked: None,
        });
    }
    let first = subjects[0].txid.clone();
    let next = subjects.split_off(1);
    let first_pass = subjects
        .into_iter()
        .chain(next.iter().map(|s| runtime::BackfillSubject {
            txid: s.txid.clone(),
            beef: s.beef.clone(),
            has_proof: s.has_proof,
            tracked: None,
        }))
        .collect();
    let writes = RefCell::new(Vec::new());
    let cursor = runtime::walk_backfill_page(
        first_pass,
        String::new(),
        100,
        |height| {
            let mut headers = roots.clone();
            if height == 2 {
                headers.insert(
                    2,
                    Err(bsv_tracker::HeaderError(
                        "transient CHAINTRACKS fault".into(),
                    )),
                );
            }
            std::future::ready(HeaderSnapshot {
                tip: Ok(3),
                headers,
            })
        },
        |row| {
            writes.borrow_mut().push(row.txid);
            std::future::ready(Ok(()))
        },
    )
    .await
    .unwrap();
    assert_eq!(cursor, first, "header fault skipped the retry subject");
    assert_eq!(writes.borrow().len(), 1, "pass did not end at header fault");
    let last = next[1].txid.clone();
    let cursor = runtime::walk_backfill_page(
        next,
        cursor,
        101,
        |_| {
            std::future::ready(HeaderSnapshot {
                tip: Ok(3),
                headers: roots.clone(),
            })
        },
        |row| {
            writes.borrow_mut().push(row.txid);
            std::future::ready(Ok(()))
        },
    )
    .await
    .unwrap();
    assert_eq!(cursor, last);
    assert_eq!(writes.borrow().len(), 3);
}

/// bsv-low #778, #781 (B-FLEET-4): A CHAINTRACKS AWAIT THAT NEVER ANSWERS NO LONGER HOLDS THE PASS. The backfill's
/// header awaits had no timeout, ahead of the chain-event intake, so one pending answer held the tracker task (and
/// the intake behind it) for as long as the platform let it (#778: a 66 s pass that skipped the intake). Now the
/// page is walked through `walk_backfill_page_within`: the second subject's header await never answers, the
/// deadline drops it at its wall less the save's reserve, the page stops there as on a CHAINTRACKS fault (the first
/// subject written, the cursor at it, the second retried next pass), all on tokio's paused clock at the deadline's
/// own arithmetic. RED with the race removed from `walk_backfill_page_within` (the header future passed through): the
/// page never answers and the outer 60 s guard fires. And the worker's backfill walks under its own deadline.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn b_fleet_4_a_header_await_that_never_answers_stops_the_page_at_the_deadline() {
    use std::time::Duration;
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
        tx.invalidate_caches();
        let id = tx.id();
        tx.merkle_path = Some(MerklePath::from_coinbase_txid(&id, n));
        roots.insert(
            n,
            Ok(Some(Header {
                hash: "02".repeat(32),
                merkle_root: id.clone(),
            })),
        );
        subjects.push(runtime::BackfillSubject {
            txid: id,
            beef: Some(tx.to_beef(true).unwrap()),
            has_proof: 1.0,
            tracked: None,
        });
    }
    let first = subjects[0].txid.clone();
    let t0 = tokio::time::Instant::now();
    let deadline = low_app_layer::pot_hook::Deadline::new(
        runtime::BACKFILL_WALL_MS,
        runtime::BACKFILL_SAVE_RESERVE_MS,
        Box::new(move || t0.elapsed().as_millis() as u64),
        Box::new(|ms| Box::pin(tokio::time::sleep(Duration::from_millis(ms)))),
    );
    let writes = RefCell::new(Vec::new());
    let walked = tokio::time::timeout(
        Duration::from_secs(60),
        runtime::walk_backfill_page_within(
            subjects,
            String::new(),
            100,
            &deadline,
            |height| {
                let roots = roots.clone();
                async move {
                    if height == 2 {
                        std::future::pending::<()>().await;
                    }
                    HeaderSnapshot {
                        tip: Ok(3),
                        headers: roots,
                    }
                }
            },
            |row| {
                writes.borrow_mut().push(row.txid);
                std::future::ready(Ok(()))
            },
        ),
    )
    .await
    .expect("a header await that never answers must not hold the backfill past its deadline");
    let elapsed = t0.elapsed().as_millis() as u64;
    assert_eq!(
        walked.unwrap(),
        first,
        "the page stops at the dropped subject"
    );
    assert_eq!(
        writes.borrow().len(),
        1,
        "only the subject before it is written"
    );
    assert_eq!(
        elapsed,
        runtime::BACKFILL_WALL_MS - runtime::BACKFILL_SAVE_RESERVE_MS,
        "dropped at the wall less the save's reserve"
    );
    // a compile-time pin (clippy: an assertion on constants): the backfill and the intake fit inside the drain task's wall
    const _: () = assert!(
        runtime::BACKFILL_WALL_MS + low_app_layer::tracker_host::chain_events::WALL_MS
            <= low_app_layer::pot_hook::DRAIN_WALL_MS,
        "the backfill and the intake fit inside the drain task's wall"
    );
    let src: String = include_str!("../src/tracker_host/runtime.rs")
        .lines()
        .map(|l| l.split("//").next().unwrap_or(""))
        .collect::<String>()
        .split_whitespace()
        .collect();
    let backfill = &src[src
        .find("pubasyncfnbackfill(env:&worker::Env)")
        .expect("the worker's backfill")..];
    let backfill = &backfill[..backfill.find("pubfnhook_may_reask").unwrap()];
    assert!(backfill.contains(
        "letdeadline=crate::routes::worker_deadline(BACKFILL_WALL_MS,BACKFILL_SAVE_RESERVE_MS);"
    ));
    assert!(
        backfill
            .contains("walk_backfill_page_within(rows,after,super::WorkerClock.now(),&deadline,"),
        "the worker's backfill walks its page under the deadline"
    );
}
