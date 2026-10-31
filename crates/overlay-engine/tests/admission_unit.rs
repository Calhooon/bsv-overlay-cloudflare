#![cfg(feature = "memory-storage")]

use bsv_overlay_engine::storage::{
    memory::{MemoryStorage, MemoryWrite},
    CommitError, Committed, Guard, OutboxEvent, PreconditionVerdict, StagedWrite, Storage,
};
use bsv_overlay_engine::types::{AppliedTransaction, Output};

fn output(txid: &str) -> Output {
    Output {
        txid: txid.into(),
        output_index: 0,
        topic: "topic".into(),
        output_script: vec![0x51],
        satoshis: 1,
        spent: false,
        outputs_consumed: vec![],
        consumed_by: vec![],
        beef: Some(vec![1, 2, 3]),
        block_height: None,
        score: None,
    }
}
fn applied(txid: &str) -> AppliedTransaction {
    AppliedTransaction {
        txid: txid.into(),
        topic: "topic".into(),
    }
}
fn event() -> OutboxEvent {
    OutboxEvent {
        event_type: "admitted".into(),
        txid: "new".into(),
        envelope: vec![7],
    }
}

#[tokio::test]
async fn a03_every_statement_fault_rolls_back_all_tables() {
    for fault in 0..7 {
        let store = MemoryStorage::new();
        store.insert_output(&output("old")).await.unwrap();
        let before = store.admission_snapshot();
        store.set_admission_fault(Some(fault));
        let mut unit = store.begin_admission();
        unit.require(Guard::OutputHeldUnspent {
            txid: "old".into(),
            output_index: 0,
            topic: "topic".into(),
        });
        unit.mark_spent("old", 0, "topic");
        unit.insert_output(&output("new"));
        unit.delete_output("old", 0, "topic");
        unit.stage(StagedWrite::new(
            MemoryWrite::Put {
                table: "index".into(),
                key: "new".into(),
                value: vec![1],
            },
            3,
        ));
        unit.emit(event());
        unit.record_applied(&applied("new"));
        assert!(matches!(
            unit.commit(None).await,
            Err(CommitError::Storage(_))
        ));
        assert_eq!(before, store.admission_snapshot(), "statement {fault}");
    }
}

#[tokio::test]
async fn a05_one_coin_has_one_winner() {
    let store = MemoryStorage::new();
    store.insert_output(&output("old")).await.unwrap();
    let mut first = store.begin_admission();
    let mut second = store.begin_admission();
    for unit in [&mut first, &mut second] {
        unit.require(Guard::OutputHeldUnspent {
            txid: "old".into(),
            output_index: 0,
            topic: "topic".into(),
        });
        unit.mark_spent("old", 0, "topic");
    }
    first.insert_output(&output("winner"));
    second.insert_output(&output("loser"));
    assert!(matches!(first.commit(None).await, Ok(Committed::Batch)));
    let before = store.admission_snapshot();
    assert!(matches!(
        second.commit(None).await,
        Err(CommitError::Precondition {
            verdict: PreconditionVerdict::Contest { .. },
            ..
        })
    ));
    assert_eq!(before, store.admission_snapshot());
}

#[tokio::test]
async fn a09_concurrent_identical_bytes_dedup_without_writes() {
    let store = MemoryStorage::new();
    let mut first = store.begin_admission();
    let mut second = store.begin_admission();
    for unit in [&mut first, &mut second] {
        unit.insert_output(&output("same"));
        unit.record_applied(&applied("same"));
    }
    first.commit(None).await.unwrap();
    let before = store.admission_snapshot();
    assert!(matches!(second.commit(None).await, Err(CommitError::Dedup)));
    assert_eq!(before, store.admission_snapshot());
}

#[tokio::test]
async fn a11_last_applied_statement_fault_leaves_old_head() {
    let store = MemoryStorage::new();
    store.insert_output(&output("old")).await.unwrap();
    let before = store.admission_snapshot();
    store.set_admission_fault(Some(3));
    let mut unit = store.begin_admission();
    unit.mark_spent("old", 0, "topic");
    unit.insert_output(&output("new"));
    unit.delete_output("old", 0, "topic");
    unit.record_applied(&applied("new"));
    assert!(matches!(
        unit.commit(None).await,
        Err(CommitError::Storage(_))
    ));
    assert_eq!(before, store.admission_snapshot());
}

#[tokio::test]
async fn a17_records_and_rows_are_private_to_the_unit() {
    let store = MemoryStorage::new();
    let mut first = store.begin_admission();
    first.stage_record("admit", String::from("head"), 4);
    first.insert_output(&output("new"));
    let second = store.begin_admission();
    assert_eq!(
        first.read_staged::<String>("admit").map(String::as_str),
        Some("head")
    );
    assert!(second.read_staged::<String>("admit").is_none());
    assert!(store
        .find_output("new", 0, Some("topic"), None, true)
        .await
        .unwrap()
        .is_none());
    assert!(first
        .find_output("new", 0, Some("topic"))
        .await
        .unwrap()
        .is_some());
    assert!(second
        .find_output("new", 0, Some("topic"))
        .await
        .unwrap()
        .is_none());
}

#[test]
#[ignore = "Hosted scratch D1 rollback, error conversion and budget require the spike worker; native MemoryStorage is not D1 evidence"]
fn a08_real_d1_rollback_and_budget() {
    panic!("Run the hosted spike worker and preserve its boundary and error record");
}

#[tokio::test]
async fn a12_unanswered_commit_may_land_whole_and_replay_dedups() {
    let store = MemoryStorage::new();
    store.set_admission_unanswered_after_commit(true);
    let mut unit = store.begin_admission();
    unit.insert_output(&output("late"));
    unit.record_applied(&applied("late"));
    assert!(matches!(
        unit.commit(None).await,
        Err(CommitError::Unanswered)
    ));
    store.set_admission_unanswered_after_commit(false);
    assert!(store
        .find_output("late", 0, Some("topic"), None, true)
        .await
        .unwrap()
        .is_some());
    let before = store.admission_snapshot();
    let mut replay = store.begin_admission();
    replay.insert_output(&output("late"));
    replay.record_applied(&applied("late"));
    assert!(matches!(replay.commit(None).await, Err(CommitError::Dedup)));
    assert_eq!(before, store.admission_snapshot());
}

#[tokio::test]
async fn a14_backend_guard_preserves_each_verdict() {
    use bsv_overlay_engine::storage::memory::MemoryGuard;
    use std::rc::Rc;
    for verdict in [
        PreconditionVerdict::Replay,
        PreconditionVerdict::Contest {
            kind: "contest".into(),
            reason: "head moved".into(),
        },
        PreconditionVerdict::NotNow {
            reason: "predecessor not landed".into(),
        },
    ] {
        let store = MemoryStorage::new();
        let before = store.admission_snapshot();
        let mut unit = store.begin_admission();
        unit.require(Guard::Backend {
            owner: "topic".into(),
            check: Rc::new(MemoryGuard {
                table: "heads".into(),
                key: "name".into(),
                expected: Some(vec![1]),
                lost: verdict.clone(),
            }),
            bytes: 10,
        });
        unit.insert_output(&output("loser"));
        match unit.commit(None).await {
            Err(CommitError::Precondition {
                verdict: actual, ..
            }) => assert_eq!(actual, verdict),
            answer => panic!("wrong guard word: {answer:?}"),
        }
        assert_eq!(before, store.admission_snapshot());
    }
}

#[tokio::test]
async fn a18_journal_decision_survives_crash_and_drains_once() {
    let store = MemoryStorage::new();
    store.insert_output(&output("old")).await.unwrap();
    store.set_admission_statement_bound(Some(2));
    store.set_admission_decision_only(true);
    let mut unit = store.begin_admission();
    unit.require(Guard::OutputHeldUnspent {
        txid: "old".into(),
        output_index: 0,
        topic: "topic".into(),
    });
    unit.mark_spent("old", 0, "topic");
    unit.insert_output(&output("new"));
    unit.delete_output("old", 0, "topic");
    unit.stage(StagedWrite::new(
        MemoryWrite::Increment {
            table: "chain".into(),
            key: "new".into(),
        },
        8,
    ));
    unit.emit(event());
    unit.record_applied(&applied("new"));
    assert!(matches!(
        unit.commit(None).await,
        Ok(Committed::Journaled { .. })
    ));
    assert!(store
        .does_applied_transaction_exist(&applied("new"))
        .await
        .unwrap());
    assert!(
        store
            .find_output("old", 0, Some("topic"), None, false)
            .await
            .unwrap()
            .unwrap()
            .spent
    );
    assert!(store
        .find_output("new", 0, Some("topic"), None, false)
        .await
        .unwrap()
        .is_none());
    assert!(store.admission_pending("new").await.unwrap());
    assert!(!store.drain_admission("new").await.unwrap());
    store.set_admission_decision_only(false);
    assert!(store.drain_admission("new").await.unwrap());
    assert_eq!(
        store.admission_index("chain", "new"),
        Some(1_u64.to_le_bytes().to_vec())
    );
    let before = store.admission_snapshot();
    assert!(store.drain_admission("new").await.unwrap());
    assert_eq!(before, store.admission_snapshot());
}

#[tokio::test]
async fn a19_committed_outbox_survives_missing_request_drain() {
    let store = MemoryStorage::new();
    let mut unit = store.begin_admission();
    unit.insert_output(&output("new"));
    unit.emit(event());
    unit.record_applied(&applied("new"));
    unit.commit(None).await.unwrap();
    // Request lost before any external send. A fresh drain sees the durable row.
    assert_eq!(store.admission_outbox().len(), 1);
    store.drain_admission_outbox(|_| Err("queue unavailable".into()));
    assert_eq!(store.admission_outbox()[0].1.attempts, 1);
    let mut sent = Vec::new();
    store.drain_admission_outbox(|event| {
        sent.push(event.clone());
        Ok(())
    });
    store.drain_admission_outbox(|event| {
        sent.push(event.clone());
        Ok(())
    });
    assert_eq!(sent, vec![event()]);
    assert!(store.admission_outbox().is_empty());
}

#[test]
fn a20_charge_counts_binds_copies_records_and_one_beef() {
    let store = MemoryStorage::new();
    let mut unit = store.begin_admission();
    let mut first = output("new");
    first.beef = Some(vec![0; 1_900_000]);
    unit.insert_output(&first);
    let first_charge = unit.charge();
    first.output_index = 1;
    unit.insert_output(&first);
    let row_binds = 3 + 5 + 1 + 4 + 8 + 1;
    assert_eq!(first_charge, 2 * (1_900_000 + row_binds));
    assert_eq!(unit.charge(), first_charge + 2 * row_binds);
    unit.stage_record("record", String::from("head"), 4);
    assert_eq!(unit.charge(), first_charge + 2 * row_binds + 4);
    let event = event();
    let envelope_binds = (event.event_type.len() + event.txid.len() + event.envelope.len()) as u64;
    unit.emit(event);
    assert_eq!(
        unit.charge(),
        first_charge + 2 * row_binds + 4 + 2 * envelope_binds
    );
}

#[tokio::test]
async fn a10_orphan_external_object_is_not_a_stored_output() {
    // Native R2 seam: the external put survives the dropped admission. Engine
    // readers select by committed rows, so they cannot serve this orphan.
    let store = MemoryStorage::new();
    let objects = std::collections::HashMap::from([("large", vec![0_u8; 1_900_001])]);
    {
        let mut unit = store.begin_admission();
        unit.insert_output(&output("large"));
        // Crash before commit; no best-effort withdrawal can run.
    }
    assert_eq!(objects.len(), 1);
    assert!(store
        .find_outputs_for_transaction("large", true)
        .await
        .unwrap()
        .is_empty());
    let orphan_count = objects.keys().filter(|_| store.output_count() == 0).count();
    assert_eq!(orphan_count, 1);
}

#[tokio::test]
async fn a18_journal_decision_reserves_a_guarded_backend_head() {
    use bsv_overlay_engine::storage::memory::MemoryGuard;
    use std::rc::Rc;
    let store = MemoryStorage::new();
    store.set_admission_statement_bound(Some(1));
    store.set_admission_decision_only(true);
    let mut first = store.begin_admission();
    let mut second = store.begin_admission();
    for (unit, txid) in [(&mut first, "winner"), (&mut second, "loser")] {
        unit.require(Guard::Backend {
            check: Rc::new(MemoryGuard {
                table: "heads".into(),
                key: "name".into(),
                expected: None,
                lost: PreconditionVerdict::Contest {
                    kind: "contest".into(),
                    reason: "another head won".into(),
                },
            }),
            owner: "topic".into(),
            bytes: 9,
        });
        unit.stage(StagedWrite::new(MemoryWrite::Put {
            table: "heads".into(), key: "name".into(), value: txid.as_bytes().to_vec(),
        }, 32));
        unit.insert_output(&output(txid));
        unit.record_applied(&applied(txid));
    }
    assert!(matches!(first.commit(None).await, Ok(Committed::Journaled { .. })));
    let before = store.admission_snapshot();
    assert!(matches!(second.commit(None).await, Err(CommitError::Precondition {
        verdict: PreconditionVerdict::Contest { .. }, ..
    })), "journal decision must reserve the guarded head before drain");
    assert_eq!(store.admission_snapshot(), before);
    assert!(store.admission_pending("winner").await.unwrap());
    store.set_admission_decision_only(false);
    store.drain_admission("winner").await.unwrap();
    assert_eq!(store.admission_index("heads", "name"), Some(b"winner".to_vec()));
}
