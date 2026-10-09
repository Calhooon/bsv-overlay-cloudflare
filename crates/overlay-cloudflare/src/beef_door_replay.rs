//! NL-6: the P0-5f Worker door witnesses, inverted. A valid BEEF over each
//! former budget passes every pure Worker door; invalid bytes are refused
//! with the offset and the kind.
use crate::{dead_letters, ef, queue};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use beef_limits::*;
use overlay_engine::beef_limits;

#[path = "../../overlay-engine/tests/support/beef_doors.rs"]
mod shapes;

fn message(bytes: &[u8]) -> queue::MutationMessage {
    queue::MutationMessage {
        beef_b64: STANDARD.encode(bytes),
        r2: None,
        topics: vec!["tm_test".into()],
        mode: "historical-tx".into(),
        reason: "boundary witness".into(),
        redrive: None,
        ef_job: None,
    }
}

/// A BEEF and its subject's txid.
type Shape = (Vec<u8>, String);

/// One shape at each former bound (read then, read now) and one over it.
fn at_and_over(door: &BeefLimits) -> [(Shape, Shape); 3] {
    [
        (shapes::transactions(512), shapes::transactions(513)),
        (shapes::bumps(512), shapes::bumps(513)),
        (
            shapes::sized_body(door.max_bytes),
            shapes::sized_body(door.max_bytes + 1),
        ),
    ]
}

#[test]
fn ef_conversion_over_every_former_bound() {
    for (_, (over, _)) in at_and_over(&EF_BEEF_LIMITS) {
        let converted = ef::beef_to_ef_batch(&over);
        assert!(converted.is_ok(), "{:?}", converted.err());
    }
}

#[test]
fn ef_auxiliary_readers_read_one_over_as_they_read_one_at() {
    for ((at, at_id), (over, over_id)) in at_and_over(&EF_BEEF_LIMITS) {
        assert!(ef::proven_subject_raw(&at).is_some());
        assert!(ef::proven_subject_raw(&over).is_some());
        assert!(ef::strip_subject_bump(&at, &at_id).is_some());
        assert!(ef::strip_subject_bump(&over, &over_id).is_some());
        assert_eq!(
            ef::missing_source_txids(&over),
            ef::missing_source_txids(&at)
        );
    }
}

#[test]
fn dead_letter_subject_over_every_former_bound() {
    for (_, (over, id)) in at_and_over(&DEAD_LETTER_BEEF_LIMITS) {
        assert_eq!(dead_letters::subject_of(&message(&over)), Some(id));
    }
}

#[test]
fn queue_replay_over_every_former_bound() {
    for (_, (over, _)) in at_and_over(&QUEUE_BEEF_LIMITS) {
        assert_eq!(
            queue::decode_replay_beef(&STANDARD.encode(&over)).unwrap(),
            over
        );
    }
}

#[test]
fn the_worker_doors_refuse_invalid_bytes_with_the_offset_and_the_kind() {
    for (bytes, offset, kind) in shapes::invalid() {
        let named = format!("invalid BEEF at byte {offset}: {kind}");
        let error = queue::decode_replay_beef(&STANDARD.encode(&bytes))
            .expect_err("the queue door read invalid bytes");
        assert!(error.contains(&named), "{named}: {error}");
        let error = match ef::beef_to_ef_batch(&bytes) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("the EF door read invalid bytes"),
        };
        assert!(error.contains(&named), "{named}: {error}");
        assert!(dead_letters::subject_of(&message(&bytes)).is_none());
        assert!(ef::proven_subject_raw(&bytes).is_none());
    }
}

/// The stored-BEEF readers that moved onto streaming folds (NL-6): the
/// subject's OWN bump of a stored body over each former bound is read as the
/// in-memory index reads it, and the funding body no longer has a byte bound.
#[test]
fn stored_readers_over_every_former_bound() {
    use bsv_rs::transaction::Beef;
    for (_, (over, id)) in at_and_over(&STORED_BEEF_LIMITS) {
        let beef = Beef::from_binary(&over).unwrap();
        let entry = beef.find_txid(&id).unwrap();
        let own = &beef.bumps[entry.bump_index().unwrap()];
        assert_eq!(
            crate::reorg_sweep::stored_bump_hex(&over, &id),
            Some(own.to_hex())
        );
        assert_eq!(
            crate::proof_fetcher::own_bump_hex(&over, &id),
            Some(own.to_hex())
        );
        let anchor = crate::reorg_sweep::stored_bump_anchor(&over, &id).unwrap();
        assert_eq!(anchor.height, 800_000);
        assert_eq!(anchor.root, own.compute_root(Some(&id)).unwrap());
        assert!(crate::d1_storage::D1Storage::beef_has_proof(&id, &over));
        assert!(crate::proof_fetcher::funding_output_script(&over, &id, 0).is_some());
    }
    for (bytes, _, _) in shapes::invalid() {
        let id = "11".repeat(32);
        assert!(crate::reorg_sweep::stored_bump_hex(&bytes, &id).is_none());
        assert!(crate::reorg_sweep::stored_bump_anchor(&bytes, &id).is_none());
        assert!(!crate::d1_storage::D1Storage::beef_has_proof(&id, &bytes));
    }
}

/// The ARC callback's JSON body is not a BEEF: its 1 MiB bound stays.
#[test]
fn arc_callback_body_at_and_one_over() {
    let mut body = format!(
        r#"{{"txid":"{}","txStatus":"SEEN_ON_NETWORK"}}"#,
        "aa".repeat(32)
    );
    body.extend(std::iter::repeat_n(
        ' ',
        crate::routes::ARC_INGEST_BODY_MAX_BYTES - body.len(),
    ));
    assert!(crate::routes::classify_arc_ingest_body(&body).is_ok());
    body.push(' ');
    let error = match crate::routes::classify_arc_ingest_body(&body) {
        Err(error) => error,
        Ok(_) => panic!("one extra body byte admitted"),
    };
    assert!(error.contains("max_bytes"), "{error}");
}

/// bsv-low #585 (door 3): a VALID 500 KB BEEF past the queue's room is carried by key and named by its SUBJECT (the
/// letter's key in both consumers, no bytes read); the object's bytes pass the replay's check exactly when the
/// consumer's policy admits them (`QUEUE_BEEF_LIMITS`, NL-6's: no cap of the R2 path's own). A body that policy
/// admits today, sent by key under a lowered room, passes it now, and its subject read from the object is the
/// message's.
#[test]
fn e585_d3_a_valid_500kb_beef_is_keyed_by_its_subject_and_replays_under_the_consumers_policy() {
    use overlay_engine::types::SubmitMode;
    let topics = vec!["tm_test".to_string()];
    let (beef, id) = shapes::sized_body(500_000);
    let plan = queue::plan_replay(
        &beef,
        &topics,
        SubmitMode::HistoricalTx,
        queue::REPLAY_REASON_PHASE3_FAULT,
        queue::QUEUE_MESSAGE_ROOM,
        &SUBMIT_BEEF_LIMITS,
    )
    .unwrap();
    let queue::Carriage::R2(msg) = plan else {
        panic!("500 KB rides by key")
    };
    let r = msg.r2.clone().unwrap();
    assert_eq!(r.txid.as_deref(), Some(id.as_str()));
    assert_eq!(
        dead_letters::letter_key(&msg, None),
        (id.clone(), "tm_test".to_string())
    );
    queue::check_blob(&r, &beef).unwrap();
    let replay = queue::check_replay_blob(&r, &beef);
    if QUEUE_BEEF_LIMITS.max_bytes >= beef.len() {
        replay.unwrap();
    } else {
        assert!(replay.unwrap_err().contains("max_bytes"));
    }

    let (small, small_id) = shapes::body(8_000);
    let plan = queue::plan_replay(
        &small,
        &topics,
        SubmitMode::HistoricalTx,
        queue::REPLAY_REASON_PHASE3_FAULT,
        queue::QUEUE_MESSAGE_ROOM_MIN,
        &QUEUE_BEEF_LIMITS,
    )
    .unwrap();
    let queue::Carriage::R2(msg) = plan else {
        panic!("8 KB rides by key under a 1 KB room")
    };
    let r = msg.r2.clone().unwrap();
    queue::check_replay_blob(&r, &small).unwrap();
    let mut named = beef_limits::parse_beef(&small, &QUEUE_BEEF_LIMITS).unwrap();
    assert_eq!(ef::subject_txid_of(&mut named), Some(small_id.clone()));
    assert_eq!(r.txid, Some(small_id));
    // the same body under the default room is inline: the door changed nothing below it
    assert!(matches!(
        queue::plan_replay(
            &small,
            &topics,
            SubmitMode::HistoricalTx,
            queue::REPLAY_REASON_PHASE3_FAULT,
            queue::QUEUE_MESSAGE_ROOM,
            &QUEUE_BEEF_LIMITS
        ),
        Ok(queue::Carriage::Inline(_))
    ));
}

// ── bsv-low #585, the d3 fold: the TWIN ──────────────────────────────────────

mod twin {
    use super::*;
    use crate::d1::{QVal, Query};
    use overlay_engine::types::SubmitMode;
    use std::collections::HashMap;

    /// The storage's own statement for an applied row (`D1Storage::insert_applied_transaction`).
    const APPLIED_INSERT: &str =
        "INSERT OR IGNORE INTO applied_transactions (txid, topic) VALUES (?, ?)";

    fn db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for m in crate::d1::OVERLAY_MIGRATIONS
            .iter()
            .filter(|m| m.contains("applied_transactions") || m.contains("mutation_dead_letters"))
        {
            conn.execute(m, []).unwrap();
        }
        conn
    }

    fn run(conn: &rusqlite::Connection, q: &Query) -> usize {
        let binds: Vec<rusqlite::types::Value> = q
            .params()
            .iter()
            .map(|p| match p {
                QVal::Null => rusqlite::types::Value::Null,
                QVal::Int(i) => rusqlite::types::Value::Integer(*i),
                QVal::Text(s) => rusqlite::types::Value::Text(s.clone()),
                QVal::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
                QVal::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
                QVal::Float(f) => rusqlite::types::Value::Real(*f),
            })
            .collect();
        let mut stmt = conn.prepare(q.sql()).unwrap();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(binds.iter()))
            .unwrap();
        let mut n = 0;
        while rows.next().unwrap().is_some() {
            n += 1;
        }
        n
    }

    /// What one delivery of a keyed message came to at the main consumer.
    #[derive(Debug, PartialEq, Eq)]
    enum Delivered {
        /// Read, replayed, durable: acked, its object deleted (the deletion rule).
        Landed,
        /// Its object was MISSING and the verdict acked it: no note, no letter.
        Acked(queue::MissingVerdict),
        /// Handed back with this fault noted (it dead-letters and is parked).
        Fault(String),
    }

    /// The consumer as `queue_handler` runs a keyed message, over a bucket and the shipped statements: the read,
    /// the check, the replay (its landing is the storage's applied rows), the ack's delete; and, on a MISSING
    /// object, the REAL verdict over the REAL read of the applied rows.
    struct Consumer {
        bucket: HashMap<String, Vec<u8>>,
        conn: rusqlite::Connection,
    }

    impl Consumer {
        fn applied(&self, txid: &str) -> Vec<String> {
            self.conn
                .prepare(queue::TWIN_APPLIED_SQL)
                .unwrap()
                .query_map([txid], |r| r.get::<_, String>(0))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        }

        fn deliver(&mut self, m: &queue::MutationMessage, lands_in: &[&str]) -> Delivered {
            let r = m.r2.as_ref().unwrap();
            let Some(bytes) = self.bucket.get(&r.key) else {
                let fault = queue::BlobFault::Missing(r.key.clone());
                let applied = r.txid.as_deref().map_or_else(Vec::new, |t| self.applied(t));
                return match queue::missing_verdict(
                    r.txid.as_deref(),
                    &m.topics,
                    &Ok(false),
                    &Ok(applied),
                ) {
                    queue::MissingVerdict::Fault(e) => {
                        Delivered::Fault(format!("{}; {e}", fault.says()))
                    }
                    acked => Delivered::Acked(acked),
                };
            };
            queue::check_blob(r, bytes).unwrap();
            let subject = r.txid.clone().unwrap();
            for topic in lands_in {
                self.conn
                    .execute(APPLIED_INSERT, [subject.as_str(), topic])
                    .unwrap();
            }
            if m.topics.iter().all(|t| lands_in.contains(&t.as_str())) {
                self.bucket.remove(&r.key);
                Delivered::Landed
            } else {
                Delivered::Fault("not durable".to_string())
            }
        }

        fn letters(&self) -> Vec<(String, String, String)> {
            self.conn
                .prepare("SELECT txid, status, class FROM mutation_dead_letters ORDER BY txid")
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        }
    }

    fn keyed(beef: &[u8], topics: &[String]) -> queue::MutationMessage {
        let queue::Carriage::R2(m) = queue::plan_replay(
            beef,
            topics,
            SubmitMode::HistoricalTx,
            queue::REPLAY_REASON_PHASE3_FAULT,
            queue::QUEUE_MESSAGE_ROOM,
            &SUBMIT_BEEF_LIMITS,
        )
        .unwrap() else {
            panic!("500 KB rides by key")
        };
        m
    }

    /// bsv-low #585, the d3 fold (the TWIN). Two identical 500 KB submissions are queued (a client's retry of a
    /// large JOIN): one object, two messages. The first lands and is acked, and the ack deletes the object. The
    /// second reads a MISSING object, finds its subject's applied rows in every topic it names, and is acked as a
    /// DUPE: no note, no letter. A THIRD message, naming a key whose bytes never landed, is the replay's FAULT and
    /// parks as a fault letter, as every missing object did before the fold. On `45aceff` the consumer has no
    /// verdict: the second twin is handed back, dead-lettered and parked as a fault letter that can never re-drive.
    #[test]
    fn e585_d3f_a_twin_whose_object_is_gone_is_a_dupe_and_an_unlanded_one_a_fault() {
        let topics = vec!["tm_a".to_string(), "tm_b".to_string()];
        let (beef, id) = shapes::sized_body(500_000);
        let (first, second) = (keyed(&beef, &topics), keyed(&beef, &topics));
        assert_eq!(first, second, "the same bytes, topics and mode: twins");
        let key = first.r2.as_ref().unwrap().key.clone();
        let mut c = Consumer {
            bucket: HashMap::new(),
            conn: db(),
        };
        // the door: each submission's write, then its send
        c.bucket.insert(key.clone(), beef.clone());
        c.bucket.insert(key.clone(), beef.clone());
        assert_eq!(c.bucket.len(), 1, "one object for the two");

        assert_eq!(c.deliver(&first, &["tm_a", "tm_b"]), Delivered::Landed);
        assert!(
            !c.bucket.contains_key(&key),
            "the ack deleted the object (the deletion rule stays on the ack)"
        );
        assert_eq!(
            c.deliver(&second, &[]),
            Delivered::Acked(queue::MissingVerdict::Twin),
            "the second twin is a dupe: on 45aceff it parks as a fault"
        );
        assert!(c.letters().is_empty(), "no letter for a twin");

        // a third message whose bytes never landed and whose object is gone (an operator's delete, a sweep)
        let (other, other_id) = shapes::sized_body(500_001);
        assert_ne!(other_id, id);
        let third = keyed(&other, &topics);
        let Delivered::Fault(fault) = c.deliver(&third, &[]) else {
            panic!("a missing object nothing shows landed is a fault")
        };
        assert!(
            fault.contains("MISSING") && fault.contains("holds no applied row in [tm_a,tm_b]"),
            "{fault}"
        );
        // noted and parked as the consumers do: a FAULT letter, never "not now"
        let r = third.r2.as_ref().unwrap();
        let (txid, tkey) = dead_letters::letter_key(&third, None);
        assert_eq!(txid, other_id);
        run(
            &c.conn,
            &dead_letters::note_failing_query(
                &txid,
                &tkey,
                &fault,
                10,
                dead_letters::LetterClass::Fault,
            ),
        );
        run(
            &c.conn,
            &dead_letters::park_query_r2(
                &txid,
                &tkey,
                &serde_json::to_string(&third).unwrap(),
                dead_letters::FAULT_UNRECORDED,
                0,
                20,
                Some((r.key.as_str(), r.bytes)),
            ),
        );
        assert_eq!(
            c.letters(),
            vec![(other_id.clone(), "parked".to_string(), "fault".to_string())]
        );

        // landed in ONE of its two topics is not landed: the other topic's write is still owed
        c.conn
            .execute(APPLIED_INSERT, [other_id.as_str(), "tm_a"])
            .unwrap();
        let Delivered::Fault(fault) = c.deliver(&third, &[]) else {
            panic!("half landed is a fault")
        };
        assert!(fault.contains("holds no applied row in [tm_b]"), "{fault}");
        // and once the rest landed (the client's re-presentation, a GASP peer), the letter's re-drive is a dupe
        c.conn
            .execute(APPLIED_INSERT, [other_id.as_str(), "tm_b"])
            .unwrap();
        assert_eq!(
            c.deliver(&third, &[]),
            Delivered::Acked(queue::MissingVerdict::Twin)
        );
    }

    /// The d3 fold: the verdict's other arms. An open eviction acks (the replay with its bytes is skipped there
    /// too, and that check reads the subject alone); a read that faults, and a message naming no subject, are
    /// never "landed". And the source shape: the consumer judges a MISSING object before it notes a fault, acks a
    /// twin without a note, counts each verdict apart, and the read is the storage's own table.
    #[test]
    fn e585_d3f_the_verdict_reads_the_ledger_first_and_a_faulted_read_is_never_landed() {
        use queue::{missing_verdict, MissingVerdict};
        let t = vec!["tm_a".to_string()];
        let all = Ok(vec!["tm_a".to_string(), "tm_other".to_string()]);
        assert_eq!(
            missing_verdict(Some("s"), &t, &Ok(false), &all),
            MissingVerdict::Twin
        );
        assert_eq!(
            missing_verdict(Some("s"), &t, &Ok(true), &Ok(vec![])),
            MissingVerdict::Evicted
        );
        assert_eq!(
            missing_verdict(Some("s"), &t, &Ok(true), &all),
            MissingVerdict::Evicted
        );
        for (subject, topics, evicted, applied) in [
            (None, &t, Ok(false), all.clone()),
            (Some("s"), &t, Err("d1 down".to_string()), all.clone()),
            (Some("s"), &t, Ok(false), Err("d1 down".to_string())),
            (Some("s"), &t, Ok(false), Ok(vec!["tm_other".to_string()])),
            (Some("s"), &Vec::new(), Ok(false), all.clone()),
        ] {
            assert!(
                matches!(
                    missing_verdict(subject, topics, &evicted, &applied),
                    MissingVerdict::Fault(_)
                ),
                "{subject:?} {topics:?} {evicted:?} {applied:?}"
            );
        }

        let code = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        assert!(
            include_str!("d1_storage.rs").contains(APPLIED_INSERT),
            "the model's landing is the storage's own statement"
        );
        let q = code(include_str!("queue.rs"));
        let start = q.find("pub async fn judge_missing(").unwrap();
        let f = &q[start..start + q[start..].find("\n}\n").unwrap()];
        assert!(f.contains("crate::admit_fast::open_eviction(db, subject)"));
        assert!(f.contains("Query::new(TWIN_APPLIED_SQL)"));
        assert!(f.contains("missing_verdict(Some(subject), topics, &evicted, &applied)"));
        let lib = code(include_str!("lib.rs"));
        let start = lib.find("async fn queue_handler(").unwrap();
        let h = &lib[start..start + lib[start..].find("\n}\n").unwrap()];
        let arm = h
            .find("Err(f @ crate::queue::BlobFault::Missing(_)) =>")
            .expect("the consumer judges a MISSING object");
        let end = arm + h[arm..].find("Err(f) => Err(f.says()),").unwrap();
        let arm = &h[arm..end];
        assert!(arm.contains("crate::queue::judge_missing(db, &body.topics, r).await"));
        assert!(arm.contains("crate::ops::COUNTER_QUEUE_R2_TWIN_ACKED"));
        assert!(arm.contains("crate::ops::COUNTER_QUEUE_R2_MISSING_FAULT"));
        assert!(arm.contains("crate::dead_letters::Resolved::Twin"));
        assert_eq!(arm.matches("msg.ack();").count(), 1);
        assert!(
            !arm.contains("note_failing") && !arm.contains("msg.retry()"),
            "a twin is acked with no note; the fault falls through to the one note below"
        );
        assert!(
            !arm.contains("body.r2.as_ref().map(|r| r.key.clone())"),
            "nothing of the message's to delete: its object is gone"
        );
        assert_eq!(
            crate::ops::COUNTER_QUEUE_R2_TWIN_ACKED,
            "queue_r2_twin_acked_total"
        );
        assert_eq!(
            crate::ops::COUNTER_QUEUE_R2_MISSING_FAULT,
            "queue_r2_missing_fault_total"
        );
    }
}
