//! NL-6: the P0-5f Worker door witnesses, inverted. A valid BEEF over each
//! former budget passes every pure Worker door; invalid bytes are refused
//! with the offset and the kind.
use crate::{dead_letters, ef, queue};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use beef_limits::*;
use overlay_engine::beef_limits;

#[path = "../../overlay-engine/tests/support/beef_doors.rs"]
mod shapes;

#[path = "../../overlay-engine/tests/support/walk_witness.rs"]
mod walk_witness;

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
    );
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
    );
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
        ),
        queue::Carriage::Inline(_)
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
        /// Read, replayed, durable: acked. `left`: its object was LEFT (the d3 fold-2, L1), else deleted.
        Landed { left: bool },
        /// Its object was MISSING and the verdict acked it: no note, no letter.
        Acked(queue::MissingVerdict),
        /// Handed back with this fault noted (it dead-letters and is parked).
        Fault(String),
    }

    /// The replay as the engine reports it, per topic the message names: already applied (a dupe), FAILED (the
    /// manager erred: no row, no fault), "not now" (`predecessor_not_landed`, a fault), or landed.
    #[derive(Clone, Copy)]
    struct Replay<'a> {
        fails: &'a [&'a str],
        not_now: bool,
    }
    const LANDS: Replay<'static> = Replay {
        fails: &[],
        not_now: false,
    };

    /// The platform under the consumer's read: a bucket and the storage's applied rows, read by the SHIPPED
    /// statement and judged by the SHIPPED verdict.
    struct Ports<'a> {
        bucket: &'a HashMap<String, Vec<u8>>,
        conn: &'a rusqlite::Connection,
    }

    impl queue::ReplayBytes for Ports<'_> {
        async fn read_beef(&self, r: &queue::BeefRef) -> Result<Vec<u8>, queue::BlobFault> {
            let bytes = self
                .bucket
                .get(&r.key)
                .ok_or_else(|| queue::BlobFault::Missing(r.key.clone()))?;
            queue::check_replay_blob(r, bytes).map_err(queue::BlobFault::Refused)?;
            Ok(bytes.clone())
        }

        async fn judge_missing(
            &self,
            topics: &[String],
            r: &queue::BeefRef,
        ) -> queue::MissingVerdict {
            let applied = r
                .txid
                .as_deref()
                .map_or_else(Vec::new, |t| applied(self.conn, t));
            queue::missing_verdict(r.txid.as_deref(), topics, &Ok(false), &Ok(applied))
        }
    }

    fn applied(conn: &rusqlite::Connection, txid: &str) -> Vec<String> {
        conn.prepare(queue::TWIN_APPLIED_SQL)
            .unwrap()
            .query_map([txid], |r| r.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    /// The consumer as `queue_handler` runs a message: the SHIPPED read and use of the verdict
    /// (`queue::read_for_replay`), the replay (its landing is the storage's applied rows), the SHIPPED ack rule
    /// (`queue::landed_ack`) and the notes by the SHIPPED statements, as the handler's arms call them.
    struct Consumer {
        bucket: HashMap<String, Vec<u8>>,
        conn: rusqlite::Connection,
    }

    impl Consumer {
        fn new() -> Self {
            Self {
                bucket: HashMap::new(),
                conn: db(),
            }
        }

        fn applied(&self, txid: &str) -> Vec<String> {
            applied(&self.conn, txid)
        }

        /// The door's write of a keyed message's object (`put_beef`), before its send.
        fn put(&mut self, m: &queue::MutationMessage, beef: &[u8]) {
            self.bucket
                .insert(m.r2.as_ref().unwrap().key.clone(), beef.to_vec());
        }

        fn note(
            &self,
            m: &queue::MutationMessage,
            subject: Option<&str>,
            fault: &str,
            class: Option<dead_letters::LetterClass>,
        ) {
            let (txid, topics) = dead_letters::letter_key(m, subject);
            let q = match class {
                Some(c) => dead_letters::note_failing_query(&txid, &topics, fault, 10, c),
                None => dead_letters::note_failing_keeping_class_query(&txid, &topics, fault, 10),
            };
            run(&self.conn, &q);
        }

        fn deliver(&mut self, m: &queue::MutationMessage, replay: Replay<'_>) -> Delivered {
            let step = block_on(queue::read_for_replay(
                &Ports {
                    bucket: &self.bucket,
                    conn: &self.conn,
                },
                m,
            ));
            let bytes = match step {
                queue::ReadStep::Bytes(b) => b,
                queue::ReadStep::Acked(v) => return Delivered::Acked(v),
                queue::ReadStep::Fault { fault, missing } => {
                    let class = (!missing).then_some(dead_letters::LetterClass::Fault);
                    self.note(m, None, &fault, class);
                    return Delivered::Fault(fault);
                }
            };
            let mut named = beef_limits::parse_beef(&bytes, &QUEUE_BEEF_LIMITS).unwrap();
            let subject = ef::subject_txid_of(&mut named).unwrap();
            let held = self.applied(&subject);
            let (mut applied, mut deduped) = (Vec::new(), Vec::new());
            for t in &m.topics {
                if held.contains(t) {
                    deduped.push(t.clone());
                } else if replay.fails.contains(&t.as_str()) {
                } else if replay.not_now {
                    let fault = format!("not durable: {t}/{}: not now", dead_letters::SITE_NOT_NOW);
                    self.note(
                        m,
                        Some(&subject),
                        &fault,
                        Some(dead_letters::LetterClass::of_sites([
                            dead_letters::SITE_NOT_NOW,
                        ])),
                    );
                    return Delivered::Fault(fault);
                } else {
                    self.conn
                        .execute(APPLIED_INSERT, [subject.as_str(), t.as_str()])
                        .unwrap();
                    applied.push(t.clone());
                }
            }
            let report = overlay_engine::engine::MutationReport {
                applied_topics: applied,
                deduped_topics: deduped,
                ..Default::default()
            };
            let ack = queue::landed_ack(m, &report, None);
            for k in &ack.delete {
                self.bucket.remove(k);
            }
            Delivered::Landed { left: ack.leaves }
        }

        /// The DLQ consumer's park of `m` (its key, no bytes read), by the shipped statement.
        fn park(&self, m: &queue::MutationMessage) {
            let (txid, tkey) = dead_letters::letter_key(m, None);
            let r = m.r2.as_ref().unwrap();
            run(
                &self.conn,
                &dead_letters::park_query_r2(
                    &txid,
                    &tkey,
                    &serde_json::to_string(m).unwrap(),
                    dead_letters::FAULT_UNRECORDED,
                    0,
                    20,
                    Some((r.key.as_str(), r.bytes)),
                ),
            );
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
        ) else {
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
        let mut c = Consumer::new();
        // the door: each submission's write, then its send
        c.put(&first, &beef);
        c.put(&second, &beef);
        assert_eq!(c.bucket.len(), 1, "one object for the two");

        assert_eq!(c.deliver(&first, LANDS), Delivered::Landed { left: false });
        assert!(
            !c.bucket.contains_key(&key),
            "the ack deleted the object (the deletion rule stays on the ack)"
        );
        assert_eq!(
            c.deliver(&second, LANDS),
            Delivered::Acked(queue::MissingVerdict::Twin),
            "the second twin is a dupe: on 45aceff it parks as a fault"
        );
        assert!(c.letters().is_empty(), "no letter for a twin");

        // a third message whose bytes never landed and whose object is gone (an operator's delete, a sweep)
        let (other, other_id) = shapes::sized_body(500_001);
        assert_ne!(other_id, id);
        let third = keyed(&other, &topics);
        let Delivered::Fault(fault) = c.deliver(&third, LANDS) else {
            panic!("a missing object nothing shows landed is a fault")
        };
        assert!(
            fault.contains("MISSING") && fault.contains("holds no applied row in [tm_a,tm_b]"),
            "{fault}"
        );
        // noted (no earlier note: a FAULT letter, an unknown is honest) and parked as the consumers do
        assert_eq!(dead_letters::letter_key(&third, None).0, other_id);
        c.park(&third);
        assert_eq!(
            c.letters(),
            vec![(other_id.clone(), "parked".to_string(), "fault".to_string())]
        );

        // landed in ONE of its two topics is not landed: the other topic's write is still owed
        c.conn
            .execute(APPLIED_INSERT, [other_id.as_str(), "tm_a"])
            .unwrap();
        let Delivered::Fault(fault) = c.deliver(&third, LANDS) else {
            panic!("half landed is a fault")
        };
        assert!(fault.contains("holds no applied row in [tm_b]"), "{fault}");
        // and once the rest landed (the client's re-presentation, a GASP peer), the letter's re-drive is a dupe
        c.conn
            .execute(APPLIED_INSERT, [other_id.as_str(), "tm_b"])
            .unwrap();
        assert_eq!(
            c.deliver(&third, LANDS),
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
        // the d3 fold-2 (L3): the read and the use of the verdict are `read_for_replay` (run natively by the pins
        // below); the handler only acts on its step
        let start = q.find("pub(crate) async fn read_for_replay<").unwrap();
        let f = &q[start..start + q[start..].find("\n}\n").unwrap()];
        assert!(f.contains(
            "Err(f @ BlobFault::Missing(_)) => match p.judge_missing(&body.topics, r).await"
        ));
        let lib = code(include_str!("lib.rs"));
        let start = lib.find("async fn queue_handler(").unwrap();
        let h = &lib[start..start + lib[start..].find("\n}\n").unwrap()];
        assert!(h.contains("crate::queue::read_for_replay(&ports, body).await"));
        assert!(
            !h.contains("judge_missing") && !h.contains("read_beef"),
            "the handler reads nothing itself"
        );
        let arm = h
            .find("crate::queue::ReadStep::Acked(verdict) => {")
            .expect("the handler acks only the verdict's ack");
        let end = arm
            + h[arm..]
                .find("crate::queue::ReadStep::Fault { fault, missing } => {")
                .unwrap();
        let fault_arm = &h[end..end + h[end..].find("msg.retry();").unwrap()];
        let arm = &h[arm..end];
        assert!(arm.contains("crate::ops::COUNTER_QUEUE_R2_TWIN_ACKED"));
        assert!(arm.contains("crate::dead_letters::Resolved::Twin"));
        assert_eq!(arm.matches("msg.ack();").count(), 1);
        assert!(
            !arm.contains("note_failing") && !arm.contains("msg.retry()"),
            "a twin is acked with no note"
        );
        assert!(
            !arm.contains("body.r2.as_ref().map(|r| r.key.clone())"),
            "nothing of the message's to delete: its object is gone"
        );
        assert!(
            !fault_arm.contains("msg.ack()")
                && fault_arm.contains("crate::ops::COUNTER_QUEUE_R2_MISSING_FAULT")
                && fault_arm.contains("crate::dead_letters::note_failing_keeping_class(db, body, None, &fault)"),
            "a fault is noted (a MISSING one keeping the letter's class, L4) and handed back, never acked"
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

    // ── bsv-low #585, the d3 fold-2 (the door 3 lens's M1, L1, L2, L3, L4) ────────────────────────────────────────

    /// E585-D3-M1 (#568): the door never refuses a body for its size. A VALID 500 KB BEEF whose replay is "not now"
    /// is carried by key under the SHIPPED policy (the route's `enqueue_replay` plans exactly this), the door's ack
    /// is the queued one (the route answers it with `X-Overlay-Mutation: queued`), its object is the bytes under the
    /// key; the consumer READS it, notes the not-now class, and the DLQ parks the letter with its key while the
    /// object stays at rest. A 500 KB body that lands is acked and its object deleted. RED on `bc32851`: the plan
    /// answered `Err("BEEF too large for the mutation queue's replay (500000 B > 90000 B, ...)")`, the route's 502.
    #[test]
    fn e585_d3f2_m1_a_500kb_not_now_body_is_queued_by_key_and_parked_with_its_bytes_at_rest() {
        let topics = vec!["tm_a".to_string()];
        let (beef, id) = shapes::sized_body(500_000);
        assert_eq!(
            QUEUE_BEEF_LIMITS.max_bytes, ENGINE_BEEF_LIMITS.max_bytes,
            "the queue parses what the engine admits"
        );
        let m = keyed(&beef, &topics);
        let r = m.r2.clone().unwrap();
        let sha = hex::encode(bsv_rs::primitives::hash::sha256(&beef));
        assert_eq!(r.key, queue::r2_key(&sha, &topics, "historical-tx"));
        assert_eq!((r.bytes, r.txid.as_deref()), (500_000, Some(id.as_str())));
        assert_eq!(
            queue::mutation_ack(false, Some(Ok(()))),
            queue::MutationAck::Queued,
            "the enqueue answered: the door acks queued"
        );
        let code = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let q = code(include_str!("queue.rs"));
        let start = q.find("pub async fn enqueue_replay(").unwrap();
        let f = &q[start..start + q[start..].find("\n}\n").unwrap()];
        assert!(f.contains("plan_replay(beef, topics, mode, REPLAY_REASON_PHASE3_FAULT, room)"));
        assert!(!f.contains("max_bytes") && !q.contains("too large for the mutation queue"));
        assert!(include_str!("routes.rs").contains(r#"h.set("X-Overlay-Mutation", "queued")"#));

        let mut c = Consumer::new();
        c.put(&m, &beef);
        let Delivered::Fault(fault) = c.deliver(
            &m,
            Replay {
                fails: &[],
                not_now: true,
            },
        ) else {
            panic!("the replay is not now")
        };
        assert!(fault.contains(dead_letters::SITE_NOT_NOW), "{fault}");
        c.park(&m);
        assert_eq!(
            c.letters(),
            vec![(id.clone(), "parked".to_string(), "not_now".to_string())]
        );
        let (key, bytes): (String, i64) = c
            .conn
            .query_row(
                "SELECT r2_key, r2_bytes FROM mutation_dead_letters",
                [],
                |x| Ok((x.get(0)?, x.get(1)?)),
            )
            .unwrap();
        assert_eq!((key.as_str(), bytes), (r.key.as_str(), 500_000));
        assert_eq!(
            c.bucket.get(&r.key),
            Some(&beef),
            "the parked letter's bytes are at rest in R2"
        );

        // another 500 KB body that lands: acked, its object deleted by the ack
        let (other, _) = shapes::sized_body(500_001);
        let o = keyed(&other, &topics);
        c.put(&o, &other);
        assert_eq!(c.deliver(&o, LANDS), Delivered::Landed { left: false });
        assert!(!c.bucket.contains_key(&o.r2.as_ref().unwrap().key));
        assert!(
            c.bucket.contains_key(&r.key),
            "the parked letter's object is untouched"
        );
    }

    /// E585-D3-L1: the lens's sequence. Twins M1 and M2 name [tm_a, tm_b]. M1 lands tm_a while tm_b's manager
    /// ERRS (a failed topic: no applied row, no fault, durable): acked, and its object LEFT. M2 reads the bytes and
    /// replays as M1 did (tm_a a dupe, tm_b failing again: durable, nothing written): acked, and THAT ack deletes the
    /// object. No letter. RED on `bc32851` (every durable ack deleted the object): M2 reads it MISSING, holds no
    /// applied row in [tm_b], and is a `fault` letter over bytes that landed.
    #[test]
    fn e585_d3f2_l1_a_twin_of_a_landing_with_a_failed_topic_reads_the_bytes() {
        let topics = vec!["tm_a".to_string(), "tm_b".to_string()];
        let (beef, id) = shapes::sized_body(500_000);
        let (m1, m2) = (keyed(&beef, &topics), keyed(&beef, &topics));
        let key = m1.r2.as_ref().unwrap().key.clone();
        let mut c = Consumer::new();
        c.put(&m1, &beef);
        c.put(&m2, &beef);
        let b_errs = Replay {
            fails: &["tm_b"],
            not_now: false,
        };
        let first = c.deliver(&m1, b_errs);
        assert_eq!(c.applied(&id), vec!["tm_a".to_string()]);
        let kept = c.bucket.contains_key(&key);
        assert_eq!(
            c.deliver(&m2, b_errs),
            Delivered::Landed { left: true },
            "M2 reads the bytes and acks as M1 did"
        );
        assert_eq!(first, Delivered::Landed { left: true });
        assert!(kept, "M1's ack left the object for its twin");
        // the d3 fold-3 (DELTA-L2): M2's tm_b failed again, so its ack LEAVES the object too (a third twin reads
        // it); nothing names it, so it is the sweep's after 8 days. On the fold-2 M2's ack deleted it.
        assert!(
            c.bucket.contains_key(&key),
            "M2's ack (tm_b failed again) leaves it"
        );
        assert!(c.letters().is_empty(), "no letter");
        // the rule itself: a landing with no failed topic and a dupe-only replay delete; any failed topic leaves
        let t = |v: &[&str]| v.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        assert!(!queue::landed_ack_leaves_object(
            &topics,
            &t(&["tm_a", "tm_b"]),
            &[]
        ));
        assert!(!queue::landed_ack_leaves_object(
            &topics,
            &t(&["tm_a"]),
            &t(&["tm_b"])
        ));
        assert!(!queue::landed_ack_leaves_object(
            &topics,
            &[],
            &t(&["tm_a", "tm_b"])
        ));
        assert!(queue::landed_ack_leaves_object(&topics, &[], &t(&["tm_a"])));
        assert!(queue::landed_ack_leaves_object(&topics, &[], &[]));
        assert!(queue::landed_ack_leaves_object(&topics, &t(&["tm_b"]), &[]));
    }

    /// E585-D3-DELTA-L2 (1), the d3 fold-3: twins M1 and M2 name [tm_a, tm_b] and EVERY topic's manager errs (a
    /// durable report, no applied row, no fault). M1 is acked and its object LEFT; M2 reads the bytes, fails both
    /// topics again and is acked with no letter; the object is the sweep's. RED with `4f592f0`'s rule
    /// (`!applied.is_empty() && ..`) grafted into `landed_ack_leaves_object`: M1's ack deletes the object, M2
    /// reads it MISSING, holds no applied row in either topic and is a `fault` letter for good.
    #[test]
    fn e585_d3f3_l2_a_twin_of_an_ack_whose_every_topic_failed_reads_the_bytes() {
        let topics = vec!["tm_a".to_string(), "tm_b".to_string()];
        let (beef, id) = shapes::sized_body(500_000);
        let (m1, m2) = (keyed(&beef, &topics), keyed(&beef, &topics));
        let key = m1.r2.as_ref().unwrap().key.clone();
        let mut c = Consumer::new();
        c.put(&m1, &beef);
        c.put(&m2, &beef);
        let all_err = Replay {
            fails: &["tm_a", "tm_b"],
            not_now: false,
        };
        assert_eq!(c.deliver(&m1, all_err), Delivered::Landed { left: true });
        assert!(c.applied(&id).is_empty(), "nothing landed");
        assert!(
            c.bucket.contains_key(&key),
            "M1's ack left the object for its twin"
        );
        assert_eq!(
            c.deliver(&m2, all_err),
            Delivered::Landed { left: true },
            "M2 reads the bytes and acks as M1 did"
        );
        assert!(c.letters().is_empty(), "no letter");
        // a transient manager error: a twin that then lands both topics deletes the object
        assert_eq!(c.deliver(&m2, LANDS), Delivered::Landed { left: false });
        assert!(!c.bucket.contains_key(&key));
    }

    /// E585-D3-DELTA-L1, the d3 fold-3: the `Landed` ack's decision over R2 is ONE function,
    /// `queue::landed_ack`, run here over every arm and called as is by `queue_handler` with the replay's own
    /// report. The delta lens's mutant G (`leaves` turned off) is RED in the first arm; its G2 (the report's
    /// `applied` and `deduped` swapped) is now an equivalent mutant (the rule reads both alike: a topic in neither
    /// failed) and cannot be written at the handler, which passes the report whole. RED on `4f592f0`: the handler
    /// calls no `landed_ack` (the function does not exist there; with it grafted into `queue.rs`, the source
    /// assertion fails).
    #[test]
    fn e585_d3f3_l1_the_landed_ack_is_one_decision_the_handler_calls_as_is() {
        let topics = vec!["tm_a".to_string(), "tm_b".to_string()];
        let (beef, _) = shapes::sized_body(500_000);
        let m = keyed(&beef, &topics);
        let key = m.r2.as_ref().unwrap().key.clone();
        let t = |v: &[&str]| v.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        let rep = |a: &[&str], d: &[&str]| overlay_engine::engine::MutationReport {
            applied_topics: t(a),
            deduped_topics: t(d),
            ..Default::default()
        };
        let row = || Some("mutations/row/obj".to_string());
        let leave = queue::LandedAck {
            leaves: true,
            delete: vec![],
        };
        let both = queue::LandedAck {
            leaves: false,
            delete: vec!["mutations/row/obj".to_string(), key.clone()],
        };
        assert_eq!(
            queue::landed_ack(&m, &rep(&["tm_a"], &[]), row()),
            leave,
            "tm_b failed: left"
        );
        assert_eq!(
            queue::landed_ack(&m, &rep(&[], &["tm_a"]), row()),
            leave,
            "deduped and failed: left"
        );
        assert_eq!(
            queue::landed_ack(&m, &rep(&[], &[]), row()),
            leave,
            "every topic failed: left"
        );
        assert_eq!(
            queue::landed_ack(&m, &rep(&["tm_a", "tm_b"], &[]), row()),
            both
        );
        assert_eq!(
            queue::landed_ack(&m, &rep(&["tm_b"], &["tm_a"]), row()),
            both
        );
        assert_eq!(
            queue::landed_ack(&m, &rep(&[], &["tm_a", "tm_b"]), row()),
            both,
            "a dupe deletes"
        );
        assert_eq!(
            queue::landed_ack(&m, &rep(&["tm_a", "tm_b"], &[]), None).delete,
            vec![key.clone()]
        );
        let inline = queue::MutationMessage {
            r2: None,
            ..m.clone()
        };
        assert_eq!(
            queue::landed_ack(&inline, &rep(&["tm_a"], &[]), row()),
            queue::LandedAck {
                leaves: false,
                delete: vec!["mutations/row/obj".to_string()],
            },
            "an inline message leaves nothing and deletes only its resolved row's object"
        );
        let squash = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<String>()
                .split_whitespace()
                .collect::<String>()
        };
        let lib = include_str!("lib.rs");
        let start = lib.find("async fn queue_handler(").unwrap();
        let h = squash(&lib[start..start + lib[start..].find("\n}\n").unwrap()]);
        let (call, extend, ack) = (
            h.find("letack=crate::queue::landed_ack(body,&report,row_object);")
                .expect("the handler calls the shipped decision with the replay's report"),
            h.find("acked_objects.extend(ack.delete);")
                .expect("and deletes what it says"),
            h.find("Resolved::Landed").unwrap(),
        );
        assert!(ack < call && call < extend);
        assert!(
            !h.contains("landed_ack_leaves_object"),
            "no second, inline use of the rule"
        );
    }

    /// E585-D3-L2: on the DLQ's last delivery a letter's object goes with it only on a DEFERRAL (a clean read said
    /// no row holds the key); a park whose read or statement FAULTED may have landed (#559 limit 5), so its object is
    /// LEFT to the sweep. RED on `bc32851`: every outcome that reached the LOST line deleted the object.
    #[test]
    fn e585_d3f2_l2_lost_deletes_the_object_only_on_a_deferral() {
        use dead_letters::{lost_deletes_object, Deferral, Parked};
        assert!(lost_deletes_object(&Ok(Parked::Ceiling(2000))));
        for d in [
            Deferral::NotNowShare(1000),
            Deferral::NotNowTxid(1),
            Deferral::NotNowDay(200),
        ] {
            assert!(lost_deletes_object(&Ok(Parked::NotNowBound(d))));
        }
        assert!(
            !lost_deletes_object(&Err("D1_ERROR: the park statement timed out".to_string())),
            "a faulted park keeps the object"
        );
        assert!(!lost_deletes_object(&Ok(Parked::Park(0, None))));
        assert!(!lost_deletes_object(&Ok(Parked::Redelivery)));
        let code = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let src = code(include_str!("dead_letters.rs"));
        let start = src.find("pub async fn park_batch(").unwrap();
        let f = &src[start..start + src[start..].find("\n}\n").unwrap()];
        let (decided, matched) = (
            f.find("let deferred = lost_deletes_object(&outcome);")
                .unwrap(),
            f.find("let fault = match outcome {").unwrap(),
        );
        assert!(decided < matched, "decided over the park's own outcome");
        let lost = f.find("if plan.last {").unwrap();
        let gate = f[lost..].find("if deferred {").unwrap() + lost;
        let delete = f[lost..].find("\"its dead letter is LOST\"").unwrap() + lost;
        assert!(gate < delete, "the LOST delete is under the deferral");
        assert_eq!(f.matches("\"its dead letter is LOST\"").count(), 1);
    }

    /// E585-D3-L3: the consumer's use of the verdict, RUN: `read_for_replay` is what `queue_handler` calls. A MISSING
    /// object the verdict does not show landed is the replay's FAULT (noted, handed back: a letter), never an ack;
    /// only Twin and Evicted ack. The lens's mutant C1 (call `judge_missing`, discard it, ack every missing object
    /// as a twin) passed every pin on `bc32851` (643/0); here it fails the first assertion.
    #[test]
    fn e585_d3f2_l3_an_unlanded_missing_object_is_a_fault_never_an_ack() {
        struct Fixed(Result<Vec<u8>, queue::BlobFault>, queue::MissingVerdict);
        impl queue::ReplayBytes for Fixed {
            async fn read_beef(&self, _: &queue::BeefRef) -> Result<Vec<u8>, queue::BlobFault> {
                self.0.clone()
            }
            async fn judge_missing(
                &self,
                _: &[String],
                _: &queue::BeefRef,
            ) -> queue::MissingVerdict {
                self.1.clone()
            }
        }
        let topics = vec!["tm_a".to_string()];
        let (beef, _) = shapes::sized_body(500_000);
        let m = keyed(&beef, &topics);
        let key = m.r2.as_ref().unwrap().key.clone();
        let missing = || Err(queue::BlobFault::Missing(key.clone()));
        let step = |p: Fixed| block_on(queue::read_for_replay(&p, &m));
        let unlanded = queue::MissingVerdict::Fault(
            "aa holds no applied row in [tm_a]: its bytes did not land".into(),
        );
        let queue::ReadStep::Fault {
            fault,
            missing: true,
        } = step(Fixed(missing(), unlanded))
        else {
            panic!("an unlanded missing object is acked: an ack over a dropped write")
        };
        assert!(
            fault.contains("MISSING") && fault.contains("did not land"),
            "{fault}"
        );
        for v in [queue::MissingVerdict::Twin, queue::MissingVerdict::Evicted] {
            assert_eq!(step(Fixed(missing(), v.clone())), queue::ReadStep::Acked(v));
        }
        assert_eq!(
            step(Fixed(Ok(beef.clone()), queue::MissingVerdict::Twin)),
            queue::ReadStep::Bytes(beef.clone()),
            "the object read: no verdict asked"
        );
        for f in [
            queue::BlobFault::Read("503".into()),
            queue::BlobFault::Refused("hashes to".into()),
        ] {
            assert!(matches!(
                step(Fixed(Err(f), queue::MissingVerdict::Twin)),
                queue::ReadStep::Fault { missing: false, .. }
            ));
        }
        let mut inline = message(b"not base64 at all");
        inline.beef_b64 = "%%".into();
        assert!(matches!(
            block_on(queue::read_for_replay(
                &Fixed(missing(), queue::MissingVerdict::Twin),
                &inline
            )),
            queue::ReadStep::Fault { missing: false, .. }
        ));
    }

    /// E585-D3-L4: the lens's sequence. A stranger's large "not now" letter L is deferred at a not-now bound (a
    /// `failing` note, class `not_now`, no bytes held). Seconds before L's last DLQ delivery he re-presents the same
    /// bytes: the door writes the key again and M2 is queued. L is LOST on its deferral and deletes the object. M2
    /// reads it MISSING, its subject unlanded: the replay's fault, and the note KEEPS the row's class. M2 parks as a
    /// `not_now` letter, inside the not-now bounds. RED on `bc32851` (the note wrote class `fault`): `fault`.
    #[test]
    fn e585_d3f2_l4_a_missing_object_keeps_a_not_now_letters_class() {
        let topics = vec!["tm_a".to_string()];
        let (beef, id) = shapes::sized_body(500_000);
        let (l, m2) = (keyed(&beef, &topics), keyed(&beef, &topics));
        let key = l.r2.as_ref().unwrap().key.clone();
        let mut c = Consumer::new();
        c.put(&l, &beef);
        let not_now = Replay {
            fails: &[],
            not_now: true,
        };
        for _ in 0..4 {
            assert!(matches!(c.deliver(&l, not_now), Delivered::Fault(_)));
        }
        assert_eq!(
            c.letters(),
            vec![(id.clone(), "failing".to_string(), "not_now".to_string())]
        );
        // the re-presentation's write, then L's LOST on its deferral
        c.put(&m2, &beef);
        assert!(dead_letters::lost_deletes_object(&Ok(
            dead_letters::Parked::NotNowBound(dead_letters::Deferral::NotNowDay(200))
        )));
        c.bucket.remove(&key);
        for _ in 0..4 {
            let Delivered::Fault(fault) = c.deliver(&m2, not_now) else {
                panic!("M2's object is missing and its subject unlanded")
            };
            assert!(fault.contains("MISSING"), "{fault}");
        }
        assert_eq!(
            c.letters(),
            vec![(id.clone(), "failing".to_string(), "not_now".to_string())],
            "the missing object did not promote the letter"
        );
        c.park(&m2);
        assert_eq!(
            c.letters(),
            vec![(id, "parked".to_string(), "not_now".to_string())]
        );
        // a fresh row (no earlier note) is a fault letter: an unknown is honest
        let (other, other_id) = shapes::sized_body(500_001);
        let o = keyed(&other, &topics);
        assert!(matches!(c.deliver(&o, LANDS), Delivered::Fault(_)));
        assert!(c
            .letters()
            .contains(&(other_id, "failing".to_string(), "fault".to_string())));
    }
}

// ── bsv-low #592: the queue replay under the engine's walk budget ────────────

mod e592 {
    use super::*;
    use async_trait::async_trait;
    use overlay_engine::builder::EngineBuilder;
    use overlay_engine::engine::{DoorBudget, WalkLimb};
    use overlay_engine::storage::memory::MemoryStorage;
    use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
    use overlay_engine::types::*;

    use super::walk_witness;

    struct AdmitOutputZero;

    #[async_trait(?Send)]
    impl TopicManager for AdmitOutputZero {
        async fn identify_admissible_outputs(
            &self,
            _: &bsv_rs::transaction::Transaction,
            _: &[u8],
            _: Option<&[u8]>,
            _: SubmitMode,
            _context: &TopicAdmittanceContext,
        ) -> Result<AdmittanceInstructions, TopicManagerError> {
            Ok(AdmittanceInstructions {
                outputs_to_admit: vec![0],
                coins_to_retain: vec![],
                coins_removed: None,
            })
        }
        async fn get_documentation(&self) -> String {
            "admits output 0".into()
        }
        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "admit-zero".into(),
                ..Default::default()
            }
        }
    }

    /// THE PIN (b), bsv-low #592: the witness (a valid BEEF whose unproven parent carries 1.9 MB of push-only
    /// unlocking script, the body that passed the gated door, broadcast and admitted, then killed the consumer's
    /// isolate on every redelivery) through the consumer's re-submit path as `queue_handler` runs it: the message
    /// the door enqueues (`plan_replay`, by key: 1.9 MB is past the inline room), its bytes read back by the
    /// consumer's own check (`replay_object`), the replay's mode (`replay_submit_mode`), a REAL engine's
    /// `submit_with_report` under the Worker's budget, then the ack decision (`is_durable`, `landed_ack`). The walk
    /// could not run (the memory limb); the replay is APPLIED and durable, so the handler acks it and deletes its
    /// object: no retry, no dead letter. The counter it bumps is `submit_engine_walk_over_memory_total`.
    /// RED on `54dbb16`: the engine there walks the parent with no charge (217,765,812 bytes natively, an isolate
    /// kill on Workers: the redelivery loop); natively it answers `Ok` with no word of the walk, and the field this
    /// pin reads does not exist there (it does not compile).
    #[test]
    fn e592_b_the_replay_of_the_witness_is_acked_as_applied_never_dead_lettered() {
        let (beef, subject) = walk_witness::witness();
        let topics = vec!["tm_test".to_string()];
        let queue::Carriage::R2(m) = queue::plan_replay(
            &beef,
            &topics,
            SubmitMode::HistoricalTx,
            queue::REPLAY_REASON_PHASE3_FAULT,
            queue::QUEUE_MESSAGE_ROOM,
        ) else {
            panic!("1.9 MB rides by key")
        };
        let r = m.r2.clone().unwrap();
        assert_eq!(r.txid.as_deref(), Some(subject.as_str()));
        queue::check_replay_blob(&r, &beef)
            .expect("the consumer's own check of the object's bytes");
        let mode = queue::replay_submit_mode(&m.mode);
        assert_eq!(mode, SubmitMode::HistoricalTx, "the replay walks");

        assert_eq!(crate::WORKER_WALK_BUDGET, DoorBudget::DEFAULT);
        let engine = EngineBuilder::new(Box::new(MemoryStorage::new()))
            .with_topic("tm_test", Box::new(AdmitOutputZero))
            .with_walk_budget(crate::WORKER_WALK_BUDGET)
            .build();
        let tagged = TaggedBEEF {
            beef: beef.clone(),
            topics: m.topics.clone(),
            off_chain_values: None,
        };
        let (_, report) = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(engine.submit_with_report(&tagged, mode))
            .expect("never a refusal for the walk's size");
        let stop = report
            .walk_could_not_run
            .clone()
            .expect("the walk could not run");
        assert_eq!(stop.subject_txid, subject);
        assert_eq!(stop.limb, WalkLimb::OverMemory);
        assert_eq!(
            crate::ops::engine_walk_over_counter(stop.limb),
            "submit_engine_walk_over_memory_total"
        );
        assert!(
            report.is_durable(),
            "the handler acks a durable report: no retry, no dead letter"
        );
        assert_eq!(report.applied_topics, topics, "acked as applied");
        let ack = queue::landed_ack(&m, &report, None);
        assert!(
            !ack.leaves,
            "every topic applied: the ack deletes the object"
        );
        assert_eq!(ack.delete, vec![r.key.clone()]);
    }

    /// The wiring (source shape), for (b) and (d): the queue consumer counts the replay's report right after its
    /// `submit_with_report`, the synchronous `/submit` (the ungated modes, under `SUBMIT_ENFORCE`'s operator bar)
    /// and `/admin/readmit` count theirs, the engine the Worker builds walks under the Worker's budget, and the
    /// gated door under the same instance. The route itself (an ungated `historical-tx` of the witness answering
    /// as before and bumping the counter) is the captain's route tier: not run in this lane.
    #[test]
    fn e592_d_every_door_that_walks_counts_the_engine_walk() {
        let lib = include_str!("lib.rs");
        let replay = lib
            .find("let replayed = engine.submit_with_report(&tagged_beef, mode).await;")
            .expect("the queue's replay");
        let note = lib[replay..]
            .find("crate::ops::note_engine_walk(db, report, \"Queue\")")
            .expect("the queue counts the replay's walk");
        assert!(note < 600, "right after the replay's submit");
        assert!(lib.contains("engine.set_walk_budget(WORKER_WALK_BUDGET);"));

        let routes = include_str!("routes.rs");
        let submit = routes
            .find("let (steak, mutation_report) = match engine.submit_with_report(&tagged_beef, mode).await {")
            .expect("the synchronous submit");
        assert!(routes[submit..]
            .contains("crate::ops::note_engine_walk(&db, &mutation_report, \"POST /submit\")"));
        assert!(
            routes.contains("crate::ops::note_engine_walk(&db, &report, \"POST /admin/readmit\")")
        );
        // The gated door walks under `DoorBudget::DEFAULT` (`verify_scripts_only`); the engine under the
        // Worker's instance, which is that budget: one budget for every walk of this Worker.
        assert!(routes.contains("engine.verify_scripts_only(&gated_beef, &subject_txid).await"));
        assert_eq!(crate::WORKER_WALK_BUDGET, DoorBudget::DEFAULT);

        // The counters are served from 0 and named by their limb.
        let ops = include_str!("ops.rs");
        assert!(ops.contains("COUNTER_SUBMIT_ENGINE_WALK_OVER_BUDGET: 0,"));
        assert!(ops.contains("COUNTER_SUBMIT_ENGINE_WALK_OVER_MEMORY: 0,"));
        assert_eq!(
            crate::ops::engine_walk_over_counter(WalkLimb::OverWork),
            "submit_engine_walk_over_budget_total"
        );
        assert_eq!(
            crate::ops::engine_walk_over_counter(WalkLimb::InterpreterMemory),
            "submit_engine_walk_over_budget_total"
        );
    }
}
