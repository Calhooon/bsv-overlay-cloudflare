//! bsv-low #911 (D2-ACTOR-A): the publish's pins over real SQLite with the overlay's migrations. Apart from
//! `owed_events.rs`, which the app layer compiles by path (these pins read the overlay's own modules).
#[cfg(test)]
mod pins {
    use crate::owed_events::*;
    use overlay_discovery::owed_event::{self, OwedEvent};
    use rusqlite::{types::ValueRef, Connection};
    use serde_json::{json, Value};
    use std::cell::RefCell;

    /// The publish's port over real SQLite with the overlay's migrations: one transaction per batch, every send
    /// recorded (or refused on demand).
    pub(crate) struct Sqlite {
        pub conn: Connection,
        pub sent: RefCell<Vec<String>>,
        pub refuse_sends: std::cell::Cell<bool>,
        pub now: std::cell::Cell<i64>,
    }

    impl Sqlite {
        pub fn migrated() -> Self {
            let conn = Connection::open_in_memory().unwrap();
            for sql in crate::d1::OVERLAY_MIGRATIONS {
                if let Err(e) = conn.execute_batch(sql) {
                    assert!(
                        crate::d1::migration_error_is_benign(sql, &e.to_string()),
                        "{sql}: {e}"
                    );
                }
            }
            Sqlite {
                conn,
                sent: RefCell::new(Vec::new()),
                refuse_sends: std::cell::Cell::new(false),
                now: std::cell::Cell::new(1_793_059_200_000),
            }
        }
        pub fn marks(&self) -> Vec<(String, String, String, String)> {
            let mut q = self
                .conn
                .prepare(
                    "SELECT identity,gameId,source,sourceKey FROM owed_dirty ORDER BY generation",
                )
                .unwrap();
            q.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        }
    }

    fn json_of(v: ValueRef<'_>) -> Value {
        match v {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(n) => json!(n),
            ValueRef::Real(f) => json!(f),
            ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
            ValueRef::Blob(b) => json!(hex::encode(b)),
        }
    }

    impl PublishPort for Sqlite {
        async fn run_reads(
            &self,
            stmts: Vec<(&'static str, Vec<Arg>)>,
        ) -> Result<Vec<Vec<Value>>, String> {
            let tx = self
                .conn
                .unchecked_transaction()
                .map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            for (sql, args) in stmts {
                let binds: Vec<rusqlite::types::Value> = args
                    .iter()
                    .map(|a| match a {
                        Arg::Text(t) => rusqlite::types::Value::Text(t.clone()),
                        Arg::Int(n) => rusqlite::types::Value::Integer(*n),
                        Arg::Null => rusqlite::types::Value::Null,
                    })
                    .collect();
                let mut q = tx.prepare(sql).map_err(|e| format!("{sql}: {e}"))?;
                let names: Vec<String> = q.column_names().iter().map(|s| s.to_string()).collect();
                let rows = q
                    .query_map(rusqlite::params_from_iter(binds.iter()), |r| {
                        let mut o = serde_json::Map::new();
                        for (i, n) in names.iter().enumerate() {
                            o.insert(n.clone(), json_of(r.get_ref(i)?));
                        }
                        Ok(Value::Object(o))
                    })
                    .map_err(|e| e.to_string())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())?;
                out.push(rows);
            }
            tx.commit().map_err(|e| e.to_string())?;
            Ok(out)
        }
        async fn send(&self, bodies: Vec<String>) -> Result<(), String> {
            if self.refuse_sends.get() {
                return Err("the queue refused the batch".into());
            }
            self.sent.borrow_mut().extend(bodies);
            Ok(())
        }
        fn now_ms(&self) -> i64 {
            self.now.get()
        }
    }

    pub(crate) fn run<F: std::future::Future>(f: F) -> F::Output {
        let mut f = std::pin::pin!(f);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        loop {
            if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    const ID: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const GAME: &str = "game";
    const POT: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const HOP: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    const SPENDER: &str = "3333333333333333333333333333333333333333333333333333333333333333";

    /// The subjects every trigger's write finds: a pot with its party, a hop with its marker, a stored owed row
    /// naming the spender's word.
    fn seed(db: &Sqlite) {
        db.conn.execute_batch(&format!(
            "INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES('{POT}',0,0,1);\
             INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt) VALUES('{ID}','opp','{GAME}','{POT}',0,1,'00','party0',0,1);\
             INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt) VALUES('{ID}','opp','{GAME}',1,1000,'k','s','i',2,'{HOP}',0,1);\
             INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs) VALUES('{ID}','{HOP}:1','hop-stranded','{GAME}','{{}}',1);\
             INSERT INTO owed_row_words(identity,outpoint,role,txid) VALUES('{ID}','{HOP}:1','spender','{SPENDER}');\
             DELETE FROM owed_dirty;"
        )).unwrap();
    }

    /// Each of the 29 triggers by name: the write that fires it and the pair its writer notes (what production notes:
    /// `pot_changes::note` for `pot`, the stores' and passes' `owed_events::note` for the rest).
    fn write_of(trigger: &str) -> Option<(String, &'static str, String)> {
        let pot = format!("{POT}:0");
        let hop = format!("{HOP}:1");
        Some(match trigger {
            "owed_dirty_pot_records_insert" => (format!("DELETE FROM pot_records; INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES('{POT}',0,0,1)"), "pot", pot),
            "owed_dirty_pot_records_update" => (format!("UPDATE pot_records SET spent=1,spendingTxid='{SPENDER}' WHERE txid='{POT}'"), "pot", pot),
            "owed_dirty_pot_records_delete" => (format!("DELETE FROM pot_records WHERE txid='{POT}'"), "pot", pot),
            "owed_dirty_potparty_records_insert" => (format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt) VALUES('{ID}','opp','{GAME}','{POT}',0,1,'00','party1',0,1)"), "party", "party1".into()),
            "owed_dirty_potparty_records_update" => ("UPDATE potparty_records SET sigValid=1 WHERE txid='party0'".into(), "party", "party0".into()),
            "owed_dirty_potparty_records_delete" => ("DELETE FROM potparty_records WHERE txid='party0'".into(), "party", "party0".into()),
            "owed_dirty_hopparty_records_insert" => (format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt) VALUES('{ID}','opp','{GAME}',1,1000,'k','s','i',2,'{HOP}',1,1)"), "hop", hop),
            "owed_dirty_hopparty_records_update" => (format!("UPDATE hopparty_records SET markerValid=1 WHERE txid='{HOP}'"), "hop", hop),
            "owed_dirty_hopparty_records_delete" => (format!("DELETE FROM hopparty_records WHERE txid='{HOP}'"), "hop", hop),
            "owed_dirty_potrefund_records_insert" => (format!("INSERT INTO potrefund_records(identity,gameId,potTxid,potVout,txid,outputIndex,createdAt) VALUES('{ID}','{GAME}','{POT}',0,'refund0',0,1)"), "refund", "refund0".into()),
            "owed_dirty_potrefund_records_update" => (format!("INSERT INTO potrefund_records(identity,gameId,potTxid,potVout,txid,outputIndex,createdAt) VALUES('{ID}','{GAME}','{POT}',0,'refund0',0,1); DELETE FROM owed_dirty; UPDATE potrefund_records SET refundValid=1 WHERE txid='refund0'"), "refund", "refund0".into()),
            "owed_dirty_result_markers_v2_insert" => (format!("INSERT INTO result_markers_v2(gameId,winner,loser,txid,outputIndex,createdAt) VALUES('{GAME}','{ID}','opp','result0',0,1)"), "result", "result0".into()),
            "owed_dirty_collected_markers_v2_insert" => (format!("INSERT INTO collected_markers_v2(identity,gameId,txid,outputIndex,createdAt) VALUES('{ID}','{GAME}','collected0',0,1)"), "collected", "collected0".into()),
            "owed_dirty_hopsweep_records_insert" => (format!("INSERT INTO hopsweep_records(identity,gameId,hopTxid,hopVout,sweepTxid,sweepRawHex,txid,outputIndex,createdAt) VALUES('{ID}','{GAME}','{HOP}',1,'sw','00','sweep0',0,1)"), "sweep", "sweep0".into()),
            "owed_dirty_pot_evictions_insert" => (format!("INSERT INTO pot_evictions(txid,reason,evictedAt) VALUES('{POT}','r',1)"), "eviction", POT.into()),
            "owed_dirty_pot_evictions_update" => (format!("INSERT INTO pot_evictions(txid,reason,evictedAt) VALUES('{POT}','r',1); DELETE FROM owed_dirty; UPDATE pot_evictions SET readmittedAt=2 WHERE txid='{POT}'"), "eviction", POT.into()),
            "owed_dirty_network_seen_insert" => (format!("INSERT INTO network_seen(txid,seenAt) VALUES('{POT}',1)"), "seen", POT.into()),
            "owed_dirty_tx_words_insert" | "owed_dirty_tx_words_insert_links" => (format!("INSERT INTO tx_words(txid,word,tier,updated_at) VALUES('{POT}','seen','hint',1)"), "word", POT.into()),
            "owed_dirty_tx_words_update" | "owed_dirty_tx_words_update_links" => (format!("INSERT INTO tx_words(txid,word,tier,updated_at) VALUES('{POT}','seen','hint',1); DELETE FROM owed_dirty; UPDATE tx_words SET word='mined',height=5 WHERE txid='{POT}'"), "word", POT.into()),
            "owed_dirty_pot_beefs_insert" => (format!("INSERT INTO pot_beefs(txid,beef,createdAt) VALUES('{POT}',x'00',1)"), "beef", POT.into()),
            "owed_dirty_pot_beefs_update" => (format!("INSERT INTO pot_beefs(txid,beef,createdAt) VALUES('{POT}',x'00',1); DELETE FROM owed_dirty; UPDATE pot_beefs SET proof_verified=1 WHERE txid='{POT}'"), "beef", POT.into()),
            "owed_dirty_submit_refusals_insert" => (format!("INSERT INTO submit_refusals(hopTxid,hopVout,refusedTxid,reason,refusedAt) VALUES('{HOP}',1,'x','r',1)"), "refusal", hop),
            "owed_dirty_submit_refusals_update" => (format!("INSERT INTO submit_refusals(hopTxid,hopVout,refusedTxid,reason,refusedAt) VALUES('{HOP}',1,'x','r',1); DELETE FROM owed_dirty; UPDATE submit_refusals SET reason='again' WHERE hopTxid='{HOP}'"), "refusal", hop),
            "owed_dirty_submit_refusals_delete" => (format!("INSERT INTO submit_refusals(hopTxid,hopVout,refusedTxid,reason,refusedAt) VALUES('{HOP}',1,'x','r',1); DELETE FROM owed_dirty; DELETE FROM submit_refusals WHERE hopTxid='{HOP}'"), "refusal", hop),
            "owed_dirty_released_hop_evidence_insert" => (format!("INSERT INTO released_hop_evidence(hopTxid,hopVout,identity,gameId,potTxid,seatSettlePubkey,hopSats,evictedAt) VALUES('{HOP}',1,'{ID}','{GAME}','{POT}','k',1000,1)"), "hop", hop),
            "owed_dirty_released_hop_evidence_missing_insert" => (format!("INSERT INTO released_hop_evidence_missing(identity,gameId,hopTxid,hopVout,potTxid,evictedAt,bytesRead) VALUES('{ID}','{GAME}','{HOP}',1,'{POT}',1,0)"), "hop", hop),
            "owed_dirty_owed_spender_bytes_insert" => (format!("INSERT INTO owed_spender_bytes(txid,bytesJson,createdAtMs) VALUES('{SPENDER}','{{}}',1)"), "word", SPENDER.into()),
            // FIX-1001 (bsv-low #1001): a filed sweep's proof, stitched (INSERT OR REPLACE) and latched in place; a hop
            // row's confirmation for its markerless filer
            "owed_dirty_transactions_proof_insert" => (format!("INSERT INTO hopsweep_records(identity,gameId,hopTxid,hopVout,sweepTxid,sweepRawHex,txid,outputIndex,createdAt) VALUES('{ID}','{GAME}','{HOP}',1,'{SPENDER}','00','sweep0',0,1); DELETE FROM owed_dirty; INSERT OR REPLACE INTO transactions(txid,beef,has_proof,proofHeight) VALUES('{SPENDER}',x'00',1,5)"), "beef", SPENDER.into()),
            "owed_dirty_transactions_proof_update" => (format!("INSERT INTO hopsweep_records(identity,gameId,hopTxid,hopVout,sweepTxid,sweepRawHex,txid,outputIndex,createdAt) VALUES('{ID}','{GAME}','{HOP}',1,'{SPENDER}','00','sweep0',0,1); INSERT INTO transactions(txid,beef,has_proof) VALUES('{SPENDER}',x'00',0); DELETE FROM owed_dirty; UPDATE transactions SET has_proof=1,proofHeight=5 WHERE txid='{SPENDER}'"), "beef", SPENDER.into()),
            "owed_dirty_pot_records_update_filers" => (format!("DELETE FROM hopparty_records; INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,createdAt) VALUES('{HOP}',1,1,'{SPENDER}',1); INSERT INTO hopsweep_records(identity,gameId,hopTxid,hopVout,sweepTxid,sweepRawHex,txid,outputIndex,createdAt) VALUES('{ID}','{GAME}','{HOP}',1,'{SPENDER}','00','sweep0',0,1); DELETE FROM owed_dirty; UPDATE pot_records SET spentConfirmed=1,spentHeight=5 WHERE txid='{HOP}'"), "pot", format!("{HOP}:1")),
            // FIX-1010 (bsv-low #1010): the families no trigger marked, and a party's other seats
            "owed_dirty_hand_markers_insert" => (format!("INSERT INTO hand_markers(gameId,identity,potTxid,cardsHex,txid,outputIndex,createdAt) VALUES('{GAME}','{ID}','{POT}','c','hand0',0,1)"), "hand", "hand0".into()),
            "owed_dirty_hand_markers_update" => (format!("INSERT INTO hand_markers(gameId,identity,potTxid,cardsHex,txid,outputIndex,createdAt) VALUES('{GAME}','{ID}','{POT}','c','hand0',0,1); DELETE FROM owed_dirty; UPDATE hand_markers SET rowValid=1 WHERE txid='hand0'"), "hand", "hand0".into()),
            "owed_dirty_proof_markers_insert" => (format!("INSERT INTO proof_markers(gameId,winner,sigHex,bundle,txid,outputIndex,createdAt) VALUES('{GAME}','{ID}','s',x'00','proof0',0,1)"), "proof", "proof0".into()),
            "owed_dirty_proof_posts_insert" => (format!("INSERT OR REPLACE INTO proof_posts(gameId,winner,sigHex,bundle,createdAt,bundleValid) VALUES('{GAME}','{ID}','s',x'00',1,1)"), "proof", GAME.into()),
            "owed_dirty_result_markers_v2_update" => (format!("INSERT INTO result_markers_v2(gameId,winner,loser,txid,outputIndex,createdAt) VALUES('{GAME}','{ID}','opp','result0',0,1); DELETE FROM owed_dirty; UPDATE result_markers_v2 SET claimValid=2 WHERE txid='result0'"), "result", "result0".into()),
            "owed_dirty_transactions_delete" => (format!("INSERT INTO hopsweep_records(identity,gameId,hopTxid,hopVout,sweepTxid,sweepRawHex,txid,outputIndex,createdAt) VALUES('{ID}','{GAME}','{HOP}',1,'{SPENDER}','00','sweep0',0,1); INSERT INTO transactions(txid,beef,has_proof) VALUES('{SPENDER}',x'00',1); DELETE FROM owed_dirty; DELETE FROM transactions WHERE txid='{SPENDER}'"), "beef", SPENDER.into()),
            "owed_dirty_potparty_records_insert_seats" => (format!("UPDATE pot_records SET pubA='k',pubB='z' WHERE txid='{POT}'; UPDATE potparty_records SET seatSettlePubkey='k',sigValid=1 WHERE txid='party0'; DELETE FROM owed_dirty; INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,seatSettlePubkey,txid,outputIndex,createdAt) VALUES('opp','{ID}','{GAME}','{POT}',0,1,'00','z','party1',0,1)"), "party", "party1".into()),
            "owed_dirty_potparty_records_update_seats" => (format!("UPDATE pot_records SET pubA='k',pubB='z' WHERE txid='{POT}'; UPDATE potparty_records SET seatSettlePubkey='k',sigValid=1 WHERE txid='party0'; INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,seatSettlePubkey,txid,outputIndex,createdAt) VALUES('opp','{ID}','{GAME}','{POT}',0,1,'00','z','party1',0,1); DELETE FROM owed_dirty; UPDATE potparty_records SET sigValid=1 WHERE txid='party1'"), "party", "party1".into()),
            "owed_dirty_pot_records_insert_seats" => (format!("DELETE FROM pot_records; DELETE FROM owed_dirty; INSERT INTO pot_records(txid,outputIndex,spent,createdAt,pubA,pubB) VALUES('{POT}',0,0,1,'k','z')"), "pot", pot),
            "owed_dirty_pot_records_update_seats" => (format!("UPDATE pot_records SET pubA='k',pubB='z' WHERE txid='{POT}'"), "pot", pot),
            "owed_dirty_pot_records_delete_seats" => (format!("UPDATE pot_records SET pubA='k',pubB='z' WHERE txid='{POT}'; DELETE FROM owed_dirty; DELETE FROM pot_records WHERE txid='{POT}'"), "pot", pot),
            "owed_dirty_pot_beefs_insert_seats" => (format!("UPDATE pot_records SET pubA='k',pubB='z' WHERE txid='{POT}'; DELETE FROM owed_dirty; INSERT INTO pot_beefs(txid,beef,createdAt) VALUES('{POT}',x'00',1)"), "beef", POT.into()),
            "owed_dirty_tx_words_delete" => (format!("INSERT INTO tx_words(txid,word,tier,updated_at) VALUES('{SPENDER}','mined','chain',1); DELETE FROM owed_dirty; DELETE FROM tx_words WHERE txid='{SPENDER}'"), "word", SPENDER.into()),
            "owed_dirty_owed_spender_bytes_delete" => (format!("INSERT INTO owed_spender_bytes(txid,bytesJson,createdAtMs) VALUES('{SPENDER}','{{}}',1); DELETE FROM owed_dirty; DELETE FROM owed_spender_bytes WHERE txid='{SPENDER}'"), "bytes", SPENDER.into()),
            _ => return None,
        })
    }

    /// The 46 triggers, read from the shipped source at test time (never a hand-fed list): each trigger's name.
    fn shipped_triggers() -> Vec<String> {
        let src = include_str!("derived_owed_triggers.rs");
        let mut out = Vec::new();
        for (i, _) in src.match_indices("trigger!(") {
            let rest = &src[i + "trigger!(".len()..];
            let mut parts = rest.splitn(3, ',');
            let table = parts.next().unwrap().trim().trim_matches('"').to_string();
            let name = parts.next().unwrap().trim().to_string();
            if table.starts_with('$') {
                continue; // the macro's own definition
            }
            out.push(format!("owed_dirty_{table}_{name}"));
        }
        out
    }

    /// (a) EVERY TRIGGER'S PAIR, NOTED, PUBLISHES ONE EVENT WITH THE ROW'S OWN FACTS. The 29 triggers are read from
    /// `derived_owed_triggers.rs`; each one's write runs under real SQLite with the shipped migrations, the pair a
    /// writer of that table notes (hand-written here) is published through the shipped [`publish_with`], and the pin
    /// holds: that pair is exactly the pair the trigger marked (so the consumer's routing read finds the marks), ONE
    /// event is sent for it, and it carries the marks and the row's own facts read in the publish's batch. It proves
    /// the spelling and the publish, NOT that a production writer notes (the D2-ACTOR-A lens's MEDIUM-3): that is
    /// [`d2_actor_a_fold_m3_every_production_writer_of_a_trigger_table_notes_its_pair`].
    #[test]
    fn d2_actor_a_a_each_trigger_pair_noted_publishes_one_event_with_its_rows_facts() {
        let triggers = shipped_triggers();
        assert_eq!(triggers.len(), 46, "the trigger list: {triggers:?}");
        for trigger in &triggers {
            let (sql, source, key) = write_of(trigger)
                .unwrap_or_else(|| panic!("{trigger}: no writer's note is pinned for it"));
            assert!(
                owed_event::TRIGGER_SOURCES
                    .iter()
                    .any(|(t, s)| *s == source && trigger.contains(t)),
                "{trigger}: source {source} is not its table's"
            );
            let db = Sqlite::migrated();
            seed(&db);
            db.conn.execute_batch(&sql).unwrap();
            let marked: std::collections::BTreeSet<(String, String)> =
                db.marks().into_iter().map(|(_, _, s, k)| (s, k)).collect();
            assert!(
                marked.contains(&(source.to_string(), key.clone())),
                "{trigger}: the trigger marked {marked:?}, the writer notes ({source}, {key})"
            );
            assert_eq!(
                marked.len(),
                1,
                "{trigger}: one pair per write, the writer's: {marked:?}"
            );
            let report = run(publish_with(
                &db,
                vec![Noted {
                    source: source.into(),
                    key: key.clone(),
                    held: None,
                }],
            ));
            assert_eq!(report.fault, None, "{trigger}");
            assert_eq!(report.sent.len(), 1, "{trigger}: one event");
            let sent = OwedEvent::decode(&db.sent.borrow()[0]).unwrap();
            assert_eq!(
                (sent.source.as_str(), sent.source_key.as_str()),
                (source, key.as_str())
            );
            assert!(sent.facts["marks"].as_i64().unwrap() >= 1, "{trigger}");
            if !trigger.contains("_delete") && !trigger.contains("missing") && source != "word" {
                assert!(
                    !sent.facts["row"].is_null(),
                    "{trigger}: the row's facts: {}",
                    sent.facts
                );
            }
        }
    }

    /// FIX-1010 (bsv-low #1010; the D2-ACTOR-F lens's LOW-1): THE PROBE ROWS' WRITERS MARK IN THEIR BATCH AND FRAME.
    /// The overlay's reorg clear and TTL sweep, each run as its batch runs it (the marks, then the framed delete) on the
    /// real record: the clear marks the hop's marker for its confirmed evidence and memo, the pot's party for the
    /// pot's memo and the spender's owed rows for a proof latch, each a downgrade; the sweep marks the evidence of an
    /// evicted hop alone (an aged memo no judge reads marks nothing); the keys the delete answers are the pairs the
    /// writer notes, and each publishes one event. RED with the marks dropped from the batch (the shipped clear alone
    /// marks nothing: `left: 0`).
    #[test]
    fn fix1010_a_the_probe_writers_mark_in_their_batch_and_frame() {
        use crate::hop_probe_memos::*;
        let run_batch = |db: &Sqlite, mark: &str, framed: &str, bind: Option<i64>| -> Vec<String> {
            let tx = db.conn.unchecked_transaction().unwrap();
            match bind {
                Some(b) => tx.execute(mark, [b]).unwrap(),
                None => tx.execute(mark, []).unwrap(),
            };
            let mut q = tx.prepare(framed).unwrap();
            let keys: Vec<String> = match bind {
                Some(b) => q
                    .query_map([b], |r| r.get(0))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect(),
                None => q
                    .query_map([], |r| r.get(0))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect(),
            };
            drop(q);
            tx.commit().unwrap();
            keys
        };
        let latch = format!("proofmined:{SPENDER}.5");
        let plant = |db: &Sqlite, key: &str, at: i64, confirmed: i64| {
            db.conn
                .execute(
                    "INSERT INTO hop_chain_probes(outpoint,probedAtMs,spent,spendingTxid,spentConfirmed) VALUES(?1,?2,1,?3,?4)",
                    rusqlite::params![key, at, SPENDER, confirmed],
                )
                .unwrap();
        };
        let marks = |db: &Sqlite| -> Vec<(String, String, String, i64)> {
            let mut q = db
                .conn
                .prepare("SELECT identity,source,sourceKey,downgrade FROM owed_dirty ORDER BY identity,sourceKey")
                .unwrap();
            q.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        // the clear
        let db = Sqlite::migrated();
        seed(&db);
        let evidence = format!("{HOP}:1");
        let memo = format!("{POT}.0");
        plant(&db, &evidence, 1, 1);
        plant(&db, &memo, 1, 1);
        plant(&db, &latch, 1, 1);
        plant(&db, &format!("{HOP}.1"), 1, 0); // unconfirmed: the clear keeps it
        assert_eq!(
            db.conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, []).unwrap(),
            3
        );
        assert_eq!(db.marks().len(), 0, "the shipped clear alone marks nothing");
        let db = Sqlite::migrated();
        seed(&db);
        plant(&db, &evidence, 1, 1);
        plant(&db, &memo, 1, 1);
        plant(&db, &latch, 1, 1);
        plant(&db, &format!("{HOP}.1"), 1, 0);
        let mut deleted = run_batch(
            &db,
            HOP_PROBE_MEMO_REORG_CLEAR_MARK_SQL,
            HOP_PROBE_MEMO_REORG_CLEAR_FRAMED_SQL,
            None,
        );
        deleted.sort();
        let mut want = vec![evidence.clone(), memo.clone(), latch.clone()];
        want.sort();
        assert_eq!(deleted, want);
        assert_eq!(
            marks(&db),
            vec![
                (ID.to_string(), "probe".to_string(), memo.clone(), 1),
                (ID.to_string(), "probe".to_string(), evidence.clone(), 1),
                (ID.to_string(), "probe".to_string(), latch.clone(), 1),
            ],
            "the hop's marker, the pot's party, the spender's owed row: each a downgrade"
        );
        let noted: Vec<Noted> = deleted
            .iter()
            .filter(|k| noted_key(false, k))
            .map(|k| Noted {
                source: PROBE_SOURCE.into(),
                key: k.clone(),
                held: None,
            })
            .collect();
        assert_eq!(noted.len(), 3);
        let report = run(publish_with(&db, noted));
        assert_eq!(
            (report.fault, report.sent.len()),
            (None, 3),
            "one event a key"
        );
        // the sweep: an evicted hop's evidence (no index row for it) and an aged memo
        let db = Sqlite::migrated();
        seed(&db);
        plant(&db, &evidence, 1, 1);
        plant(&db, &format!("{HOP}.1"), 1, 0);
        plant(&db, &latch, 1, 1); // a latch: never swept
        let deleted = run_batch(
            &db,
            HOP_PROBE_MEMO_EXPIRE_MARK_SQL,
            HOP_PROBE_MEMO_EXPIRE_FRAMED_SQL,
            Some(10),
        );
        assert_eq!(deleted.len(), 2, "{deleted:?}");
        assert_eq!(
            marks(&db),
            vec![(ID.to_string(), "probe".to_string(), evidence.clone(), 1)],
            "only the evidence a judge still reads is marked"
        );
        let noted: Vec<&String> = deleted.iter().filter(|k| noted_key(true, k)).collect();
        assert_eq!(noted, vec![&evidence]);
        // evidence whose index row stands is never swept, so never marked
        let db = Sqlite::migrated();
        seed(&db);
        db.conn
            .execute_batch(&format!(
                "INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES('{HOP}',1,0,1)"
            ))
            .unwrap();
        db.conn.execute("DELETE FROM owed_dirty", []).unwrap();
        plant(&db, &evidence, 1, 1);
        assert!(run_batch(
            &db,
            HOP_PROBE_MEMO_EXPIRE_MARK_SQL,
            HOP_PROBE_MEMO_EXPIRE_FRAMED_SQL,
            Some(10)
        )
        .is_empty());
        assert!(marks(&db).is_empty());
    }

    /// The text of the function whose signature starts with `sig` in `src`: from the signature to the first line that
    /// closes it at its own indentation (whitespace squashed).
    fn fn_body(src: &str, sig: &str) -> String {
        let at = src
            .find(sig)
            .unwrap_or_else(|| panic!("no writer `{sig}`"));
        let line_start = src[..at].rfind('\n').map_or(0, |i| i + 1);
        let indent = &src[line_start..at];
        let indent: String = indent.chars().take_while(|c| *c == ' ').collect();
        let close = format!("\n{indent}}}\n");
        let end = src[at..]
            .find(&close)
            .map_or(src.len(), |i| at + i + close.len());
        src[at..end].split_whitespace().collect()
    }

    /// MEDIUM-3 (the D2-ACTOR-A lens): EVERY PRODUCTION WRITER OF A TRIGGER TABLE NOTES ITS PAIR, by source. Each
    /// writer function named here (the lens's enumeration, both Workers) holds its note; a writer that stops noting is
    /// RED here (the lens's mutant, the `seen` latch's note removed at `ops.rs`, passed every pin before: now `seen:
    /// latch_network_seen does not note`). Outside the contract, stated: the backfill's evidence writes (a scheduled
    /// pass; the sweep), the refusal ledger's 48 h GC delete (outside the judge's window), the eviction's
    /// missing-evidence arm (its subjects are the `eviction` event's), the belt's own find (`owed_spender_bytes` and
    /// the probe memo in the judge's own batch, which acknowledges them) and the tip's marks (`internal_events`
    /// publishes `tip`); every one of them is swept within a tick.
    #[test]
    fn d2_actor_a_fold_m3_every_production_writer_of_a_trigger_table_notes_its_pair() {
        let disc = include_str!("d1_discovery.rs");
        let disc = &disc[..disc.find("#[cfg(test)]\nmod").unwrap_or(disc.len())];
        let app = |f: &str| -> &'static str {
            match f {
                "record_post" => include_str!("../../low-app-layer/src/record_post.rs"),
                "owed_write" => include_str!("../../low-app-layer/src/record_post/owed_write.rs"),
                "handfile" => include_str!("../../low-app-layer/src/handfile.rs"),
                "runtime" => include_str!("../../low-app-layer/src/tracker_host/runtime.rs"),
                "routes" => include_str!("../../low-app-layer/src/routes.rs"),
                _ => unreachable!(),
            }
        };
        let writers: Vec<(&str, &str, &str, &str)> = vec![
            ("pot", include_str!("pot_changes.rs"), "pub fn note(", r#"crate::owed_events::note("pot","#),
            ("party", disc, "pub fn potparty_insert_query(", r#"note("party","#),
            ("party", disc, "pub fn potparty_relatch_query(", r#"note("party","#),
            ("hop", disc, "pub fn hopparty_insert_query(", r#"note("hop","#),
            ("hop", disc, "pub fn hopparty_relatch_query(", r#"note("hop","#),
            ("refund", disc, "async fn store_record(&self, record: &PotrefundRecord)", "SOURCE_REFUND"),
            ("result", disc, "pub fn result_insert_query(", r#"note("result","#),
            ("result", disc, "pub fn result_relatch_query(", r#"note("result","#),
            ("collected", disc, "async fn store_record(&self, record: &CollectedRecord)", "SOURCE_COLLECTED"),
            ("beef", disc, "async fn unlatch_pot_beef_proof(", r#"note("beef","#),
            ("beef", disc, "async fn store_beef(", r#"note("beef","#),
            ("beef", disc, "async fn compact_pot_beef(", r#"note("beef","#),
            ("beef", disc, "async fn mark_pot_beef_proven_at(", r#"note("beef","#),
            ("beef", disc, "async fn mark_pot_beefs_proven_at(", r#"note("beef","#),
            ("beef", disc, "async fn mark_pot_beef_proven(&self", r#"note("beef","#),
            ("beef", disc, "async fn mark_pot_beefs_proven(&self", r#"note("beef","#),
            ("eviction", include_str!("admit_fast.rs"), "async fn evict_core_with_abort(", r#"note("eviction","#),
            ("eviction", include_str!("admit_fast.rs"), "pub async fn evict_txid_everywhere_repointed(", r#"note("eviction","#),
            ("eviction", include_str!("admit_fast.rs"), "pub async fn readmit_if_evicted(", r#"note("eviction","#),
            ("eviction", include_str!("eviction_write.rs"), "pub(crate) fn plan_pairs(", r#"("eviction","#),
            ("hop", include_str!("eviction_write.rs"), "pub(crate) fn plan_pairs(", r#""hop","#),
            ("eviction", include_str!("eviction_write.rs"), "pub(crate) async fn evict(", "for(source,key)inplan_pairs(&snapshot,txid){crate::owed_events::note(source,&key);}"),
            ("seen", include_str!("ops.rs"), "pub async fn latch_network_seen(", r#"note("seen","#),
            ("refusal", include_str!("submit_refusals.rs"), "pub async fn record(", r#"note("refusal","#),
            ("refund", app("record_post"), "pub(crate) async fn potrefund_chain_noop<", "SOURCE_REFUND"),
            ("party", app("owed_write"), "pub(crate) fn note_events(", r#"note("party","#),
            ("refund", app("owed_write"), "pub(crate) fn note_events(", r#"note("refund","#),
            ("result", app("owed_write"), "pub(crate) fn note_events(", r#"note("result","#),
            ("collected", app("owed_write"), "pub(crate) fn note_events(", r#"note("collected","#),
            ("sweep", app("owed_write"), "pub(crate) fn note_events(", r#"note("sweep","#),
            ("filing", app("record_post"), "pub async fn record_post(", "owed_write::note_events(&verified);"),
            ("hop", app("handfile"), "pub async fn handfile_post(", r#"note("hop","#),
            ("party", app("handfile"), "pub async fn handfile_post(", r#"note("party","#),
            ("refund", app("handfile"), "pub async fn handfile_post(", "owed_write::note_events(&VerifiedRecord::Potrefund("),
            ("word", app("runtime"), "async fn event_held(", r#"note("word","#),
            // FIX-1001 (bsv-low #1001): the transaction's proof (a filed sweep's `index-proof`), each of its writers
            ("beef", include_str!("d1_storage.rs"), "async fn update_transaction_beef(", r#"if has_proof == 1 { crate::owed_events::note("beef", txid); }"#),
            ("beef", include_str!("d1_storage.rs"), "async fn mark_transaction_proven_at(", r#"note("beef","#),
            ("beef", include_str!("d1_storage.rs"), "async fn mark_transaction_proven(&self", r#"note("beef","#),
            ("beef", include_str!("reorg_sweep.rs"), "async fn unprove(", r#"note("beef","#),
            ("pot", include_str!("reorg_sweep.rs"), "pub async fn unmined_hint(", "crate::pot_changes::note(&rec.txid,rec.output_index);"),
            // FIX-1010 (bsv-low #1010): the families no trigger marked, each writer
            ("hand", disc, "pub fn hand_insert_query(", r#"note("hand","#),
            ("hand", disc, "pub fn hand_relatch_query(", r#"note("hand","#),
            ("proof", disc, "async fn store_record(&self, record: &ProofRecord)", r#"note("proof","#),
            ("proof", include_str!("../../low-app-layer/src/proof_post.rs"), "pub async fn proof_post(", "owed_events_publish::note(PROOF_POST_SOURCE,"),
            ("beef", include_str!("d1_storage.rs"), "async fn delete_output(", r#"note("beef", txid);"#),
            ("probe", include_str!("hop_probe_memos.rs"), "async fn marked_delete(", "crate::owed_events::note(PROBE_SOURCE,&d.outpoint);"),
            ("probe", include_str!("hop_probe_memos.rs"), "pub async fn clear_on_reorg(", "HOP_PROBE_MEMO_REORG_CLEAR_MARK_SQL,HOP_PROBE_MEMO_REORG_CLEAR_FRAMED_SQL"),
            ("probe", include_str!("hop_probe_memos.rs"), "pub async fn expire(", "HOP_PROBE_MEMO_EXPIRE_MARK_SQL,HOP_PROBE_MEMO_EXPIRE_FRAMED_SQL"),
            ("word", app("runtime"), "pub async fn tick(", r#"note("word", &gone.txid);"#),
            ("bytes", app("routes"), "fn note_swept_bytes(", "note(crate::owed::SPENDER_BYTES_SOURCE,&g.txid);"),
            ("bytes", app("routes"), "pub async fn sweep_owed_spender_bytes(", "Ok(gone)=>note_swept_bytes(&gone),"),
        ];
        for (source, src, sig, note) in writers {
            let body = fn_body(src, sig);
            let note: String = note.split_whitespace().collect();
            assert!(
                body.contains(&note),
                "{source}: {} does not note",
                sig.trim_start_matches("pub ")
                    .trim_start_matches("(crate) ")
                    .trim_start_matches("async ")
                    .trim_start_matches("fn ")
                    .split('(')
                    .next()
                    .unwrap_or(sig)
            );
        }
        // LOW-8 (the lens): the eviction's and the refusal's notes are made after their writes, never while the
        // statements are planned (a concurrent publish between the plan and the commit read no mark)
        for (src, sig) in [
            (include_str!("eviction_write.rs"), "pub(crate) fn plan("),
            (include_str!("eviction_write.rs"), "fn evidence_inserts("),
            (include_str!("submit_refusals.rs"), "pub fn write_queries("),
        ] {
            assert!(!fn_body(src, sig).contains("owed_events::note"), "{sig} notes at plan time");
        }
        let evict = fn_body(include_str!("eviction_write.rs"), "pub(crate) async fn evict(");
        assert!(
            evict.find("counted_batch(db,statements)").unwrap() < evict.find("owed_events::note(").unwrap(),
            "the eviction notes after its batch"
        );
        // every trigger source but the word's spender bytes has a noting writer here
        for (_, source) in owed_event::TRIGGER_SOURCES {
            assert!(
                [
                    "pot",
                    "party",
                    "hop",
                    "refund",
                    "result",
                    "collected",
                    "sweep",
                    "eviction",
                    "seen",
                    "word",
                    "beef",
                    "refusal",
                    "hand",
                    "proof",
                    "probe",
                    "bytes"
                ]
                .contains(&source),
                "{source}"
            );
        }
    }

    /// LOW-7 and LOW-9 (the D2-ACTOR-A lens): a publish dropped at the answer wall is counted (on the isolate, written
    /// with its next publish, served `publishWallDrops`), and with no queue binding the notes are dropped, never kept
    /// for the isolate's life. RED on 59d98ec6: no wall counter, the no-port arm returns with the notes held.
    #[test]
    fn d2_actor_a_fold_l7_l9_the_wall_drop_is_counted_and_no_port_keeps_no_notes() {
        let _ = take_wall_drops();
        note_wall_drop();
        note_wall_drop();
        assert_eq!(take_wall_drops(), 2);
        assert_eq!(take_wall_drops(), 0);
        assert!(COUNTERS.contains(&COUNTER_WALL_DROPS));
        let view = crate::applayer_counters::owed_events_view(
            &json!({ COUNTER_WALL_DROPS: 3 }),
            Value::Null,
        );
        assert_eq!(view["publishWallDrops"], 3);
        let src = include_str!("owed_events.rs");
        let wall = fn_body(src, "pub async fn publish_before_answer(");
        assert!(wall.contains("Either::Right(_)=>{note_wall_drop();"));
        let env = fn_body(src, "pub async fn publish_env(");
        assert!(env.contains("let_=drain();returnframes;"));
        assert!(env.contains("(COUNTER_WALL_DROPS,take_wall_drops()),"));
    }

    /// The facts of every source read under the shipped schema (a column renamed is a red here, not a fault at publish).
    #[test]
    fn d2_actor_a_every_facts_read_prepares_on_the_shipped_schema() {
        let db = Sqlite::migrated();
        for (_, source) in owed_event::TRIGGER_SOURCES {
            let sql = read_sql(source).unwrap();
            db.conn
                .prepare(sql)
                .unwrap_or_else(|e| panic!("{source}: {e}"));
            let plan: Vec<String> = db
                .conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(
                    rusqlite::params_from_iter(
                        read_args(&Noted {
                            source: source.to_string(),
                            key: "t:0".into(),
                            held: None,
                        })
                        .into_iter()
                        .map(|a| match a {
                            Arg::Text(t) => rusqlite::types::Value::Text(t),
                            Arg::Int(n) => rusqlite::types::Value::Integer(n),
                            Arg::Null => rusqlite::types::Value::Null,
                        }),
                    ),
                    |r| r.get::<_, String>(3),
                )
                .unwrap()
                .map(Result::unwrap)
                .collect();
            // D2-ACTOR-F: a carried source reads its marks and its row once each into a one-row co-routine over a
            // constant row (`row_and_payload!`); scanning THAT co-routine reads no table, so it is allowed by name
            // and only where the plan built it (every table is still a SEARCH)
            let coroutines: Vec<String> = plan
                .iter()
                .filter_map(|l| l.strip_prefix("CO-ROUTINE ").map(str::to_string))
                .collect();
            assert!(
                plan.iter().all(|l| !l.starts_with("SCAN")
                    || l.contains("json_each")
                    || l.contains("CONSTANT")
                    || coroutines.iter().any(|c| l == &format!("SCAN {c}"))),
                "{source}: a facts read scans: {plan:?}"
            );
        }
        for s in owed_event::OWN_SOURCES {
            assert!(read_sql(s).is_some());
        }
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&SEQ_CREATE));
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&DEAD_LETTERS_CREATE));
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&RELEASED_MISSING_HOP_INDEX));
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&crate::d1_ledger::COST_DAILY_QUEUE_ALTER));
    }

    /// (d) `seq` IS MONOTONIC ACROSS TWO WRITERS' BATCHES (MEDIUM-1): each publish takes its tokens in its own batch
    /// from the one counter row, so a later publish's events are all above an earlier one's, in order within each,
    /// and a later publish of the same row carries the facts of every write committed before it.
    #[test]
    fn d2_actor_a_d_seq_is_monotonic_across_two_writers_batches() {
        let db = Sqlite::migrated();
        seed(&db);
        db.conn.execute_batch(&format!("DELETE FROM pot_records; INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES('{POT}',0,0,1)")).unwrap();
        let pot = || Noted {
            source: "pot".into(),
            key: format!("{POT}:0"),
            held: None,
        };
        let hop = || Noted {
            source: "hop".into(),
            key: format!("{HOP}:1"),
            held: None,
        };
        db.conn
            .execute_batch(&format!(
                "UPDATE hopparty_records SET markerValid=1 WHERE txid='{HOP}'"
            ))
            .unwrap();
        // writer A
        let a = run(publish_with(&db, vec![pot(), hop()]));
        // writer B writes the pot row, then publishes it
        db.conn
            .execute_batch(&format!(
                "UPDATE pot_records SET spent=1,spendingTxid='{SPENDER}' WHERE txid='{POT}'"
            ))
            .unwrap();
        let b = run(publish_with(&db, vec![pot()]));
        let seqs: Vec<i64> = a.sent.iter().chain(&b.sent).map(|e| e.seq).collect();
        assert_eq!(seqs.len(), 3);
        assert!(seqs.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");
        assert_eq!(
            b.sent[0].facts["row"]["spent"], 1,
            "the later seq carries the later write"
        );
        assert_eq!(a.sent[0].facts["row"]["spent"], 0);
        // a fresh counter starts at the clock, above any token a counter of the same clock behind it handed out
        assert!(seqs[0] > db.now.get() * 1024 - 1);
    }

    /// (g) A CANCELLED OR REFUSED PUBLISH LEAVES THE MARK: the send refused, the note is answered `unsent` and the
    /// trigger's mark stays in `owed_dirty` for the sweep (pinned at the app layer, which ships it).
    #[test]
    fn d2_actor_a_g_a_refused_publish_leaves_the_mark() {
        let db = Sqlite::migrated();
        seed(&db);
        db.conn
            .execute_batch(&format!(
                "UPDATE pot_records SET spent=1 WHERE txid='{POT}'"
            ))
            .unwrap();
        db.refuse_sends.set(true);
        let n = Noted {
            source: "pot".into(),
            key: format!("{POT}:0"),
            held: None,
        };
        let report = run(publish_with(&db, vec![n.clone()]));
        assert!(report.fault.is_some());
        assert_eq!(report.unsent, vec![n]);
        assert!(report.sent.is_empty());
        assert_eq!(db.marks().len(), 1, "the mark waits for the sweep");
    }

    /// A word frame rides its event as held facts (no read), and a frame whose publish faulted goes back to the POST.
    #[test]
    fn d2_actor_a_a_word_frame_rides_its_event() {
        let db = Sqlite::migrated();
        let frame = json!({"txid": POT, "kind": "proof", "body": null});
        let n = frame_note(&frame).unwrap();
        let report = run(publish_with(&db, vec![n]));
        assert_eq!(report.sent.len(), 1);
        assert_eq!(report.sent[0].frame(), Some(&frame));
        // a pair with no mark is not sent
        let report = run(publish_with(
            &db,
            vec![Noted {
                source: "pot".into(),
                key: "ff:0".into(),
                held: None,
            }],
        ));
        assert_eq!((report.sent.len(), report.skipped), (0, 1));
    }

    /// The pot writers' one note is the owed event of the row (every `pot_changes::note` caller publishes).
    #[test]
    fn d2_actor_a_a_every_pot_note_is_an_owed_event() {
        drain();
        crate::pot_changes::note(&POT.to_ascii_uppercase(), 3);
        let notes = drain();
        crate::pot_changes::drain();
        assert_eq!(
            notes,
            vec![Noted {
                source: "pot".into(),
                key: format!("{POT}:3"),
                held: None
            }]
        );
        // and every flush point publishes before it ships (the source)
        let pots = include_str!("pot_changes.rs");
        let code = &pots[..pots.find("#[cfg(test)]").unwrap()];
        assert_eq!(
            code.matches("crate::owed_events::publish_env(").count(),
            3,
            "flush, flush_inline, flush_inline_within"
        );
        let lib = include_str!("lib.rs");
        assert!(lib.contains(
            "crate::owed_events::publish_before_answer(&env, crate::tracker_events::drain()).await"
        ));
    }

    /// FIX-1010-BUDGET (bsv-low #1010, the land's `record-potparty-filing` refusal): A PARTY FILING MARKS THE POT'S
    /// COMMITTED SEAT ALONE, IN A BOUNDED READ. A pot committing `ka`/`kb`, its two verified seats A and B, and a pile of
    /// stranger rows (unverified copies of a committed key, verified rows under their own keys, keyless rows, naming A or B; a verified row under a committed key is that key's holder, a seat, since its seat signature needs the key). Then: a
    /// keyless stranger filing naming A (the tier's shape) marks only its own pair; a stranger under a committed key
    /// naming A marks A and itself, no stranger; a stranger naming a stranger who holds a verified row under its own key
    /// marks nobody else; B's own filing reaches A, A's re-latch reaches B (the two seats, exactly). The insert's VM
    /// steps are the same over 5 and 200 strangers, and the trigger's read is two index seeks (the opponent's verified
    /// row, then the pot by its key): no SCAN. RED on 0c80ed5c: every stranger on the pot marked and the steps growing.
    #[test]
    fn fix1010_budget_a_party_filing_marks_the_pots_committed_seat_alone() {
        const P: &str = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1";
        let party = |id: &str, opp: &str, key: Option<&str>, valid: i64, txid: &str| {
            format!(
                "INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,seatSettlePubkey,txid,outputIndex,createdAt,sigValid) VALUES('{id}','{opp}','g','{P}',0,1,'00',{},'{txid}',0,1,{valid});",
                key.map_or("NULL".to_string(), |k| format!("'{k}'"))
            )
        };
        let setup = |strangers: usize| -> Sqlite {
            let db = Sqlite::migrated();
            let mut sql = format!(
                "INSERT INTO pot_records(txid,outputIndex,spent,createdAt,pubA,pubB) VALUES('{P}',0,0,1,'ka','kb');"
            );
            sql += &party("A", "B", Some("ka"), 1, "pa");
            sql += &party("B", "A", Some("kb"), 1, "pb");
            for i in 0..strangers {
                let (key, valid, opp) = match i % 5 {
                    0 => (Some("ka"), 0, "A"),
                    1 => (Some("kb"), 0, "B"),
                    2 => (Some("ks"), 1, "A"),
                    3 => (None, 0, "B"),
                    _ => (Some("kt"), 1, "B"),
                };
                sql += &party(&format!("S{i}"), opp, key, valid, &format!("s{i}"));
            }
            sql += "DELETE FROM owed_dirty;";
            db.conn.execute_batch(&sql).unwrap();
            db
        };
        let marked = |db: &Sqlite, txid: &str| -> std::collections::BTreeSet<String> {
            db.marks()
                .into_iter()
                .filter(|(_, _, s, k)| s == "party" && k == txid)
                .map(|(i, ..)| i)
                .collect()
        };
        let set = |ids: &[&str]| {
            ids.iter()
                .map(|s| s.to_string())
                .collect::<std::collections::BTreeSet<_>>()
        };
        let db = setup(5);
        db.conn
            .execute_batch(&party("X1", "A", None, 0, "x1"))
            .unwrap();
        assert_eq!(
            marked(&db, "x1"),
            set(&["X1"]),
            "a keyless filing reaches no seat (the tier's shape)"
        );
        db.conn
            .execute_batch(&party("X2", "A", Some("ka"), 0, "x2"))
            .unwrap();
        assert_eq!(
            marked(&db, "x2"),
            set(&["A", "X2"]),
            "a stranger under a committed key: seat A, no stranger"
        );
        db.conn
            .execute_batch(&party("X3", "S2", Some("kb"), 0, "x3"))
            .unwrap();
        assert_eq!(
            marked(&db, "x3"),
            set(&["X3"]),
            "a stranger's verified row under its own key is no seat"
        );
        // The honest pair: B files again (its second game row), A's row re-latched.
        let db = setup(5);
        db.conn
            .execute_batch(&format!(
                "INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,seatSettlePubkey,txid,outputIndex,createdAt,sigValid) VALUES('B','A','g2','{P}',0,1,'00','kb','pb2',0,2,1);"
            ))
            .unwrap();
        assert_eq!(marked(&db, "pb2"), set(&["A", "B"]), "B's filing reaches A");
        db.conn.execute_batch("DELETE FROM owed_dirty; UPDATE potparty_records SET recoveryHeight=2 WHERE txid='pa';").unwrap();
        let both: std::collections::BTreeSet<String> = db
            .marks()
            .into_iter()
            .filter(|(_, _, s, k)| s == "party" && k == "pa")
            .map(|(i, ..)| i)
            .collect();
        assert_eq!(
            both,
            set(&["A", "B"]),
            "A's re-latch reaches B: the two seats, exactly"
        );
        // Flat in the pile: one filing's VM steps over 5 and over 200 strangers.
        let steps = |strangers: usize| -> i64 {
            let db = setup(strangers);
            let mut q = db
                .conn
                .prepare(&party("X9", "A", Some("ka"), 0, "x9"))
                .unwrap();
            q.raw_execute().unwrap();
            q.get_status(rusqlite::StatementStatus::VmStep) as i64
        };
        let (small, large) = (steps(5), steps(200));
        assert_eq!(
            small, large,
            "a filing's cost does not follow the pot's pile: {small} against {large}"
        );
        // The trigger's read: two seeks, no scan.
        let subjects = crate::derived_owed::write_time::PARTY_INSERT_SEATS;
        let select = &subjects[subjects.find("FROM (").unwrap() + "FROM (".len()
            ..subjects.find(") s WHERE").unwrap()];
        let bound = select
            .replace("NEW.opponentIdentity", "'A'")
            .replace("NEW.identity", "'X'")
            .replace("NEW.gameId", "'g'")
            .replace("NEW.potTxid", &format!("'{P}'"))
            .replace("NEW.potVout", "0")
            .replace("NEW.seatSettlePubkey", "'ka'");
        let db = setup(5);
        let mut q = db
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {bound}"))
            .unwrap();
        let plan: Vec<String> = q
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(plan.iter().all(|l| !l.starts_with("SCAN")), "{plan:?}");
        // the opponent first, the pot only past a verified opponent row (the land tier's keyless filing reads
        // neither: its key is refused on NEW before the loop)
        assert!(
            plan.len() == 2
                && plan[0].contains("pp USING INDEX idx_party_verified_identity")
                && plan[1].contains("p USING"),
            "{plan:?}"
        );
        assert!(
            plan.iter().any(|l| l
                .contains("idx_party_verified_identity (identity=? AND sigValid=? AND <expr>=?)")),
            "{plan:?}"
        );
        let rows: Vec<String> = db
            .conn
            .prepare(bound.as_str())
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(rows, vec!["A".to_string()]);
    }
}
