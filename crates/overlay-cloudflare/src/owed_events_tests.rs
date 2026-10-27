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
            _ => return None,
        })
    }

    /// The 29 triggers, read from the shipped source at test time (never a hand-fed list): each trigger's name.
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
        assert_eq!(triggers.len(), 29, "the trigger list: {triggers:?}");
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
            if !trigger.ends_with("_delete") && !trigger.contains("missing") && source != "word" {
                assert!(
                    !sent.facts["row"].is_null(),
                    "{trigger}: the row's facts: {}",
                    sent.facts
                );
            }
        }
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
                ["pot", "party", "hop", "refund", "result", "collected", "sweep", "eviction", "seen", "word", "beef", "refusal"]
                    .contains(&source),
                "{source}"
            );
        }
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
            assert!(
                plan.iter().all(|l| !l.starts_with("SCAN")
                    || l.contains("json_each")
                    || l.contains("CONSTANT")),
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
}
