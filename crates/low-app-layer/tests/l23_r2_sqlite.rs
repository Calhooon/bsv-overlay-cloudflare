use async_trait::async_trait;
use bsv_overlay_cloudflare::{
    admit_fast::*,
    d1::{QVal, OVERLAY_MIGRATIONS},
};
use low_app_layer::owed::*;
use rusqlite::{Connection, OptionalExtension};
struct Sqlite(Connection);
fn params(binds: Vec<QVal>) -> Vec<rusqlite::types::Value> {
    binds
        .into_iter()
        .map(|v| match v {
            QVal::Null => rusqlite::types::Value::Null,
            QVal::Int(v) => v.into(),
            QVal::Text(v) => v.into(),
            QVal::Bool(v) => i64::from(v).into(),
            QVal::Blob(v) => v.into(),
            QVal::Float(v) => v.into(),
        })
        .collect()
}
#[async_trait(?Send)]
impl ShadowDb for Sqlite {
    async fn columns(&self, table: &str) -> Result<Vec<ColumnInfo>, String> {
        let mut q = self
            .0
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .map_err(|e| e.to_string())?;
        let result = q
            .query_map([], |r| {
                Ok(ColumnInfo {
                    name: r.get(1)?,
                    ty: r.get(2)?,
                    notnull: r.get(3)?,
                    dflt_value: r.get(4)?,
                })
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string());
        result
    }
    async fn count(&self, table: &str, key: &str, txid: &str) -> Result<u64, String> {
        self.0
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{table}\" WHERE \"{key}\"=?"),
                [txid],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())
    }
    async fn vouts(&self, table: &str, txid: &str) -> Result<Vec<u32>, String> {
        let mut q = self
            .0
            .prepare(&format!("SELECT outputIndex FROM \"{table}\" WHERE txid=?"))
            .map_err(|e| e.to_string())?;
        let result = q
            .query_map([txid], |r| r.get(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string());
        result
    }
    async fn exec(&self, sql: &str, binds: Vec<QVal>) -> Result<(), String> {
        self.0
            .execute(sql, rusqlite::params_from_iter(params(binds)))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    async fn query_i64(&self, sql: &str, binds: Vec<QVal>) -> Result<Option<i64>, String> {
        self.0
            .query_row(sql, rusqlite::params_from_iter(params(binds)), |r| {
                r.get::<_, Option<i64>>(0)
            })
            .optional()
            .map(Option::flatten)
            .map_err(|e| e.to_string())
    }
}

#[tokio::test]
async fn l23_r2_synthetic_ninth_eviction_twin_only_25h_late_filing_and_planted_marker() {
    // Synthetic pair-13 shape: captured party/lock/BEEF bytes were not available in the findings.
    use bsv_rs::{
        script::LockingScript,
        transaction::{Beef, Transaction, TransactionOutput},
    };
    let db = Sqlite(Connection::open_in_memory().unwrap());
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.0.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    let (game, me) = ("cd".repeat(32), "02".repeat(33));
    let k = format!("03{}", "bb".repeat(32));
    let a = format!("02{}", "aa".repeat(32));
    let lock = overlay_discovery::pot::p2pkh_lock(&bsv_rs::primitives::hash::hash160(
        &hex::decode(&k).unwrap(),
    ));
    let mut hop = Transaction::new();
    hop.inputs.push(bsv_rs::transaction::TransactionInput::new(
        "99".repeat(32),
        0,
    ));
    hop.outputs.push(TransactionOutput::new(
        20_190,
        LockingScript::from_binary(&lock).unwrap(),
    ));
    let hop_id = hop.id();
    let mut joined = Transaction::new();
    joined
        .inputs
        .push(bsv_rs::transaction::TransactionInput::new(
            hop_id.clone(),
            0,
        ));
    joined.outputs.push(TransactionOutput::new(
        40_000,
        LockingScript::from_hex("51").unwrap(),
    ));
    let join = joined.id();
    let mut beef = Beef::new();
    beef.merge_raw_tx(hop.to_binary(), None);
    beef.merge_raw_tx(joined.to_binary(), None);
    db.0.execute("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid,seatSettlePubkey) VALUES (?1,'opp',?2,?3,0,100,'sig',?4,0,1,1,?5)", rusqlite::params![me,game,join,"ef".repeat(32),k]).unwrap();
    db.0.execute("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind,paramsDecoded,pubA,pubB,payPkhA,payPkhB) VALUES (?1,0,0,1,'covenant',1,?2,?3,?4,?5)", rusqlite::params![join,a,k,"11".repeat(20),"22".repeat(20)]).unwrap();
    db.0.execute(
        "INSERT INTO pot_beefs(txid,beef,createdAt) VALUES (?1,?2,1)",
        rusqlite::params![join, beef.to_binary()],
    )
    .unwrap();
    let eviction = evict_core(&db, &join, "REJECTED", 1000).await;
    assert!(eviction.faults.is_empty(), "{:?}", eviction.faults);
    let ledger = vec![(
        join.clone(),
        Some(format!(r#"[{{"txid":"{hop_id}","vout":0}}]"#)),
    )];
    // Eight stranger evictions sort before MY ninth. Their evidence never occupies my bound.
    for i in 1..=8 {
        db.0.execute("INSERT INTO pot_evictions(txid,reason,evictedAt,releasedSpends) VALUES (?1,'REJECTED',1000,?2)", rusqlite::params![format!("{i:064x}"), format!(r#"[{{"txid":"{}","vout":0}}]"#, "aa".repeat(32))]).unwrap();
    }
    db.0.execute(
        "UPDATE pot_evictions SET releasedSpends=?1 WHERE txid=?2",
        rusqlite::params![ledger[0].1, join],
    )
    .unwrap();
    assert!(
        db.0.query_row(OWED_IDENTITY_PROBE_SQL, [&me], |_| Ok(()))
            .optional()
            .unwrap()
            .is_some(),
        "twin-only identity passes the route's probe"
    );
    assert_eq!(
        db.0.query_row(OWED_ATTRIBUTE_BY_POT_SQL, rusqlite::params![join, 0], |r| r
            .get::<_, String>(0))
            .unwrap(),
        me,
        "eviction hooks reach the seat"
    );
    let candidates = read_candidates(&db.0, &me, "");
    let (plan, total) = markerless_plan(&candidates);
    assert_eq!(total, 1);
    assert_eq!(plan[0].txid, join, "identity's ninth eviction is selected");
    // Simulated 25h: the old window is empty; the permanent route still selects the same JOIN.
    assert!(db
        .0
        .query_row(OWED_EVICTIONS_WINDOW_SQL, [25 * 60 * 60 * 1000i64], |_| Ok(
            ()
        ))
        .optional()
        .unwrap()
        .is_none());
    assert_eq!(read_candidates(&db.0, &me, "")[0].txid, join);
    let read = || {
        let seats =
            db.0.prepare(&markerless_seats_sql(1))
                .unwrap()
                .query_map(rusqlite::params![me, join], |r| {
                    Ok(EvictedSeat {
                        pot_txid: r.get(0)?,
                        game_id: r.get(1)?,
                        opponent_identity: r.get(2)?,
                        seat_settle_pubkey: r.get(3)?,
                    })
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        let keys =
            db.0.prepare(MARKERLESS_KEYS_SQL)
                .unwrap()
                .query_map([&join], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, u32>(1)?,
                        low_app_layer::results::CommittedKeys::from_columns(
                            r.get::<_, Option<String>>(2)?.as_deref(),
                            r.get::<_, Option<String>>(3)?.as_deref(),
                            r.get::<_, Option<String>>(4)?.as_deref(),
                            r.get::<_, Option<String>>(5)?.as_deref(),
                        )
                        .unwrap(),
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        let raw: String =
            db.0.query_row(MARKERLESS_BEEF_SQL, [&join], |r| r.get(0))
                .unwrap();
        let outputs =
            hop_outputs_from_join_beef(&hex::decode(raw).unwrap(), &[(hop_id.clone(), 0)]);
        markerless_released_hops(&ledger, &[], &seats, &keys, &outputs)
    };
    let rebuilt = read();
    assert_eq!(rebuilt.len(), 1);
    assert_eq!(rebuilt[0].game_id, game);
    let mut marker = rebuilt[0].clone();
    marker.marker_txid = "fa".repeat(32);
    marker.game_id = "fb".repeat(32); // A VERIFIED later marker already attests this UTXO.
    let extra = markerless_released_hops(
        &ledger,
        std::slice::from_ref(&marker),
        &[],
        &[],
        &Default::default(),
    );
    assert!(extra.is_empty(), "no duplicate; marker wins");
    marker.marker_verified = low_app_layer::hops_view::MarkerVerification::Unverified;
    let valid = read();
    let rebuilt = markerless_released_hops(
        &ledger,
        std::slice::from_ref(&marker),
        &[EvictedSeat {
            pot_txid: join.clone(),
            game_id: game.clone(),
            opponent_identity: "opp".into(),
            seat_settle_pubkey: k.clone(),
        }],
        &[(
            join.clone(),
            0,
            low_app_layer::results::CommittedKeys {
                pub_a: a.clone(),
                pub_b: k.clone(),
                pay_pkh_a: "11".repeat(20),
                pay_pkh_b: "22".repeat(20),
            },
        )],
        &hop_outputs_from_join_beef(&beef.to_binary(), &[(hop_id.clone(), 0)]),
    );
    assert_eq!(
        rebuilt.len(),
        1,
        "planted unverified marker cannot veto evidence"
    );
    let mut view = vec![marker];
    assert_eq!(merge_markerless_hops(&mut view, rebuilt), 1);
    assert_eq!(view[0].game_id, valid[0].game_id);
    assert_eq!(
        derive_for_hops(
            &me,
            &view,
            &ledger,
            &Default::default(),
            &Default::default(),
            &[]
        )[0]
        .game_id,
        game,
        "real derivation keeps the rebuilt entry"
    );
    // A late signed filing lands LIVE after eviction and remains discoverable.
    db.0.execute("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid,seatSettlePubkey) VALUES (?1,'opp',?2,?3,0,100,'sig',?4,0,1,1,?5)", rusqlite::params![me,game,join,"late-filed",k]).unwrap();
    db.0.execute("UPDATE potparty_records_evicted SET sigValid=0", [])
        .unwrap();
    assert_eq!(read().len(), 1, "late live filing alone rebuilds");
    db.0.execute("DELETE FROM potparty_records WHERE txid='late-filed'", [])
        .unwrap();
    db.0.execute("UPDATE potparty_records_evicted SET sigValid=0", [])
        .unwrap();
    assert!(read().is_empty(), "forged party refused");
    db.0.execute(
        "UPDATE potparty_records_evicted SET sigValid=1, identity='stranger'",
        [],
    )
    .unwrap();
    assert!(read().is_empty(), "other identity refused");
    db.0.execute("UPDATE potparty_records_evicted SET identity=?1", [&me])
        .unwrap();
    db.0.execute("UPDATE pot_records_evicted SET pubB=?1", [&a])
        .unwrap();
    assert!(read().is_empty(), "lock not committing K refused");
    db.0.execute("UPDATE pot_records_evicted SET pubB=?1", [&k])
        .unwrap();
    let other_k = format!("03{}", "ee".repeat(32));
    db.0.execute(
        "UPDATE potparty_records_evicted SET seatSettlePubkey=?1",
        [&other_k],
    )
    .unwrap();
    db.0.execute("UPDATE pot_records_evicted SET pubB=?1", [&other_k])
        .unwrap();
    assert!(
        read().is_empty(),
        "stored output exists but does not pay the committed K"
    );
    db.0.execute(
        "UPDATE potparty_records_evicted SET seatSettlePubkey=?1",
        [&k],
    )
    .unwrap();
    db.0.execute("UPDATE pot_records_evicted SET pubB=?1", [&k])
        .unwrap();
    db.0.execute("UPDATE pot_beefs SET beef=x'00'", []).unwrap();
    assert!(read().is_empty(), "unparseable JOIN bytes refused");
}

fn read_candidates(db: &Connection, me: &str, cursor: &str) -> Vec<MarkerlessCandidate> {
    db.prepare(MARKERLESS_CANDIDATES_SQL)
        .unwrap()
        .query_map([me, cursor], |r| {
            Ok(MarkerlessCandidate {
                txid: r.get(0)?,
                released_spends: r.get(1)?,
                evicted_at: r.get(2)?,
                total: r.get(3)?,
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn derive_for_hops(
    me: &str,
    hops: &[low_app_layer::hops_view::HopEntry],
    ledger: &[(String, Option<String>)],
    chain: &std::collections::HashMap<String, HopChainWord>,
    outputs: &std::collections::HashMap<String, Vec<SpenderOutput>>,
    keys: &[(String, u32, low_app_layer::results::CommittedKeys)],
) -> Vec<OwedRow> {
    let refused = refused_hop_outpoints_of(&released_hop_outpoints(ledger), hops);
    let homes = evicted_pot_homes(&released_hops_by_eviction(ledger, hops), hops, keys)
        .into_iter()
        .collect();
    derive_owed_rows(&OwedInputs {
        identity_lc: me,
        tip: Some(970754),
        now_ms: 25 * 60 * 60 * 1000,
        results: &[],
        refunds: &[],
        hops,
        valid_refunds: &Default::default(),
        collected_verified: &Default::default(),
        collected_present: &Default::default(),
        held_verified: &Default::default(),
        pot_spenders: &Default::default(),
        pot_spenders_faulted: false,
        hop_chain: chain,
        evicted_hop_outpoints: &refused,
        door_refused_hop_outpoints: &Default::default(),
        hop_sweeps: &Default::default(),
        evicted_pots: &Default::default(),
        spender_outputs: outputs,
        spender_inputs: &Default::default(),
        courier_spenders: &Default::default(),
        my_pkh_by_game: &homes,
        home_spends: &Default::default(),
    })
}

#[tokio::test]
async fn l23_r2_identity_cursor_walks_own_ninth_and_beyond_and_counts_tail() {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    let k = "038884be9dd2274f39f6a60a0d0a54ed3ca1aaf9bebdde62d0d48ebd5aafd1a5a9";
    let lock = overlay_discovery::pot::p2pkh_lock(&bsv_rs::primitives::hash::hash160(
        &hex::decode(k).unwrap(),
    ));
    let mut outputs = std::collections::HashMap::new();
    let mut seats = Vec::new();
    let mut keys = Vec::new();
    for i in 1..=19 {
        let join = format!("{i:064x}");
        let hop = format!("{:064x}", 100 + i);
        db.execute("INSERT INTO potparty_records_evicted(identity,potTxid,gameId,opponentIdentity,sigValid,seatSettlePubkey,af_evictedAt) VALUES ('me',?1,?1,'opponent',1,?2,1000)", rusqlite::params![join,k]).unwrap();
        db.execute("INSERT INTO pot_evictions(txid,reason,evictedAt,releasedSpends) VALUES (?1,'REJECTED',1000,?2)", rusqlite::params![join,format!(r#"[{{"txid":"{hop}","vout":0}}]"#)]).unwrap();
        seats.push(EvictedSeat {
            pot_txid: join.clone(),
            game_id: join.clone(),
            opponent_identity: "opponent".into(),
            seat_settle_pubkey: k.into(),
        });
        keys.push((
            join,
            0,
            low_app_layer::results::CommittedKeys {
                pub_a: "02".repeat(33),
                pub_b: k.into(),
                pay_pkh_a: "11".repeat(20),
                pay_pkh_b: "22".repeat(20),
            },
        ));
        outputs.insert(format!("{hop}:0"), (lock.clone(), 20190));
    }
    let mut cursor = String::new();
    let mut reached = std::collections::HashSet::new();
    let (cursor_txid, cursor_vout) = markerless_cursor_target("me");
    let cursor_key = low_app_layer::hops_view::probe_memo_key(&cursor_txid, cursor_vout);
    for step in 0..3 {
        let (selected, total) = markerless_plan(&read_candidates(&db, "me", &cursor));
        assert_eq!(total, 19);
        assert_eq!(selected.len(), 8);
        let ledger: Vec<_> = selected
            .iter()
            .map(|r| (r.txid.clone(), r.released_spends.clone()))
            .collect();
        let rebuilt = markerless_released_hops(&ledger, &[], &seats, &keys, &outputs);
        assert_eq!(
            rebuilt.len(),
            8,
            "the cursor page rebuilds its evidence, including own ninth and later JOINs"
        );
        reached.extend(selected.iter().map(|r| r.txid.clone()));
        let next = selected.last().unwrap().txid.clone();
        let progress =
            serde_json::json!({"cursor":next,"candidates":total,"examined":selected.len()})
                .to_string();
        let stamp = 1000 + step;
        db.execute(
            OWED_STATE_UPSERT_SQL,
            rusqlite::params!["me", stamp, 0, 8, 1],
        )
        .unwrap();
        db.execute(
            MARKERLESS_CURSOR_UPSERT_SQL,
            rusqlite::params![cursor_key, stamp, progress, "me"],
        )
        .unwrap();
        let stored: String = db
            .query_row(OWED_STATE_READ_SQL, ["me"], |r| r.get(6))
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored).unwrap()["candidates"],
            19
        );
        cursor = markerless_cursor_of(&stored);
    }
    assert_eq!(
        db.execute(
            MARKERLESS_CURSOR_UPSERT_SQL,
            rusqlite::params![cursor_key, 1, "older-progress", "me"]
        )
        .unwrap(),
        0,
        "a superseded walk cannot rewind the cursor"
    );
    assert_eq!(
        reached.len(),
        19,
        "the durable ring reaches the ninth and every later identity-owned JOIN"
    );
    // L23R2D-L1: nine evictions, every hop held by judged evidence: count alone never cuts.
    let nine: Vec<_> = (1..=9)
        .map(|i| {
            (
                format!("{i:064x}"),
                Some(format!(r#"[{{"txid":"{:064x}","vout":0}}]"#, 100 + i)),
            )
        })
        .collect();
    let judged = markerless_released_hops(&nine, &[], &seats, &keys, &outputs);
    assert_eq!(judged.len(), 9);
    assert!(unheld_released_by_eviction(&nine, &judged).is_empty());
    assert!(
        !markerless_tail_unheld(9, 8, 0, 0, false),
        "nine fully judged evictions are not truncated"
    );
    assert!(
        markerless_tail_unheld(9, 8, 1, 0, false),
        "unheld evidence before a complete ring is a cut"
    );
    assert!(!markerless_tail_unheld(9, 8, 1, 0, true));
    assert!(
        markerless_tail_unheld(9, 8, 0, 1, true),
        "a carried unjudged row remains a cut"
    );
    let selected = markerless_plan(&read_candidates(&db, "me", &cursor)).0;
    assert!(markerless_ring_complete(
        r#"{"complete":true,"candidates":19}"#,
        &cursor,
        &selected,
        19
    ));
}

#[tokio::test]
async fn l23_r2_captured_pair13_rows_rebuild_and_tell_spent_elsewhere() {
    use serde_json::Value;
    let captured: Value =
        serde_json::from_str(include_str!("fixtures/l23_r2_pair13_captured.json")).unwrap();
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    load_captured(&db, &captured);
    let me = "02f9f80a92a92677d97fce5bb0a03d0321425b918868b3ef5773120d9cf5364e49";
    let hop = "97288e47d846066d59f0caa8a2bec22fd9040dd5af05f323c67cf010373ab064";
    let competitor = "57d8e2907420da08795e3146fb5bd05446a51ef2f870213775a074eccae73851";
    let (selected, total) = markerless_plan(&read_candidates(&db, me, ""));
    assert_eq!(total, 1);
    let join = &selected[0].txid;
    let seats = db
        .prepare(&markerless_seats_sql(1))
        .unwrap()
        .query_map([me, join], |r| {
            Ok(EvictedSeat {
                pot_txid: r.get(0)?,
                game_id: r.get(1)?,
                opponent_identity: r.get(2)?,
                seat_settle_pubkey: r.get(3)?,
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        seats.len(),
        1,
        "captured p28 v2 filing has sigValid=1 and K"
    );
    let keys = db
        .prepare(MARKERLESS_KEYS_SQL)
        .unwrap()
        .query_map([join], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, u32>(1)?,
                low_app_layer::results::CommittedKeys::from_columns(
                    r.get::<_, Option<String>>(2)?.as_deref(),
                    r.get::<_, Option<String>>(3)?.as_deref(),
                    r.get::<_, Option<String>>(4)?.as_deref(),
                    r.get::<_, Option<String>>(5)?.as_deref(),
                )
                .unwrap(),
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let hex_beef: String = db
        .query_row(MARKERLESS_BEEF_SQL, [join], |r| r.get(0))
        .unwrap();
    let bytes = hex::decode(hex_beef).unwrap();
    assert!(low_app_layer::logic::extract_raw_tx_hex(&bytes, hop).is_some());
    let outputs = hop_outputs_from_join_beef(&bytes, &[(hop.into(), 0)]);
    assert_eq!(outputs[&format!("{hop}:0")].1, 20190);
    assert_eq!(
        outputs[&format!("{hop}:0")].0,
        overlay_discovery::pot::p2pkh_lock(&bsv_rs::primitives::hash::hash160(
            &hex::decode(&seats[0].seat_settle_pubkey).unwrap()
        ))
    );
    let ledger: Vec<_> = selected
        .iter()
        .map(|r| (r.txid.clone(), r.released_spends.clone()))
        .collect();
    let rebuilt = markerless_released_hops(&ledger, &[], &seats, &keys, &outputs);
    assert_eq!(rebuilt.len(), 1);
    let tx = bsv_rs::transaction::Transaction::from_hex(
        captured["competitor_raw_hex"].as_str().unwrap(),
    )
    .unwrap();
    assert_eq!(
        tx.id(),
        competitor,
        "WoC raw bytes hash to the captured competitor"
    );
    assert!(tx
        .inputs
        .iter()
        .any(|i| i.source_txid.as_deref() == Some(hop) && i.source_output_index == 0));
    let chain = [(
        format!("{hop}:0"),
        HopChainWord {
            looked: true,
            spent: Some(true),
            spending_txid: Some(competitor.into()),
            spent_confirmed: Some(true),
            stale: false,
            age_ms: None,
        },
    )]
    .into_iter()
    .collect();
    let spends = [(
        competitor.into(),
        tx.outputs
            .iter()
            .enumerate()
            .map(|(i, o)| SpenderOutput {
                vout: i as u32,
                pkh_hex: p2pkh_pkh_hex(&o.locking_script.to_binary()),
                sats: o.satoshis.unwrap(),
                spent: None,
                pot_lock: false,
            })
            .collect(),
    )]
    .into_iter()
    .collect();
    let rows = derive_for_hops(me, &rebuilt, &ledger, &chain, &spends, &keys);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].facts["spendKind"], "spent-elsewhere");
    assert_eq!(rows[0].facts["claim"], Value::Null);
}

#[test]
fn l23_r2_faulted_rows_do_not_offer_a_press() {
    let rows = vec![OwedRow {
        identity: "me".into(),
        outpoint: format!("{}:0", "ab".repeat(32)),
        family: OwedFamily::HopStranded,
        game_id: "game".into(),
        sats: Some(20190),
        opponent_identity: None,
        at_height: None,
        facts: serde_json::json!({"markerlessEvidence":true,"claim":"sweep-hop","claimable":true}),
        reason: None,
    }];
    let rows = markerless_faulted_rows(rows);
    assert_eq!(rows[0].family, OwedFamily::Unbound);
    assert!(rows[0].facts["claim"].is_null());
    assert_eq!(rows[0].facts["claimable"], false);
    assert!(rows[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("could not judge"));
}

fn load_captured(db: &Connection, captured: &serde_json::Value) {
    use serde_json::Value;
    // Captured D1 rows, including the original signatures and filed: txids, without synthesising their keys.
    for (name, table) in [
        ("evictions", "pot_evictions"),
        ("parties", "potparty_records_evicted"),
        ("locks", "pot_records_evicted"),
        ("join_beef", "pot_beefs"),
    ] {
        let cols: Vec<String> = db
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap()
            .query_map([], |r| r.get(1))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        for row in captured["rows"][name].as_array().unwrap() {
            let fields: Vec<_> = row
                .as_object()
                .unwrap()
                .iter()
                .filter(|(k, _)| cols.contains(k))
                .collect();
            let binds: Vec<rusqlite::types::Value> = fields
                .iter()
                .map(|(k, v)| match v {
                    Value::Null => rusqlite::types::Value::Null,
                    Value::Number(n) => n.as_i64().unwrap().into(),
                    Value::String(s) if k.as_str() == "beef" => hex::decode(s).unwrap().into(),
                    Value::String(s) => s.clone().into(),
                    _ => panic!("unexpected captured field"),
                })
                .collect();
            let sql = format!(
                "INSERT INTO {table} ({}) VALUES ({})",
                fields
                    .iter()
                    .map(|(k, _)| format!("\"{k}\""))
                    .collect::<Vec<_>>()
                    .join(","),
                vec!["?"; fields.len()].join(",")
            );
            db.execute(&sql, rusqlite::params_from_iter(binds)).unwrap();
        }
    }
}

struct EvidenceSqlite<'a> {
    db: &'a Connection,
    fail: Option<&'static str>,
}
impl MarkerlessEvidenceReads for EvidenceSqlite<'_> {
    async fn index(&mut self, txid: &str, vout: u32) -> Result<Option<MarkerlessIndex>, String> {
        if self.fail == Some("index") {
            return Err("index transport fault".into());
        }
        self.db.query_row("SELECT spent, spendingTxid, spentConfirmed FROM pot_records WHERE txid=?1 AND outputIndex=?2",
            rusqlite::params![txid,vout], |r| Ok(MarkerlessIndex {spent:r.get(0)?,spending_txid:r.get(1)?,spent_confirmed:r.get(2)?}))
            .optional().map_err(|e| e.to_string())
    }
    async fn keys(
        &mut self,
        join: &str,
    ) -> Result<Vec<(String, u32, low_app_layer::results::CommittedKeys)>, String> {
        if self.fail == Some("keys") {
            return Err("keys transport fault".into());
        }
        let mut stmt = self
            .db
            .prepare(MARKERLESS_KEYS_SQL)
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([join], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, u32>(1)?,
                    low_app_layer::results::CommittedKeys::from_columns(
                        r.get::<_, Option<String>>(2)?.as_deref(),
                        r.get::<_, Option<String>>(3)?.as_deref(),
                        r.get::<_, Option<String>>(4)?.as_deref(),
                        r.get::<_, Option<String>>(5)?.as_deref(),
                    )
                    .unwrap(),
                ))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string());
        rows
    }
    async fn beef(&mut self, join: &str) -> Result<Option<Vec<u8>>, String> {
        if self.fail == Some("beef") {
            return Err("beef transport fault".into());
        }
        self.db
            .query_row(MARKERLESS_BEEF_SQL, [join], |r| {
                r.get::<_, Option<String>>(0)
            })
            .optional()
            .map(|v| v.flatten().and_then(|v| hex::decode(v).ok()))
            .map_err(|e| e.to_string())
    }
}

#[tokio::test]
async fn l23_r2_delta_missing_beef_faults_one_candidate_and_keeps_other_rows() {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    let captured: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/l23_r2_pair13_captured.json")).unwrap();
    load_captured(&db, &captured);
    let me = "02f9f80a92a92677d97fce5bb0a03d0321425b918868b3ef5773120d9cf5364e49";
    let selected = markerless_plan(&read_candidates(&db, me, "")).0;
    let candidate = &selected[0];
    let ledger = vec![(candidate.txid.clone(), candidate.released_spends.clone())];
    let points = unheld_released_by_eviction(&ledger, &[])[0].1.clone();
    let seats = vec![EvictedSeat {
        pot_txid: candidate.txid.clone(),
        game_id: "game".into(),
        opponent_identity: "opponent".into(),
        seat_settle_pubkey: "038884be9dd2274f39f6a60a0d0a54ed3ca1aaf9bebdde62d0d48ebd5aafd1a5a9"
            .into(),
    }];
    let mut reads = EvidenceSqlite {
        db: &db,
        fail: None,
    };
    let mut budget = OWED_MARKERLESS_INDEX_READS_PER_RECOMPUTE;
    let healthy =
        read_markerless_candidate(&mut reads, me, candidate, &points, &seats, &mut budget).await;
    assert!(healthy.faults.is_empty());
    assert_eq!(healthy.outputs.len(), points.len());
    db.execute("DELETE FROM pot_beefs WHERE txid=?1", [&candidate.txid])
        .unwrap();
    for fail in [None, Some("keys"), Some("index"), Some("beef")] {
        reads.fail = fail;
        let faulty =
            read_markerless_candidate(&mut reads, me, candidate, &points, &seats, &mut budget)
                .await;
        assert_eq!(
            faulty.faults.len(),
            points.len(),
            "candidate absence must be served, never list death"
        );
        assert!(faulty.outputs.is_empty());
        assert!(faulty.keys.is_empty());
        assert!(faulty
            .faults
            .iter()
            .all(|r| r.family == OwedFamily::Unbound && r.facts["claim"].is_null()));
        if fail.is_none() {
            assert!(faulty.faults[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("evidence-missing"));
        }
        // Other already-derived families remain fresh; a candidate fault contributes only its marked rows.
        let mut rows = vec![OwedRow {
            identity: me.into(),
            outpoint: "other:0".into(),
            family: OwedFamily::Payout,
            game_id: "other".into(),
            sats: Some(17),
            opponent_identity: None,
            at_height: None,
            facts: serde_json::json!({"claim":"internalize"}),
            reason: None,
        }];
        let mut same_outpoint = rows[0].clone();
        same_outpoint.outpoint = faulty.faults[0].outpoint.clone();
        let mut overlap = vec![same_outpoint.clone()];
        merge_markerless_faults(&mut overlap, faulty.faults.clone());
        assert_eq!(
            overlap[0], same_outpoint,
            "a candidate fault cannot mask an independently judged payout on the same UTXO"
        );
        let mut planted = same_outpoint;
        planted.facts = serde_json::json!({"markerVerified":"unknown","claim":"sweep-hop"});
        let mut unlatched = vec![planted];
        merge_markerless_faults(&mut unlatched, faulty.faults.clone());
        assert_eq!(unlatched[0].family, OwedFamily::Unbound);
        assert!(
            unlatched[0].facts["claim"].is_null(),
            "an Unknown attestation cannot mask the current fault"
        );
        merge_markerless_faults(&mut rows, faulty.faults);
        assert_eq!(rows.len(), 1 + points.len());
        assert_eq!(rows[0].family, OwedFamily::Payout);
        assert_eq!(rows[0].facts["claim"], "internalize");
    }
    reads.fail = None;
    db.execute(
        "INSERT INTO pot_beefs(txid,beef) VALUES (?1,x'00')",
        [&candidate.txid],
    )
    .unwrap();
    let faulty =
        read_markerless_candidate(&mut reads, me, candidate, &points, &seats, &mut budget).await;
    assert_eq!(
        faulty.faults.len(),
        points.len(),
        "malformed bytes are the same candidate fault"
    );
}
