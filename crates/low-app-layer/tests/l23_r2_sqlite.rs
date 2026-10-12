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
async fn l23_r2_markerless_route_reads_real_sqlite() {
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
    marker.game_id = "fb".repeat(32); // Even a disagreeing later marker owns this UTXO.
    let extra = markerless_released_hops(
        &ledger,
        std::slice::from_ref(&marker),
        &[],
        &[],
        &Default::default(),
    );
    assert!(extra.is_empty(), "no duplicate; marker wins");
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
