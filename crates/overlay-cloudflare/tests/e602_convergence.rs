use async_trait::async_trait;
use bsv_overlay_cloudflare::{
    admit_fast::*,
    d1::{QVal, OVERLAY_MIGRATIONS},
    proof_fetcher::NetworkPresence,
};
use overlay_engine::gasp::{AncestorFetcher, FetchedAncestor, GASPError};
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
#[async_trait(?Send)]
impl ConflictEviction for Sqlite {
    async fn evict_conflict(&self, txid: &str, reason: &str, now_ms: u64) -> Result<(), String> {
        let result = evict_core(self, txid, reason, now_ms).await;
        assert!(result.faults.is_empty(), "{:?}", result.faults);
        assert!(result.yielded_to_readmission.is_none());
        Ok(())
    }
}
struct Evidence {
    raw: String,
    height: Option<u32>,
}
#[async_trait(?Send)]
impl AncestorFetcher for Evidence {
    async fn fetch_ancestor(&self, _: &str) -> Result<FetchedAncestor, GASPError> {
        unreachable!()
    }
    async fn resolve_spender(&self, _: &str, _: u32) -> Result<Option<String>, String> {
        Ok(Some(
            bsv_rs::transaction::Transaction::from_hex(&self.raw)
                .unwrap()
                .id(),
        ))
    }
    async fn spender_binding_raw(
        &self,
        _: &str,
        _: &str,
        _: u32,
    ) -> Result<Option<String>, String> {
        Ok(Some(self.raw.clone()))
    }
}
#[async_trait(?Send)]
impl ConflictPresence for Evidence {
    async fn presence(&self, _: &str) -> Option<NetworkPresence> {
        Some(NetworkPresence::Present {
            height: self.height,
        })
    }
}
fn spending(parent: &str, satoshis: u64) -> bsv_rs::transaction::Transaction {
    use bsv_rs::{
        script::LockingScript,
        transaction::{Transaction, TransactionInput, TransactionOutput},
    };
    let mut tx = Transaction::new();
    tx.inputs.push(TransactionInput::new(parent.into(), 0));
    tx.outputs.push(TransactionOutput {
        satoshis: Some(satoshis),
        locking_script: LockingScript::from_hex("51").unwrap(),
        change: false,
    });
    tx
}
#[tokio::test]
async fn e602_readmitted_pot_converges_only_on_a_bound_confirmed_competitor() {
    let db = Sqlite(Connection::open_in_memory().unwrap());
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.0.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    let hop = "aa".repeat(32);
    let subject = spending(&hop, 40_000);
    let txid = subject.id();
    let raw = subject.to_binary();
    db.0.execute(
        "INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES (?,0,0,1)",
        [&txid],
    )
    .unwrap();
    let evicted = evict_core(&db, &txid, "UTXO_SPENT", 1000).await;
    assert!(evicted.faults.is_empty(), "{:?}", evicted.faults);
    assert_eq!(db.count("pot_records", "txid", &txid).await.unwrap(), 0);
    let (_, faults) = restore_twins(&db, &txid).await;
    assert!(faults.is_empty());
    db.0.execute(
        "UPDATE pot_evictions SET readmittedAt=2000 WHERE txid=?",
        [&txid],
    )
    .unwrap();
    let mut beef = bsv_rs::transaction::Beef::new();
    beef.merge_raw_tx(raw.clone(), None);
    db.0.execute("INSERT INTO pot_beefs(txid,beef,createdAt,has_proof,proof_verified,structurally_unprovable) VALUES (?, ?, 1, 0, 0, 1)", rusqlite::params![txid, beef.to_binary()]).unwrap();
    let (selected, hex_beef): (String, String) =
        db.0.query_row(CONFLICT_CANDIDATES_SQL, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap();
    assert_eq!(
        selected, txid,
        "retired readmissions stay in the production candidate query"
    );
    let stored_raw =
        bsv_overlay_cloudflare::ef::proven_subject_raw(&hex::decode(hex_beef).unwrap()).unwrap();
    assert_eq!(stored_raw, raw);
    let mut evidence = Evidence {
        raw: spending(&hop, 39_000).to_hex(),
        height: None,
    };
    assert!(
        !converge_confirmed_conflict(&db, &evidence, &evidence, &txid, &raw, 3000)
            .await
            .unwrap()
    );
    assert_eq!(db.count("pot_records", "txid", &txid).await.unwrap(), 1);
    evidence.height = Some(970710);
    evidence.raw = spending(&"bb".repeat(32), 39_000).to_hex();
    assert!(
        !converge_confirmed_conflict(&db, &evidence, &evidence, &txid, &raw, 3000)
            .await
            .unwrap()
    );
    evidence.raw = spending(&hop, 39_000).to_hex();
    assert!(
        converge_confirmed_conflict(&db, &evidence, &evidence, &txid, &raw, 3000)
            .await
            .unwrap()
    );
    assert_eq!(db.count("pot_records", "txid", &txid).await.unwrap(), 0);
    assert_eq!(
        db.count("pot_records_evicted", "txid", &txid)
            .await
            .unwrap(),
        1
    );
    let stamp: Option<i64> =
        db.0.query_row(
            "SELECT readmittedAt FROM pot_evictions WHERE txid=?",
            [&txid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stamp, None);
}

#[tokio::test]
async fn e602_fold_the_conflict_counters_exist_and_move_on_one_eviction() {
    let db = Sqlite(Connection::open_in_memory().unwrap());
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.0.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    let hop = "cc".repeat(32);
    let subject = spending(&hop, 40_000);
    let txid = subject.id();
    let raw = subject.to_binary();
    db.0.execute(
        "INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES (?,0,0,1)",
        [&txid],
    )
    .unwrap();
    let evidence = Evidence {
        raw: spending(&hop, 39_000).to_hex(),
        height: Some(970710),
    };
    let mut summary = ConflictPassSummary::default();
    summary.note(&converge_confirmed_conflict(&db, &evidence, &evidence, &txid, &raw, 3000).await);
    summary.note(&Err("courier 429".into()));
    assert_eq!(
        db.count("pot_records_evicted", "txid", &txid)
            .await
            .unwrap(),
        1
    );
    for (name, delta) in summary.counter_deltas() {
        if delta > 0 {
            db.0.execute(
                bsv_overlay_cloudflare::ops::BUMP_COUNTER_SQL,
                rusqlite::params![name, delta as i64],
            )
            .unwrap();
        }
    }
    let read = |name: &str| -> Option<i64> {
        db.0.query_row("SELECT value FROM ops_counters WHERE name=?", [name], |r| {
            r.get(0)
        })
        .optional()
        .unwrap()
    };
    assert_eq!(read("admit_fast_conflict_scanned_total"), Some(2));
    assert_eq!(read("admit_fast_conflict_evicted_total"), Some(1));
    assert_eq!(read("admit_fast_conflict_faults_total"), Some(1));
    let ops = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/ops.rs")).unwrap();
    let served = &ops[ops.find("async fn read_counters(").unwrap()..];
    for name in [
        "COUNTER_ADMIT_FAST_CONFLICT_SCANNED,",
        "COUNTER_ADMIT_FAST_CONFLICT_EVICTED,",
        "COUNTER_ADMIT_FAST_CONFLICT_FAULTS,",
    ] {
        assert!(served.contains(name), "{name} served from 0");
    }
}
