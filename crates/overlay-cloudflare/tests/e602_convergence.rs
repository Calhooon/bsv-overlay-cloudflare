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
        // bsv-low #605: the production eviction (`evict_txid_everywhere`) then releases the evicted tx's unconfirmed
        // pointers with the shipped statement; modelled here so the pin sees what the release leaves
        let cols = self.columns("pot_records").await?;
        self.exec(
            &release_pot_spends_sql(&cols),
            vec![QVal::Text(txid.to_ascii_lowercase())],
        )
        .await
    }
    async fn record_conflict_spender(
        &self,
        outpoint_txid: &str,
        vout: u32,
        evicted: &str,
        competitor: &str,
    ) -> Result<bool, String> {
        record_conflict_spender(self, outpoint_txid, vout, evicted, competitor).await
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

/// bsv-low #605: the conflict pass writes the confirmed competitor as the HOP's spender before the eviction releases
/// the evicted JOIN's pointers, so the hop reads `spent = 1, spendingTxid = <competitor>` (unconfirmed; the #186
/// chaser's CAS latches it), the JOIN's other input (a hop the competitor did not take) is released as before, a
/// CONFIRMED pointer never moves, the pot's rows are in the twins and the pass's counters are the #602 ones.
/// Under the REAL shipped schema. RED on `22742afd` with the port's method grafted inert (the call removed): the hop
/// reads `(0, None)`.
#[tokio::test]
async fn e605_a_conflict_eviction_leaves_the_hop_spent_by_the_confirmed_competitor() {
    use bsv_rs::transaction::TransactionInput;
    let db = Sqlite(Connection::open_in_memory().unwrap());
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.0.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    let (hop, other_hop) = ("aa".repeat(32), "bb".repeat(32));
    let mut join = spending(&hop, 40_000);
    join.inputs
        .push(TransactionInput::new(other_hop.clone(), 0));
    let txid = join.id();
    let raw = join.to_binary();
    let competitor = spending(&hop, 20_190);
    let competitor_txid = competitor.id();
    // the JOIN's pot row, and the two hops it spent (unconfirmed, with the verdict group riding the pointer)
    db.0.execute(
        "INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES (?,0,0,1)",
        [&txid],
    )
    .unwrap();
    for h in [&hop, &other_hop] {
        db.0.execute(
            "INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,spentAt,spentHeight,spenderFinal,verdict,verdictTxid,createdAt) VALUES (?1,0,1,?2,0,5,970700,1,'x',?2,1)",
            rusqlite::params![h, &txid],
        )
        .unwrap();
    }
    let evidence = Evidence {
        raw: competitor.to_hex(),
        height: Some(970_710),
    };
    let mut summary = ConflictPassSummary::default();
    summary.note(&converge_confirmed_conflict(&db, &evidence, &evidence, &txid, &raw, 3000).await);
    type HopRow = (
        i64,
        Option<String>,
        i64,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<i64>,
    );
    let row = |h: &str| -> HopRow {
        db.0.query_row(
            "SELECT spent, spendingTxid, spentConfirmed, spentHeight, verdict, spenderFinal, spentAt FROM pot_records WHERE txid=? AND outputIndex=0",
            [h],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
        )
        .unwrap()
    };
    let (spent, spender, confirmed, height, verdict, fin, at) = row(&hop);
    assert_eq!(
        (spent, spender.as_deref(), confirmed),
        (1, Some(competitor_txid.as_str()), 0),
        "the hop is spent by the confirmed competitor, unconfirmed until the chaser latches it"
    );
    assert_eq!(
        (height, verdict, fin),
        (None, None, None),
        "the evicted JOIN's facts left with it"
    );
    assert!(
        at.is_some_and(|a| a > 5),
        "spentAt restamped: the chaser's age gate starts now"
    );
    // the JOIN's other input, which the competitor did not take: released as before
    assert_eq!(row(&other_hop).0, 0);
    assert_eq!(row(&other_hop).1, None);
    // the pot's rows are in the twins, and the pass counted as #602 counts
    assert_eq!(db.count("pot_records", "txid", &txid).await.unwrap(), 0);
    assert_eq!(
        db.count("pot_records_evicted", "txid", &txid)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        (summary.scanned, summary.evicted, summary.faults),
        (1, 1, 0)
    );
    // the #186 chaser's confirm CAS (the shipped statement) latches the pointer once the competitor's proof verifies
    let latched: Option<String> =
        db.0.query_row(
            bsv_overlay_cloudflare::d1_discovery::confirm_spend_cas_sql(),
            rusqlite::params![970_710i64, &hop, 0i64, &competitor_txid],
            |r| r.get(0),
        )
        .optional()
        .unwrap();
    assert_eq!(latched.as_deref(), Some(hop.as_str()));
    assert_eq!((row(&hop).2, row(&hop).3), (1, Some(970_710)));
}

/// bsv-low #605: the guard. A CONFIRMED pointer never moves on the pass's word, another spender's pointer is not
/// taken, an already-released row (none) is re-pointed, and a miss leaves the eviction to go on.
#[tokio::test]
async fn e605_b_the_record_is_guarded_on_the_evicted_pointer_and_unconfirmed() {
    let db = Sqlite(Connection::open_in_memory().unwrap());
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.0.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    let (join, comp, other) = ("ab".repeat(32), "cd".repeat(32), "ef".repeat(32));
    let (confirmed, foreign, released) = ("01".repeat(32), "02".repeat(32), "03".repeat(32));
    db.0.execute("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,createdAt) VALUES (?1,0,1,?2,1,1)", rusqlite::params![&confirmed, &join]).unwrap();
    db.0.execute("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,createdAt) VALUES (?1,0,1,?2,0,1)", rusqlite::params![&foreign, &other]).unwrap();
    db.0.execute("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,createdAt) VALUES (?1,0,0,NULL,0,1)", rusqlite::params![&released]).unwrap();
    assert!(!record_conflict_spender(&db, &confirmed, 0, &join, &comp)
        .await
        .unwrap());
    assert!(!record_conflict_spender(&db, &foreign, 0, &join, &comp)
        .await
        .unwrap());
    assert!(
        !record_conflict_spender(&db, &"04".repeat(32), 0, &join, &comp)
            .await
            .unwrap(),
        "no such row"
    );
    assert!(record_conflict_spender(
        &db,
        &released.to_ascii_uppercase(),
        0,
        &join.to_ascii_uppercase(),
        &comp.to_ascii_uppercase()
    )
    .await
    .unwrap());
    let pointer = |h: &str| -> (i64, Option<String>, i64) {
        db.0.query_row(
            "SELECT spent, spendingTxid, spentConfirmed FROM pot_records WHERE txid=?",
            [h],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
    };
    assert_eq!(pointer(&confirmed), (1, Some(join.clone()), 1));
    assert_eq!(pointer(&foreign), (1, Some(other.clone()), 0));
    assert_eq!(pointer(&released), (1, Some(comp.clone()), 0));
}
