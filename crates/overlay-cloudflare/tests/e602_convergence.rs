use async_trait::async_trait;
use bsv_overlay_cloudflare::{
    admit_fast::*,
    d1::{QVal, OVERLAY_MIGRATIONS},
    proof_fetcher::NetworkPresence,
};
use overlay_engine::gasp::{AncestorFetcher, FetchedAncestor, GASPError};
use rusqlite::{Connection, OptionalExtension};

struct Sqlite(Connection);
#[async_trait(?Send)]
impl bsv_overlay_cloudflare::conflict_repair::RepairDb for Sqlite {
    async fn repair_page(
        &self,
        after: i64,
    ) -> Result<Vec<bsv_overlay_cloudflare::conflict_repair::Eviction>, String> {
        use bsv_overlay_cloudflare::conflict_repair::{Eviction, PAGE_SQL};
        let mut q = self.0.prepare(PAGE_SQL).map_err(|e| e.to_string())?;
        let rows = q
            .query_map([after], |r| {
                Ok(Eviction {
                    position: r.get(0)?,
                    txid: r.get(1)?,
                    reason: r.get(2)?,
                    readmitted_at: r.get(3)?,
                })
            })
            .map_err(|e| e.to_string())?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    }
}
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
    async fn evict_conflict(
        &self,
        txid: &str,
        reason: &str,
        now_ms: u64,
        repointed: &[ReleasedSpend],
    ) -> Result<ConflictEvictOutcome, String> {
        let txid = txid.to_ascii_lowercase();
        let result = evict_core(self, &txid, reason, now_ms).await;
        assert!(result.faults.is_empty(), "{:?}", result.faults);
        assert!(result.yielded_to_readmission.is_none());
        // bsv-low #605: the production eviction (`evict_txid_everywhere_repointed`) then releases the evicted tx's
        // unconfirmed pointers, adds the re-pointed outpoints, merges with the ledger's list and closes the ledger
        // row, each with the shipped statement; modelled here so the pins see what the release and the ledger leave
        let cols = self.columns("pot_records").await?;
        let mut released: Vec<ReleasedSpend> = {
            let mut q = self.0.prepare(&select_pot_spends_sql(&cols)).unwrap();
            let rows = q
                .query_map([&txid], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            rows.into_iter()
                .filter(|(_, _, confirmed)| *confirmed == 0)
                .map(|(t, v, _)| ReleasedSpend {
                    table: "pot_records".into(),
                    txid: t.to_ascii_lowercase(),
                    vout: v as u32,
                })
                .collect()
        };
        self.exec(
            &release_pot_spends_sql(&cols),
            vec![QVal::Text(txid.clone())],
        )
        .await?;
        released.extend(repointed.iter().cloned());
        let existing: Option<String> = self
            .0
            .query_row(RELEASED_SPENDS_SQL, [&txid], |r| r.get(0))
            .optional()
            .unwrap()
            .flatten();
        let merged = merge_released(existing.as_deref(), &released);
        self.exec(
            CLOSE_EVICTION_MARKER_SQL,
            vec![
                QVal::Text(txid.clone()),
                QVal::Text(reason.into()),
                QVal::Int(now_ms as i64),
                QVal::Int(result.moved as i64),
                QVal::Text(serde_json::to_string(&merged).unwrap()),
            ],
        )
        .await?;
        Ok(ConflictEvictOutcome::Evicted)
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
        // bsv-low #825: the draw binds its time (the fault memo's hold)
        db.0.query_row(CONFLICT_CANDIDATES_SQL, [3000], |r| Ok((r.get(0)?, r.get(1)?)))
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
    // the E606 lens's M1: the ledger still NAMES the re-pointed hop beside the released one, so the app layer's
    // `evicted_pot_homes` finds the game's home and the owed story reads "spent outside this game" (on `7b285f8` the
    // list held `other_hop` alone)
    let ledger: String =
        db.0.query_row(RELEASED_SPENDS_SQL, [&txid], |r| r.get(0))
            .unwrap();
    let mut named: Vec<(String, u32)> = serde_json::from_str::<Vec<ReleasedSpend>>(&ledger)
        .unwrap()
        .into_iter()
        .map(|r| {
            assert_eq!(r.table, "pot_records");
            (r.txid, r.vout)
        })
        .collect();
    named.sort();
    assert_eq!(
        named,
        vec![(hop.clone(), 0), (other_hop.clone(), 0)],
        "releasedSpends names both hops: {ledger}"
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

/// bsv-low #649 M2 (the lens's yielded outcome): a conflict eviction that YIELDED to a readmission landing under
/// it does not stand, so the pass answers "not evicted" and the summary counts no eviction. On `c8e4d8f` the port
/// answered `()` and the pass `Ok(true)` whatever the wrapper did: an eviction logged and counted over a served pot.
struct Yielding {
    asked: std::cell::Cell<u32>,
}
#[async_trait(?Send)]
impl ConflictEviction for Yielding {
    async fn evict_conflict(
        &self,
        _: &str,
        _: &str,
        _: u64,
        _: &[ReleasedSpend],
    ) -> Result<ConflictEvictOutcome, String> {
        self.asked.set(self.asked.get() + 1);
        Ok(ConflictEvictOutcome::Yielded)
    }
    async fn record_conflict_spender(
        &self,
        _: &str,
        _: u32,
        _: &str,
        _: &str,
    ) -> Result<bool, String> {
        Ok(false)
    }
}
#[tokio::test]
async fn e649_m2_a_yielded_conflict_eviction_is_neither_logged_nor_counted() {
    let hop = "dd".repeat(32);
    let subject = spending(&hop, 40_000);
    let txid = subject.id();
    let raw = subject.to_binary();
    let evidence = Evidence {
        raw: spending(&hop, 39_000).to_hex(),
        height: Some(970710),
    };
    let store = Yielding {
        asked: std::cell::Cell::new(0),
    };
    let result = converge_confirmed_conflict(&store, &evidence, &evidence, &txid, &raw, 3000).await;
    assert_eq!(
        store.asked.get(),
        1,
        "the confirmed competitor reached the eviction"
    );
    assert_eq!(
        result,
        Ok(false),
        "a yielded eviction does not stand: the pass must not answer evicted"
    );
    let mut summary = ConflictPassSummary::default();
    summary.note(&result);
    assert_eq!(
        (summary.scanned, summary.evicted, summary.faults),
        (1, 0, 0),
        "a yielded eviction is scanned, never counted evicted"
    );
}

mod e607 {
    use super::*;
    use bsv_overlay_cloudflare::conflict_repair::*;

    fn database() -> Sqlite {
        let db = Sqlite(Connection::open_in_memory().unwrap());
        for sql in OVERLAY_MIGRATIONS {
            if let Err(e) = db.0.execute_batch(sql) {
                assert!(e.to_string().contains("duplicate column"), "{e}");
            }
        }
        db
    }

    fn released(db: &Sqlite, n: u32) -> (String, String, String) {
        let hop = format!("{n:064x}");
        let join = format!("{:064x}", 1_000 + n);
        let competitor = format!("{:064x}", 2_000 + n);
        db.0.execute("INSERT INTO pot_records(txid, outputIndex, spent, spentConfirmed, createdAt) VALUES (?, 0, 0, 0, 1)", [&hop]).unwrap();
        db.0.execute(
            "INSERT INTO pot_evictions(txid, reason, evictedAt) VALUES (?, ?, 1)",
            rusqlite::params![
                join,
                conflict_eviction_reason(&hop, 0, &competitor, 970_710)
            ],
        )
        .unwrap();
        (hop, join, competitor)
    }

    fn pointer(db: &Sqlite, hop: &str) -> (i64, Option<String>, i64) {
        db.0.query_row(
            "SELECT spent, spendingTxid, spentConfirmed FROM pot_records WHERE txid = ?",
            [hop],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn e607_pages_sixteen_and_permanently_stops_at_the_original_window() {
        let db = database();
        let rows: Vec<_> = (1..=33).map(|n| released(&db, n)).collect();
        let first = repair_pass(&db, 10).await.unwrap();
        assert_eq!((first.scanned, first.repaired, first.done), (16, 16, false));
        let later = released(&db, 34);
        let second = repair_pass(&db, 20).await.unwrap();
        assert_eq!(
            (second.scanned, second.repaired, second.done),
            (16, 16, false)
        );
        let third = repair_pass(&db, 30).await.unwrap();
        assert_eq!((third.scanned, third.repaired, third.done), (1, 1, true));
        for (hop, _, competitor) in rows {
            assert_eq!(pointer(&db, &hop), (1, Some(competitor), 0));
        }
        let changes = db.0.total_changes();
        let again = repair_pass(&db, 40).await.unwrap();
        assert_eq!((again.scanned, again.repaired, again.done), (0, 0, true));
        assert_eq!(
            db.0.total_changes(),
            changes,
            "the done stamp leaves even the cursor untouched"
        );
        assert_eq!(
            pointer(&db, &later.0),
            (0, None, 0),
            "later evictions are outside the one-time window"
        );
        let done: i64 =
            db.0.query_row("SELECT done_at FROM conflict_spender_repair", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(done, 30);
    }

    #[tokio::test]
    async fn e607_only_open_valid_conflicts_move_unconfirmed_evicted_or_empty_pointers() {
        let db = database();
        let rows: Vec<_> = (1..=9).map(|n| released(&db, n)).collect();
        db.0.execute(
            "UPDATE pot_records SET spent = 1, spendingTxid = ? WHERE txid = ?",
            rusqlite::params![rows[1].1, rows[1].0],
        )
        .unwrap();
        db.0.execute(
            "UPDATE pot_records SET spent = 1, spendingTxid = ?, spentConfirmed = 1 WHERE txid = ?",
            rusqlite::params![rows[2].1, rows[2].0],
        )
        .unwrap();
        let foreign = "ff".repeat(32);
        db.0.execute(
            "UPDATE pot_records SET spent = 1, spendingTxid = ? WHERE txid = ?",
            rusqlite::params![foreign, rows[3].0],
        )
        .unwrap();
        db.0.execute(
            "UPDATE pot_evictions SET readmittedAt = 2 WHERE txid = ?",
            [&rows[4].1],
        )
        .unwrap();
        db.0.execute(
            "UPDATE pot_evictions SET reason = ? WHERE txid = ?",
            rusqlite::params![
                conflict_eviction_reason(&rows[5].0, 0, &rows[5].2, 0),
                rows[5].1
            ],
        )
        .unwrap();
        db.0.execute("DELETE FROM pot_records WHERE txid = ?", [&rows[6].0])
            .unwrap();
        db.0.execute(
            "UPDATE pot_records SET spent = 1, spendingTxid = ? WHERE txid = ?",
            rusqlite::params![rows[7].2, rows[7].0],
        )
        .unwrap();
        db.0.execute(
            "UPDATE pot_evictions SET reason = 'REJECTED' WHERE txid = ?",
            [&rows[8].1],
        )
        .unwrap();
        let summary = repair_pass(&db, 10).await.unwrap();
        assert_eq!(
            (
                summary.scanned,
                summary.repaired,
                summary.skipped,
                summary.done
            ),
            (9, 2, 7, true)
        );
        for row in &rows[..2] {
            assert_eq!(pointer(&db, &row.0), (1, Some(row.2.clone()), 0));
        }
        assert_eq!(pointer(&db, &rows[2].0), (1, Some(rows[2].1.clone()), 1));
        assert_eq!(pointer(&db, &rows[3].0), (1, Some(foreign), 0));
        for n in [4, 5, 8] {
            assert_eq!(pointer(&db, &rows[n].0), (0, None, 0));
        }
        assert!(
            recorded_competitor(&conflict_eviction_reason("not-a-txid", 0, &rows[0].2, 1))
                .is_none()
        );
        assert!(recorded_competitor(&format!(
            "{} trailing",
            conflict_eviction_reason(&rows[0].0, 0, &rows[0].2, 1)
        ))
        .is_none());
    }

    #[tokio::test]
    async fn e607_a_write_fault_retries_the_failed_row_without_stamping_done() {
        let db = database();
        let first = released(&db, 1);
        let second = released(&db, 2);
        db.0.execute_batch(&format!("CREATE TRIGGER fail_repair BEFORE UPDATE ON pot_records WHEN OLD.txid = '{}' BEGIN SELECT RAISE(ABORT, 'injected write fault'); END", second.0)).unwrap();
        let summary = repair_pass(&db, 10).await.unwrap();
        assert_eq!(
            (
                summary.scanned,
                summary.repaired,
                summary.faults,
                summary.done
            ),
            (2, 1, 1, false)
        );
        assert_eq!(db.query_i64(STATE_SQL, vec![]).await.unwrap(), Some(1));
        assert_eq!(pointer(&db, &first.0), (1, Some(first.2), 0));
        assert_eq!(pointer(&db, &second.0), (0, None, 0));
        db.0.execute_batch("DROP TRIGGER fail_repair").unwrap();
        let retry = repair_pass(&db, 20).await.unwrap();
        assert_eq!(
            (retry.scanned, retry.repaired, retry.faults, retry.done),
            (1, 1, 0, true)
        );
        assert_eq!(pointer(&db, &second.0), (1, Some(second.2), 0));
    }

    #[test]
    fn e607_append_only_migration_and_the_page_uses_the_rowid_range() {
        assert_eq!(OVERLAY_MIGRATIONS.len(), 346);
        assert_eq!(OVERLAY_MIGRATIONS[345], REPAIR_CREATE);
        let db = database();
        db.0.execute_batch(REPAIR_CREATE).unwrap();
        let plan: Vec<String> =
            db.0.prepare(&format!("EXPLAIN QUERY PLAN {PAGE_SQL}"))
                .unwrap()
                .query_map([0], |r| r.get(3))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        assert!(
            plan.iter().any(|s| s
                .contains("SEARCH pot_evictions USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)")),
            "{plan:?}"
        );
        assert!(
            !plan.iter().any(|s| s.contains("SCAN pot_evictions")),
            "{plan:?}"
        );
        assert_eq!(REPAIR_LIMIT, 16);
    }

    #[test]
    fn e607_the_operator_authenticates_before_any_database_setup_and_the_tick_runs_it() {
        let lib = include_str!("../src/lib.rs");
        let dispatch = &lib[lib.find("async fn dispatch(").unwrap()..];
        let route = dispatch
            .find("crate::conflict_repair::internal_repair(")
            .unwrap();
        assert!(route < dispatch.find("env.d1(").unwrap());
        let source = include_str!("../src/conflict_repair.rs");
        let route = &source[source.find("pub async fn internal_repair(").unwrap()..];
        assert!(route.find("bearer_ok(").unwrap() < route.find("req.bytes()").unwrap());
        assert!(route.find("bearer_ok(").unwrap() < route.find("env.d1(").unwrap());
        assert!(route.contains(r#"Response::error("unauthorized", 401)"#));
        assert!(include_str!("../src/tick.rs")
            .contains("crate::conflict_repair::run_counted(&ops_db).await"));
        assert!(
            include_str!("../src/ops.rs").contains("for name in crate::conflict_repair::COUNTERS")
        );
    }
}
