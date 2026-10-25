//! The eviction's atomic move, release list and released-hop evidence (#893).

use crate::admit_fast::{
    move_sql, release_pot_spends_sql, ColumnInfo, ReleasedSpend, CLOSE_EVICTION_MARKER_SQL,
    MOVED_TABLES, OPEN_EVICTION_MARKER_SQL,
};
use crate::d1::Query;

#[derive(Default)]
pub(crate) struct Snapshot {
    pub tables: Vec<(&'static str, Vec<ColumnInfo>)>,
    pub released: Vec<ReleasedSpend>,
    pub moved: u64,
    pub beef: Option<Vec<u8>>,
}

pub(crate) fn plan(snapshot: &Snapshot, txid: &str, reason: &str, now_ms: u64) -> Vec<Query> {
    let mut out = vec![Query::new(OPEN_EVICTION_MARKER_SQL)
        .bind(txid)
        .bind(reason)
        .bind(now_ms)];
    for (table, keys) in MOVED_TABLES {
        let Some((_, cols)) = snapshot.tables.iter().find(|(name, _)| name == table) else {
            continue;
        };
        for key in *keys {
            if !cols.iter().any(|c| c.name == *key) {
                continue;
            }
            let (copy, delete) = move_sql(table, key, cols);
            out.push(Query::new(copy).bind(now_ms).bind(reason).bind(txid));
            out.push(Query::new(delete).bind(txid));
        }
    }
    if let Some((_, cols)) = snapshot.tables.iter().find(|(t, _)| *t == "pot_records") {
        out.push(Query::new(release_pot_spends_sql(cols)).bind(txid));
    }
    out.push(
        Query::new(CLOSE_EVICTION_MARKER_SQL)
            .bind(txid)
            .bind(reason)
            .bind(now_ms)
            .bind(snapshot.moved)
            .bind(serde_json::to_string(&snapshot.released).unwrap_or_else(|_| "[]".into())),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admit_fast::{create_shadow_sql, shadow_table};
    use crate::d1::{QVal, OVERLAY_MIGRATIONS};
    use bsv_rs::{script::LockingScript, transaction::{Beef, Transaction, TransactionInput, TransactionOutput}};
    use rusqlite::{params, Connection};

    fn sql_value(v: &QVal) -> rusqlite::types::Value {
        match v {
            QVal::Null => rusqlite::types::Value::Null,
            QVal::Int(v) => (*v).into(),
            QVal::Text(v) => v.clone().into(),
            QVal::Bool(v) => i64::from(*v).into(),
            QVal::Blob(v) => v.clone().into(),
            QVal::Float(v) => (*v).into(),
        }
    }

    fn execute(db: &Connection, queries: &[Query]) -> rusqlite::Result<()> {
        let transaction = db.unchecked_transaction()?;
        for q in queries {
            transaction.execute(q.sql(), rusqlite::params_from_iter(q.params().iter().map(sql_value)))?;
        }
        transaction.commit()
    }

    fn columns(db: &Connection, table: &str) -> Vec<ColumnInfo> {
        db.prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .unwrap()
            .query_map([], |r| Ok(ColumnInfo {
                name: r.get(1)?, ty: r.get(2)?, notnull: r.get(3)?, dflt_value: r.get(4)?,
            }))
            .unwrap().collect::<Result<_, _>>().unwrap()
    }

    struct Fixture {
        db: Connection,
        snapshot: Snapshot,
        join: String,
        hops: Vec<String>,
        keys: Vec<String>,
    }

    fn fixture() -> Fixture {
        let db = Connection::open_in_memory().unwrap();
        for sql in OVERLAY_MIGRATIONS {
            if let Err(e) = db.execute_batch(sql) {
                assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
            }
        }
        let keys = vec![format!("02{}", "aa".repeat(32)), format!("03{}", "bb".repeat(32))];
        let mut beef = Beef::new();
        let mut hops = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            let mut tx = Transaction::new();
            tx.inputs.push(TransactionInput::new(format!("{i:064x}"), 0));
            tx.outputs.push(TransactionOutput {
                satoshis: Some(20_190 + i as u64),
                locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(
                    &bsv_rs::primitives::hash::hash160(&hex::decode(key).unwrap()),
                )).unwrap(),
                change: false,
            });
            hops.push(tx.id());
            beef.merge_raw_tx(tx.to_binary(), None);
        }
        let join = "cc".repeat(32);
        db.execute("INSERT INTO pot_records (txid,outputIndex,spent,createdAt,pubA,pubB,payPkhA,payPkhB) VALUES (?1,0,0,1,?2,?3,?4,?5)", params![join,keys[0],keys[1],"dd".repeat(20),"ee".repeat(20)]).unwrap();
        for (i, hop) in hops.iter().enumerate() {
            db.execute("INSERT INTO pot_records (txid,outputIndex,spent,spendingTxid,spentConfirmed,createdAt) VALUES (?1,0,1,?2,0,1)", params![hop,join]).unwrap();
            db.execute("INSERT INTO potparty_records (txid,outputIndex,identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,seatSettlePubkey,sigValid,createdAt) VALUES (?1,0,?2,'opponent',?3,?4,0,99,?5,1,1)", params![format!("{i:064x}"),format!("identity-{i}"),"game",join,keys[i]]).unwrap();
        }
        let mut snapshot = Snapshot { beef: Some(beef.to_binary()), moved: 3, ..Snapshot::default() };
        for (table, _) in MOVED_TABLES {
            let cols = columns(&db, table);
            if !cols.is_empty() {
                db.execute_batch(&create_shadow_sql(table, &cols)).unwrap();
                snapshot.tables.push((table, cols));
            }
        }
        snapshot.released = hops.iter().map(|hop| ReleasedSpend { table: "pot_records".into(), txid: hop.clone(), vout: 0 }).collect();
        Fixture { db, snapshot, join, hops, keys }
    }

    fn count(db: &Connection, table: &str) -> i64 {
        db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn d2_owed_fix518_two_released_hops_have_their_own_keys_and_sats() {
        let f = fixture();
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        let rows: Vec<(String,String,String,i64)> = f.db.prepare("SELECT hopTxid,identity,seatSettlePubkey,hopSats FROM released_hop_evidence ORDER BY identity").unwrap()
            .query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap().collect::<Result<_,_>>().unwrap();
        assert_eq!(rows, vec![(f.hops[0].clone(),"identity-0".into(),f.keys[0].clone(),20_190),(f.hops[1].clone(),"identity-1".into(),f.keys[1].clone(),20_191)]);
    }

    #[test]
    fn d2_owed_unverified_party_has_no_evidence_and_is_counted() {
        let f = fixture();
        f.db.execute("UPDATE potparty_records SET sigValid=0", []).unwrap();
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        assert_eq!(count(&f.db,"released_hop_evidence"),0);
        let skipped: i64 = f.db.query_row("SELECT value FROM ops_counters WHERE name='released_hop_evidence_skipped_total'",[],|r|r.get(0)).unwrap();
        assert_eq!(skipped,2);
    }

    #[test]
    fn d2_owed_readmission_supersedes_without_deleting_evidence() {
        let f = fixture();
        execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).unwrap();
        assert_eq!(count(&f.db,"released_hop_evidence"),2);
        f.db.execute("UPDATE pot_evictions SET readmittedAt=2000 WHERE txid=?1",[&f.join]).unwrap();
        assert_eq!(count(&f.db,"released_hop_evidence"),2);
        let active: i64 = f.db.query_row("SELECT COUNT(*) FROM released_hop_evidence h JOIN pot_evictions e ON e.txid=h.potTxid WHERE e.readmittedAt IS NULL AND h.evictedAt=e.evictedAt",[],|r|r.get(0)).unwrap();
        assert_eq!(active,0);
    }

    #[test]
    fn d2_owed_faulted_evidence_insert_leaves_no_shadow_move_or_release() {
        let f = fixture();
        f.db.execute_batch("CREATE TRIGGER fault_evidence BEFORE INSERT ON released_hop_evidence BEGIN SELECT RAISE(ABORT,'fault evidence'); END").unwrap();
        assert!(execute(&f.db, &plan(&f.snapshot, &f.join, "REJECTED", 1_000)).is_err());
        assert_eq!(count(&f.db,"pot_records"),3);
        assert_eq!(count(&f.db,"potparty_records"),2);
        assert_eq!(count(&f.db,&shadow_table("pot_records")),0);
        assert_eq!(count(&f.db,&shadow_table("potparty_records")),0);
        assert_eq!(count(&f.db,"pot_evictions"),0);
        let spent: i64 = f.db.query_row("SELECT COUNT(*) FROM pot_records WHERE spendingTxid=?1",[&f.join],|r|r.get(0)).unwrap();
        assert_eq!(spent,2);
    }

    #[test]
    fn d2_owed_eviction_move_statements_keep_their_order_and_text() {
        let f = fixture();
        let queries = plan(&f.snapshot, &f.join, "REJECTED", 1_000);
        let actual: Vec<_> = queries.iter().filter(|q|q.sql().starts_with("INSERT INTO \"") || q.sql().starts_with("DELETE FROM \"")).map(Query::sql).collect();
        let mut expected = Vec::new();
        for (table, keys) in MOVED_TABLES {
            let cols = columns(&f.db,table);
            for key in *keys {
                if cols.iter().any(|c|c.name==*key) {
                    let (copy,delete)=move_sql(table,key,&cols);
                    expected.extend([copy,delete]);
                }
            }
        }
        assert_eq!(actual,expected);
    }
}
