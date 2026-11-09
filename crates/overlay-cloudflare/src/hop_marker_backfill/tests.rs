//! MARKER-BACKFILL pins (bsv-low #1146): the pass over a real SQLite port running the SHIPPED statements
//! ([`STATE_SQL`], [`PAGE_SQL`], [`SAVE_SQL`]) and the SHIPPED hopparty write (`hopparty_insert_query`, the value
//! `D1HoppartyStorage::store_record` executes) under the production migrations, with the derived and owed triggers.

use super::*;
use crate::d1_discovery::hopparty_insert_query;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

fn schema() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for sql in crate::d1::OVERLAY_MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            let msg = e.to_string().to_ascii_lowercase();
            assert!(msg.contains("duplicate column"), "{e}\n{sql}");
        }
    }
    conn
}

fn values(q: &crate::d1::Query) -> Vec<rusqlite::types::Value> {
    q.params()
        .iter()
        .map(|p| match p {
            crate::d1::QVal::Null => rusqlite::types::Value::Null,
            crate::d1::QVal::Int(i) => rusqlite::types::Value::Integer(*i),
            crate::d1::QVal::Text(s) => rusqlite::types::Value::Text(s.clone()),
            crate::d1::QVal::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
            crate::d1::QVal::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
            crate::d1::QVal::Float(f) => rusqlite::types::Value::Real(*f),
        })
        .collect()
}

/// The pass's port over real SQLite: the shipped statements, each call counted.
struct Sqlite {
    conn: rusqlite::Connection,
    calls: RefCell<BTreeMap<&'static str, u32>>,
    beef_reads: RefCell<Vec<String>>,
    stamp: Cell<i64>,
    fail_beef: RefCell<Option<String>>,
}

impl Sqlite {
    fn new() -> Self {
        Self {
            conn: schema(),
            calls: RefCell::default(),
            beef_reads: RefCell::default(),
            stamp: Cell::new(1_700_000_100),
            fail_beef: RefCell::default(),
        }
    }
    fn count(&self, what: &'static str) {
        *self.calls.borrow_mut().entry(what).or_default() += 1;
    }
    fn calls(&self) -> BTreeMap<&'static str, u32> {
        std::mem::take(&mut *self.calls.borrow_mut())
    }
    fn int(&self, sql: &str) -> i64 {
        self.conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }
}

#[async_trait(?Send)]
impl Port for Sqlite {
    async fn state(&self) -> Result<Option<State>, String> {
        self.count("state");
        let mut stmt = self.conn.prepare(STATE_SQL).unwrap();
        let mut rows = stmt.query([]).unwrap();
        Ok(rows.next().unwrap().map(|r| {
            let n = |i: usize| r.get::<_, i64>(i).unwrap() as u64;
            State {
                cursor: r.get(0).unwrap(),
                done: r.get::<_, i64>(1).unwrap() != 0,
                totals: Counts {
                    scanned: n(2),
                    bodies: n(3),
                    stored: n(4),
                    invalid: n(5),
                    no_marker: n(6),
                    missing_beef: n(7),
                    unparseable: n(8),
                    faults: n(9),
                },
            }
        }))
    }
    async fn page(&self, after: i64, rows: u32) -> Result<Vec<PageRow>, String> {
        self.count("page");
        let mut stmt = self.conn.prepare(PAGE_SQL).unwrap();
        let out = stmt
            .query_map(rusqlite::params![after, rows], |r| {
                let vouts: Option<String> = r.get(5)?;
                Ok(PageRow {
                    rowid: r.get(0)?,
                    txid: r.get(1)?,
                    first: r.get::<_, Option<i64>>(2)?.unwrap_or(0) != 0,
                    marked: r.get::<_, i64>(3)? != 0,
                    held: r.get::<_, i64>(4)? != 0,
                    vouts: parse_vouts(vouts.as_deref()),
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        Ok(out)
    }
    async fn beef(&self, txid: &str) -> Result<Option<Vec<u8>>, String> {
        self.count("beef");
        self.beef_reads.borrow_mut().push(txid.to_string());
        if self.fail_beef.borrow().as_deref() == Some(txid) {
            return Err("injected read fault".into());
        }
        let mut stmt = self
            .conn
            .prepare("SELECT beef FROM pot_beefs WHERE txid = ?1")
            .unwrap();
        let mut rows = stmt.query([txid]).unwrap();
        Ok(rows.next().unwrap().map(|r| r.get(0).unwrap()))
    }
    async fn store(&self, record: &HoppartyRecord) -> Result<(), String> {
        self.count("store");
        let insert = hopparty_insert_query(record, self.stamp.get());
        let q = insert.query();
        self.conn
            .execute(q.sql(), rusqlite::params_from_iter(values(q).iter()))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    async fn save(&self, cursor: i64, done: bool, counts: &Counts) -> Result<(), String> {
        self.count("save");
        let q = save_query(cursor, done, counts);
        self.conn
            .execute(q.sql(), rusqlite::params_from_iter(values(&q).iter()))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

const CHANGE: &str = "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac";

/// A hop body as the client builds it: the stake P2PKH to hash160(K) at vout 0, `marker` (when given) at vout 1, a
/// change P2PKH after it; as an atomic BEEF.
fn hop_body(marker_hex: Option<&str>, nonce: u8) -> (Vec<u8>, String) {
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};
    let m = overlay_discovery::hopparty::parse_hopparty_marker(
        &hex::decode(overlay_discovery::hopparty::GOLDEN_HOPPARTY_HEX).unwrap(),
    )
    .unwrap();
    let lock = overlay_discovery::hopparty::validity::expected_hop_lock_hex(&m.seat_settle_pubkey)
        .unwrap();
    let mut tx = Tx::new();
    tx.add_input(TransactionInput::new(format!("{nonce:02x}").repeat(32), 0))
        .unwrap();
    let mut outs = vec![(m.hop_sats, lock)];
    if let Some(marker) = marker_hex {
        outs.push((0, marker.to_string()));
    }
    outs.push((1_234, CHANGE.to_string()));
    for (sats, script) in outs {
        tx.add_output(TransactionOutput {
            satoshis: Some(sats),
            locking_script: LockingScript::from_hex(&script).unwrap(),
            change: false,
        })
        .unwrap();
    }
    let beef = bsv_rs::transaction::Beef::from_binary(&tx.to_beef(true).unwrap())
        .unwrap()
        .to_binary_atomic(&tx.id())
        .unwrap();
    (beef, tx.id())
}

/// The golden marker with its seat signature's last byte flipped.
fn forged_marker() -> String {
    let golden = overlay_discovery::hopparty::GOLDEN_HOPPARTY_HEX;
    let m =
        overlay_discovery::hopparty::parse_hopparty_marker(&hex::decode(golden).unwrap()).unwrap();
    let seat_sig = hex::encode(&m.seat_sig);
    let mut forged_sig = seat_sig.clone();
    let tail = forged_sig.split_off(forged_sig.len() - 2);
    forged_sig.push_str(if tail == "00" { "01" } else { "00" });
    golden.replacen(&seat_sig, &forged_sig, 1)
}

/// The index's rows of a hop admitted under `tm_lowfund`: its P2PKH outputs (0 and the change) and its body.
fn seed_hop(db: &Sqlite, txid: &str, vouts: &[u32], beef: Option<&[u8]>) {
    for v in vouts {
        db.conn
            .execute(
                "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) \
                 VALUES (?1, ?2, 0, 1700000000, 'p2pkh')",
                rusqlite::params![txid, v],
            )
            .unwrap();
    }
    if let Some(beef) = beef {
        db.conn
            .execute(
                "INSERT INTO pot_beefs (txid, beef, createdAt) VALUES (?1, ?2, 1700000000)",
                rusqlite::params![txid, beef],
            )
            .unwrap();
    }
}

fn golden() -> overlay_discovery::hopparty::HoppartyMarker {
    overlay_discovery::hopparty::parse_hopparty_marker(
        &hex::decode(overlay_discovery::hopparty::GOLDEN_HOPPARTY_HEX).unwrap(),
    )
    .unwrap()
}

/// Pin 1 (and 3): one pass stores exactly the hop's marker row from its stored body (latched by the one gate, its
/// container facts decoded), the hop-link trigger links it to the pot that spent the hop committing K, the owed dirty
/// trigger marks the marker's identity as the live path's INSERT does, the cursor moves past the hop's rows, the short
/// page stamps done, and a later pass runs ONE statement (the state read) and reads no body.
#[tokio::test]
async fn marker_backfill_a_one_pass_stores_the_hops_marker_row_from_its_stored_body() {
    let m = golden();
    let db = Sqlite::new();
    let (beef, t) = hop_body(Some(overlay_discovery::hopparty::GOLDEN_HOPPARTY_HEX), 0x51);
    // The pot that spent the hop, committing K as seat A; the hop's stake spent by it.
    let pot = "9a".repeat(32);
    let settle = hex::encode(&m.seat_settle_pubkey);
    db.conn
        .execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind, paramsDecoded, \
             recoveryHeight, pubA, pubB) VALUES (?1, 0, 0, 1700000000, 'covenant', 1, 900000, ?2, ?3)",
            rusqlite::params![pot, settle, "02".to_string() + &"bb".repeat(32)],
        )
        .unwrap();
    seed_hop(&db, &t, &[0, 2], Some(&beef));
    db.conn
        .execute(
            "UPDATE pot_records SET spent = 1, spendingTxid = ?2 WHERE txid = ?1 AND outputIndex = 0",
            rusqlite::params![t, pot],
        )
        .unwrap();
    db.conn.execute("DELETE FROM owed_dirty", []).unwrap();
    assert_eq!(db.int("SELECT COUNT(*) FROM hopparty_records"), 0);

    let r = run_pass(&db, HOP_MARKER_BACKFILL_ROWS, HOP_MARKER_BACKFILL_BODIES).await;
    assert_eq!(
        r.counts,
        Counts {
            scanned: 2,
            bodies: 1,
            stored: 1,
            ..Default::default()
        }
    );
    assert!(r.done, "the page came back short");
    let last: i64 = db.int(&format!(
        "SELECT MAX(rowid) FROM pot_records WHERE txid = '{t}'"
    ));
    assert_eq!(r.cursor, last, "the cursor moved past the hop's rows");
    let row: (String, String, i64, i64, String, Option<String>, Option<i64>, i64, i64) = db
        .conn
        .query_row(
            "SELECT txid, identity, outputIndex, hopVout, seatSettlePubkey, hopLockHex, hopSatsOnChain, \
             markerValid, containerOutputs FROM hopparty_records",
            [],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                ))
            },
        )
        .unwrap();
    let lock = overlay_discovery::hopparty::validity::expected_hop_lock_hex(&m.seat_settle_pubkey)
        .unwrap();
    assert_eq!(
        row,
        (
            t.clone(),
            hex::encode(&m.identity),
            1,
            0,
            settle.clone(),
            Some(lock),
            Some(m.hop_sats as i64),
            1,
            3
        )
    );
    // derive_hop_insert: the verified marker links its identity to the pot that spent the hop.
    let links: Vec<(String, String)> = db
        .conn
        .prepare("SELECT identity, potTxid FROM hop_pot_links")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(links, vec![(hex::encode(&m.identity), pot.clone())]);
    // Pin 3: the owed dirty mark the backfill's INSERT leaves.
    let marks: Vec<(String, String, String, String)> = db
        .conn
        .prepare(
            "SELECT identity, gameId, source, sourceKey FROM owed_dirty WHERE source = 'hop' \
             ORDER BY identity",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(
        marks.contains(&(
            hex::encode(&m.identity),
            hex::encode(m.game_id),
            "hop".into(),
            format!("{t}:0")
        )),
        "{marks:?}"
    );
    let s = db.calls();
    assert_eq!(
        (s["state"], s["page"], s["beef"], s["store"], s["save"]),
        (1, 1, 1, 1, 1),
        "one state read, one page, one body, one insert, one state write"
    );

    // The later pass: one primary-key read, nothing else.
    let r2 = run_pass(&db, HOP_MARKER_BACKFILL_ROWS, HOP_MARKER_BACKFILL_BODIES).await;
    assert!(r2.already_done && r2.counts == Counts::default());
    assert_eq!(
        db.calls().into_iter().collect::<Vec<_>>(),
        vec![("state", 1)]
    );
    let served = state_json(&db.state().await.unwrap().unwrap());
    assert_eq!(served["done"], true);
    assert_eq!(served["stored"], 1);
    assert_eq!(served["cursor"], last);
}

/// Pin 1 continued: the classes, over pages of two rows and one body a pass. A txid a hopparty row already names is
/// skipped with no body read; a body with no marker is `noMarker`, stored nothing, never revisited; a forged seat
/// signature is stored latched 0; a txid with no body is `missingBeef` and passed; a body `STORED_BEEF_LIMITS`
/// refuses is `unparseable` and passed; a read fault stops the pass before that txid, which the next pass reads; each
/// body is read once over the whole backfill, which ends `done` on its short page.
#[tokio::test]
async fn marker_backfill_b_every_class_is_counted_once_and_the_cursor_never_loses_a_hop() {
    let db = Sqlite::new();
    let golden_hex = overlay_discovery::hopparty::GOLDEN_HOPPARTY_HEX;
    let (b1, t1) = hop_body(Some(golden_hex), 1);
    let (b2, t2) = hop_body(Some(golden_hex), 2);
    let (b3, t3) = hop_body(None, 3);
    let forged = forged_marker();
    let (b4, t4) = hop_body(Some(&forged), 4);
    let (_, t5) = hop_body(Some(golden_hex), 5);
    let (_, t6) = hop_body(Some(golden_hex), 6);
    seed_hop(&db, &t1, &[0, 2], Some(&b1));
    seed_hop(&db, &t2, &[0, 2], Some(&b2));
    // T2's marker is already indexed (the live path, or the client's own submit).
    let pre: Vec<_> = records_of_body(&b2, &t2, &[0, 2]).unwrap();
    db.store(&pre[0]).await.unwrap();
    seed_hop(&db, &t3, &[0, 1], Some(&b3));
    seed_hop(&db, &t4, &[0, 2], Some(&b4));
    seed_hop(&db, &t5, &[0, 2], None);
    seed_hop(&db, &t6, &[0, 2], Some(b"not a beef"));
    db.calls();
    *db.fail_beef.borrow_mut() = Some(t4.clone());

    let mut passes = 0;
    let mut faulted = false;
    loop {
        passes += 1;
        assert!(passes < 50, "the backfill converges");
        let r = run_pass(&db, 2, 1).await;
        if r.counts.faults > 0 {
            faulted = true;
            *db.fail_beef.borrow_mut() = None;
        }
        if r.done {
            break;
        }
    }
    assert!(faulted, "the injected fault ran");
    let s = db.state().await.unwrap().unwrap();
    assert_eq!(
        s.totals,
        Counts {
            scanned: 12,
            bodies: 4,
            stored: 2,
            invalid: 1,
            no_marker: 1,
            missing_beef: 1,
            unparseable: 1,
            faults: 1,
        }
    );
    let mut reads = db.beef_reads.borrow().clone();
    reads.sort();
    let mut want = vec![t1.clone(), t3.clone(), t4.clone(), t4.clone(), t6.clone()];
    want.sort();
    assert_eq!(
        reads, want,
        "T2 (marked) and T5 (no body) never read; T4 read again after its fault"
    );
    let latched: Vec<(String, i64)> = db
        .conn
        .prepare("SELECT txid, markerValid FROM hopparty_records ORDER BY txid")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut want = vec![(t1, 1), (t2, 1), (t4, 0)];
    want.sort();
    assert_eq!(latched, want, "the forged seat signature stored, latched 0");
    db.calls();
    let r = run_pass(&db, 2, 1).await;
    assert!(r.already_done);
    assert_eq!(
        db.calls().into_iter().collect::<Vec<_>>(),
        vec![("state", 1)]
    );
}

/// Pin 2 (parity): for one body the record the backfill stores is the record the `tm_lowfund` arm stores, field for
/// field (both through `records_for_body`; `createdAt` is the store's stamp on both).
#[tokio::test]
async fn marker_backfill_c_parity_with_the_tm_lowfund_arm() {
    use overlay_discovery::hopparty::lookup_service::HoppartyLookupService;
    use overlay_discovery::hopparty::storage::MemoryHoppartyStorage;
    use overlay_engine::lookup_service::LookupService;
    use overlay_engine::types::OutputAdmittedByTopic;
    for marker in [
        overlay_discovery::hopparty::GOLDEN_HOPPARTY_HEX.to_string(),
        forged_marker(),
    ] {
        let (beef, txid) = hop_body(Some(&marker), 0x61);
        let store = std::rc::Rc::new(MemoryHoppartyStorage::new());
        let svc = HoppartyLookupService::new(store.clone());
        for index in [0, 2] {
            svc.output_admitted_by_topic(&OutputAdmittedByTopic::WholeTx {
                atomic_beef: beef.clone(),
                output_index: index,
                topic: "tm_lowfund".into(),
                off_chain_values: None,
            })
            .await
            .unwrap();
        }
        let live = store.all_records();
        let backfilled = records_of_body(&beef, &txid, &[0, 2]).unwrap();
        assert_eq!(backfilled, live, "one builder, one record");
        assert_eq!(backfilled.len(), 1);

        // The same INSERT through the one store: the live record and the backfilled one write the same row.
        assert_eq!(stored_row(&backfilled[0]).await, stored_row(&live[0]).await);
    }
    // A body whose only marker names an output the index does not hold stores nothing.
    let (beef, txid) = hop_body(Some(overlay_discovery::hopparty::GOLDEN_HOPPARTY_HEX), 0x62);
    assert!(records_of_body(&beef, &txid, &[2]).unwrap().is_empty());
    // A body read under another txid does not parse hash-bound.
    assert!(records_of_body(&beef, &"00".repeat(32), &[0, 2]).is_err());
}

async fn stored_row(r: &HoppartyRecord) -> Vec<rusqlite::types::Value> {
    let db = Sqlite::new();
    db.store(r).await.unwrap();
    db.conn
        .query_row(
            "SELECT identity, opponentIdentity, gameId, hopVout, hopSats, seatSettlePubkey, seatSigHex, \
             identitySigHex, hopLockHex, hopSatsOnChain, containerOutputs, txid, outputIndex, sizeBytes, \
             feeSats, markerValid FROM hopparty_records",
            [],
            |row| {
                (0..16)
                    .map(|i| row.get::<_, rusqlite::types::Value>(i))
                    .collect::<Result<Vec<_>, _>>()
            },
        )
        .unwrap()
}

/// Pin 5 (the plans): the page reads `pot_records` by its rowid range and every per-row fact by a key; nothing scans a
/// table. The state read and write are primary-key statements.
#[test]
fn marker_backfill_d_the_page_reads_in_proportion_to_its_rows() {
    let db = Sqlite::new();
    let plan = |sql: &str, binds: usize| -> Vec<String> {
        let explain = format!("EXPLAIN QUERY PLAN {sql}");
        let mut stmt = db.conn.prepare(&explain).unwrap();
        let params: Vec<i64> = vec![0; binds];
        stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| {
            r.get::<_, String>(3)
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
    };
    let page = plan(PAGE_SQL, 2);
    assert!(
        page.iter()
            .any(|l| l.contains("SEARCH r USING INTEGER PRIMARY KEY (rowid>?)")),
        "{page:?}"
    );
    for l in &page {
        assert!(!l.starts_with("SCAN"), "a scan in the page: {page:?}");
    }
    let state = plan(STATE_SQL, 0);
    assert!(state.iter().all(|l| !l.starts_with("SCAN")), "{state:?}");
}
