//! OWED-1173 (bsv-low #1173): a join-refused seat's FILED hop sweep that mined is the index's proof of the hop's
//! spend through its verified proof in `pot_beefs`. A sweep admits no output, so it never has a `transactions` row;
//! after the JOIN's eviction the hop's `pot_records` pointer is never written; the tracker's word for the sweep is the
//! one other word for the spend, and the tracker's eight-day sweep (`stored::SWEEP_FRAMED`) deletes it. The judge
//! read the sweep's proof from `transactions` only, so from then on it reached the index-unspent press and offered a
//! claimable `hop-stranded` sweep for money already home (beta's five hops of 030d9e9f...). Every pin runs over the
//! overlay's migrations on real SQLite: the overlay's own rows, the dirty triggers, the shipped drain and read.
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;
use bsv_rs::primitives::hash::hash160;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Beef, Transaction as Tx, TransactionInput, TransactionOutput};
use owed_db::*;
use serde_json::Value;

const X: &str = "02aa00000000000000000000000000000000000000000000000000000000001173";
const OTHER: &str = "03bb00000000000000000000000000000000000000000000000000000000001173";
/// X's seat settle key K and the opponent's.
const K: &str = "021111111111111111111111111111111111111111111111111111111111111173";
const K2: &str = "032222222222222222222222222222222222222222222222222222222222221173";
const GAME: &str = "1173000000000000000000000000000000000000000000000000000000000001";
const DAY_MS: u64 = 86_400_000;

fn pkh(key: &str) -> String {
    hex::encode(hash160(&hex::decode(key).unwrap()))
}
fn p2pkh(pkh_hex: &str) -> LockingScript {
    let mut lock = vec![0x76, 0xa9, 0x14];
    lock.extend_from_slice(&hex::decode(pkh_hex).unwrap());
    lock.extend_from_slice(&[0x88, 0xac]);
    LockingScript::from_binary(&lock).unwrap()
}
fn out(sats: u64, lock: LockingScript) -> TransactionOutput {
    TransactionOutput {
        satoshis: Some(sats),
        locking_script: lock,
        change: false,
    }
}

struct Game {
    hop: String,
    sweep: String,
}

/// How the sweep's `pot_beefs` row stands.
#[derive(Clone, Copy, PartialEq)]
enum Proof {
    /// `has_proof 1, proof_verified 1, proofHeight`: the overlay's chaintracks-verified latch.
    Verified,
    /// `has_proof 1, proof_verified 0`: a proof the overlay never verified.
    Unverified,
    /// The verified row is stored under ANOTHER txid; the sweep has none.
    OtherTxid,
}

/// A join-refused game: X's hop (P2PKH to hash160(K), 20,000 sats) and the opponent's, the JOIN spending both into
/// the pot, Arcade's refusal evicting the JOIN (its covenant row moved to `pot_records_evicted`, both hops RELEASED:
/// back in `pot_records` unspent, named in `releasedSpends`, X's in `released_hop_evidence`). X's VERIFIED hop
/// marker; X FILES its pre-signed sweep of the hop to its home (`hopsweep_records`); the sweep is SEEN
/// (`network_seen`), mined (`tx_words` `mined`) and its proof stored in `pot_beefs` as `proof`. No `transactions` row
/// for the sweep, no belt evidence, no pointer on the hop.
fn seed(db: &Db, proof: Proof) -> Game {
    let mut hop = Tx::new();
    hop.add_input(TransactionInput::new("41".repeat(32), 0))
        .unwrap();
    hop.add_output(out(20_000, p2pkh(&pkh(K)))).unwrap();
    let mut opp = Tx::new();
    opp.add_input(TransactionInput::new("42".repeat(32), 0))
        .unwrap();
    opp.add_output(out(20_000, p2pkh(&pkh(K2)))).unwrap();
    let mut join = Tx::new();
    join.add_input(TransactionInput::new(hop.id(), 0)).unwrap();
    join.add_input(TransactionInput::new(opp.id(), 0)).unwrap();
    join.outputs
        .push(out(40_000, LockingScript::from_binary(&[0x51]).unwrap()));
    let mut sweep = Tx::new();
    sweep.add_input(TransactionInput::new(hop.id(), 0)).unwrap();
    sweep.add_output(out(19_800, p2pkh(&pkh(K)))).unwrap();
    let (h, o, j, s) = (hop.id(), opp.id(), join.id(), sweep.id());
    let mut beef = Beef::new();
    beef.merge_raw_tx(sweep.to_binary(), None);
    let beef = beef.to_binary();

    let now_ms = db.now.get() as i64;
    let made = (now_ms - 3 * HOUR) / 1000;
    let evicted = now_ms - 2 * HOUR;
    // the two hops, released by the eviction: unspent, no pointer
    db.exec(&format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{h}',0,0,{made},'p2pkh'),('{o}',0,0,{made},'p2pkh');"));
    db.exec(&format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('{X}','{OTHER}','{GAME}',0,20000,'{K}','00','00',2,'{h}',1,{made},1),('{OTHER}','{X}','{GAME}',0,20000,'{K2}','00','00',2,'{o}',1,{made},1);"));
    // the JOIN, evicted on Arcade's refusal of the opponent's hop
    db.exec(&format!("INSERT INTO pot_records_evicted(af_evictedAt,af_reason,txid,outputIndex,spent,createdAt,lockKind,pubA,pubB,payPkhA,payPkhB,paramsDecoded,recoveryHeight,stakeA,stakeB,potSats) VALUES ({evicted},'UTXO_SPENT','{j}',0,0,{made},'covenant','{K}','{K2}','{a}','{b}',1,899000,20000,20000,40000);", a = pkh(K), b = pkh(K2)));
    db.exec(&format!("INSERT INTO pot_evictions(txid,reason,evictedAt,releasedSpends) VALUES ('{j}','UTXO_SPENT (70): {o}:0 utxo already spent by tx {}',{evicted},'[{{\"table\":\"pot_records\",\"txid\":\"{h}\",\"vout\":0}},{{\"table\":\"pot_records\",\"txid\":\"{o}\",\"vout\":0}}]');", "43".repeat(32)));
    db.exec(&format!("INSERT INTO released_hop_evidence(hopTxid,hopVout,identity,gameId,potTxid,seatSettlePubkey,hopSats,evictedAt) VALUES ('{h}',0,'{X}','{GAME}','{j}','{K}',20000,{evicted}),('{o}',0,'{OTHER}','{GAME}','{j}','{K2}',20000,{evicted});"));
    // X's filed sweep, seen and mined
    let filed = (now_ms - HOUR) / 1000;
    db.exec(&format!("INSERT INTO hopsweep_records(identity,gameId,hopTxid,hopVout,sweepTxid,sweepRawHex,txid,outputIndex,createdAt) VALUES ('{X}','{GAME}','{h}',0,'{s}','{}','{}',0,{filed});", hex::encode(sweep.to_binary()), "f1".repeat(32)));
    db.exec(&format!(
        "INSERT INTO network_seen(txid,seenAt,seenSource) VALUES ('{s}',{filed},'taal');"
    ));
    db.exec(&format!("INSERT INTO tx_words(txid,word,tier,height,updated_at) VALUES ('{s}','mined','chain',899400,{filed});"));
    let (proof_txid, verified) = match proof {
        Proof::Verified => (s.clone(), 1),
        Proof::Unverified => (s.clone(), 0),
        Proof::OtherTxid => ("44".repeat(32), 1),
    };
    db.conn
        .borrow()
        .execute(
            "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof, proof_verified, proofHeight) VALUES (?1, ?2, ?3, 1, ?4, 899400)",
            rusqlite::params![proof_txid, beef, filed, verified],
        )
        .unwrap();
    assert_eq!(
        db.int(&format!(
            "SELECT COUNT(*) FROM transactions WHERE txid='{s}'"
        )),
        0
    );
    assert_eq!(db.int("SELECT COUNT(*) FROM hop_chain_probes"), 0);
    Game { hop: h, sweep: s }
}

/// The tracker's eight-day sweep, the SHIPPED statement as `runtime::tick` binds it, eight days and a second after
/// the word: answers the words it deleted.
fn sweep_words(db: &Db) -> Vec<String> {
    db.now.set(db.now.get() + 8 * DAY_MS + 1_000);
    let cutoff = (db.now.get() / 1000).saturating_sub(8 * 86_400) as f64;
    let conn = db.conn.borrow();
    let mut stmt = conn
        .prepare(low_app_layer::tracker_host::stored::SWEEP_FRAMED)
        .unwrap();
    let gone = stmt
        .query_map([cutoff], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    gone
}

fn hop_rows<'a>(body: &'a Value, hop: &str) -> Vec<&'a Value> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["outpoint"] == format!("{hop}:0"))
        .collect()
}
fn presses(body: &Value, hop: &str) -> Vec<Value> {
    hop_rows(body, hop)
        .into_iter()
        .filter(|r| r["family"] == "hop-stranded" && r["facts"]["claimable"] == true)
        .cloned()
        .collect()
}

/// Judge every game of X afresh (a mark per game), as the word's delete does in Phase A, and serve X's list.
async fn rejudged(db: &Db) -> Value {
    let marked = db.now.get() as i64;
    db.exec(&format!("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs,downgrade) VALUES ('{X}','{GAME}','age','age',{marked},0) ON CONFLICT DO NOTHING;"));
    drain_all(db).await;
    served(db, X, Some(900_000)).await
}

/// The issue's pin: no claimable `hop-stranded` press for the hop X's filed, mined sweep spent, (a) while the
/// sweep's word stands and (b) after the tracker's shipped eight-day sweep deletes it. RED on main at (b): the judge
/// read the sweep's proof from `transactions` only, so with the word gone the hop read index-unspent and pressed.
#[tokio::test]
async fn owed1173_a_a_filed_sweeps_verified_proof_in_pot_beefs_ends_the_press() {
    let db = Db::new();
    let g = seed(&db, Proof::Verified);
    let before = rejudged(&db).await;
    assert!(
        presses(&before, &g.hop).is_empty(),
        "(a) no press while the sweep's mined word stands: {before}"
    );
    assert_eq!(sweep_words(&db), vec![g.sweep.clone()], "the word swept");
    let after = rejudged(&db).await;
    assert!(
        presses(&after, &g.hop).is_empty(),
        "(b) no press after the word's delete: {after}"
    );
}

/// The negative: the same game with the sweep's `pot_beefs` row UNVERIFIED (`proof_verified 0`, whatever its
/// `has_proof`): it proves nothing, so after the word's delete the hop still reads index-unspent and presses.
#[tokio::test]
async fn owed1173_b_an_unverified_pot_beefs_row_proves_nothing() {
    let db = Db::new();
    let g = seed(&db, Proof::Unverified);
    assert_eq!(sweep_words(&db), vec![g.sweep.clone()]);
    let after = rejudged(&db).await;
    let p = presses(&after, &g.hop);
    assert_eq!(p.len(), 1, "the press stands: {after}");
    assert_eq!(p[0]["facts"]["claim"], "sweep-hop", "{}", p[0]);
}

/// The negative: a verified `pot_beefs` row for ANOTHER txid proves nothing of this sweep.
#[tokio::test]
async fn owed1173_c_a_verified_proof_of_another_txid_proves_nothing() {
    let db = Db::new();
    let g = seed(&db, Proof::OtherTxid);
    assert_eq!(sweep_words(&db), vec![g.sweep.clone()]);
    let after = rejudged(&db).await;
    assert_eq!(
        presses(&after, &g.hop).len(),
        1,
        "the press stands: {after}"
    );
}

/// The control: an honest stranded hop with no filed sweep (the seat's own index-unspent hop, its marker verified)
/// still gets its press, the verified proofs of other transactions in `pot_beefs` beside it.
#[tokio::test]
async fn owed1173_d_an_honest_stranded_hop_with_no_filed_sweep_still_presses() {
    let db = Db::new();
    let g = seed(&db, Proof::Verified);
    db.exec("DELETE FROM hopsweep_records; DELETE FROM tx_words; DELETE FROM network_seen;");
    let body = rejudged(&db).await;
    let p = presses(&body, &g.hop);
    assert_eq!(p.len(), 1, "the honest press: {body}");
    assert_eq!(p[0]["facts"]["claim"], "sweep-hop", "{}", p[0]);
}
