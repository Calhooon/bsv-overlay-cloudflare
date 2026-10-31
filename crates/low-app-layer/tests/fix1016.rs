//! bsv-low #1016 (L56 pair 13, `spec-admit-fast-join-refused`): the eviction on Arcade's live terminal word that NAMES
//! a competitor (`<hop>:0 utxo already spent by tx <competitor>`) writes that competitor as the hop's unconfirmed
//! spender, so the index-unspent hop's sweep press is withheld at once, never left standing until the belt's look.
//! On the producer path: the overlay's migrations on real SQLite, the reason as the evidence check writes it
//! (`evidence_verdict` over Arcade's live `Fatal` word, wrapped as `refusal_job` wraps it), the overlay's shipped
//! re-point (`repoint_arcade_named`, called by `evict_txid_everywhere` before the eviction), the dirty triggers, the
//! notes, the shipped drain and read.
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;
use async_trait::async_trait;
use bsv_overlay_cloudflare::admit_fast::{
    arcade_named_spends, evidence_verdict, repoint_arcade_named, ColumnInfo, EvidenceVerdict,
    ReleasedSpend, ShadowDb,
};
use bsv_overlay_cloudflare::d1::QVal;
use bsv_overlay_cloudflare::proof_fetcher::ArcadeLook;
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Beef, Transaction as Tx, TransactionInput, TransactionOutput};
use owed_db::*;
use rusqlite::OptionalExtension;
use serde_json::Value;

/// The overlay's eviction port over the pin's one connection.
struct Shadow<'a>(&'a Db);
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
impl ShadowDb for Shadow<'_> {
    async fn columns(&self, table: &str) -> Result<Vec<ColumnInfo>, String> {
        let conn = self.0.conn.borrow();
        let mut q = conn
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .map_err(|e| e.to_string())?;
        let rows = q
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
        rows
    }
    async fn count(&self, table: &str, key: &str, txid: &str) -> Result<u64, String> {
        self.0
            .conn
            .borrow()
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{table}\" WHERE \"{key}\"=?"),
                [txid],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())
    }
    async fn vouts(&self, _: &str, _: &str) -> Result<Vec<u32>, String> {
        unreachable!("the re-point reads no vouts")
    }
    async fn exec(&self, sql: &str, binds: Vec<QVal>) -> Result<(), String> {
        self.0
            .conn
            .borrow()
            .execute(sql, rusqlite::params_from_iter(params(binds)))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    async fn query_i64(&self, sql: &str, binds: Vec<QVal>) -> Result<Option<i64>, String> {
        self.0
            .conn
            .borrow()
            .query_row(sql, rusqlite::params_from_iter(params(binds)), |r| {
                r.get::<_, Option<i64>>(0)
            })
            .optional()
            .map(Option::flatten)
            .map_err(|e| e.to_string())
    }
}

fn row_of<'a>(body: &'a Value, outpoint: &str) -> Option<&'a Value> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == outpoint)
}

/// A P2PKH hop, the owner's dead sweep of its output 0 (its atomic BEEF as the overlay stores it), and a competitor
/// that spent the same output first.
fn hop_sweep_competitor() -> (Tx, Tx, Vec<u8>, Tx) {
    let mut hop = Tx::new();
    hop.add_input(TransactionInput::new("41".repeat(32), 0))
        .unwrap();
    let mut lock = vec![0x76, 0xa9, 0x14];
    lock.extend_from_slice(&[0x41; 20]);
    lock.extend_from_slice(&[0x88, 0xac]);
    hop.add_output(TransactionOutput {
        satoshis: Some(20_190),
        locking_script: LockingScript::from_binary(&lock).unwrap(),
        change: false,
    })
    .unwrap();
    let spend = |sats: u64, tag: u8| {
        let mut tx = Tx::new();
        let mut input = TransactionInput::new(hop.id(), 0);
        input.source_transaction = Some(Box::new(hop.clone()));
        tx.inputs.push(input);
        tx.outputs.push(TransactionOutput {
            satoshis: Some(sats),
            locking_script: LockingScript::from_binary(&[0x51, tag]).unwrap(),
            change: false,
        });
        tx
    };
    let sweep = spend(20_000, 0x01);
    let beef = Beef::from_binary(&sweep.to_beef(true).unwrap())
        .unwrap()
        .to_binary_atomic(&sweep.id())
        .unwrap();
    let competitor = spend(19_900, 0x02);
    (hop, sweep, beef, competitor)
}

/// The eviction reason `refusal_job` (and the pending watch) hands `evict_txid_everywhere`, from the shipped
/// evidence check over Arcade's live word and both indexers absent.
fn live_reason(status: &str, extra: &str) -> String {
    match evidence_verdict(
        &ArcadeLook::Fatal(status.into(), extra.into()),
        Some(false),
        Some(false),
        false,
    ) {
        EvidenceVerdict::Refused(r) => format!("{status} ({r})"),
        v => panic!("{v:?}"),
    }
}

/// The issue's pin. The owner's index-unspent stranded hop presses a sweep; the dead sweep's eviction on Arcade's live
/// `UTXO_SPENT ... utxo already spent by tx <competitor>` re-points the hop to the competitor (unconfirmed), writes a
/// DOWNGRADE mark through the row's trigger and frames the `pot` pair, and answers the outpoint for the ledger's
/// `releasedSpends`; the read withholds the press at once (pending, the downgrade), and the drain's judgement serves
/// no press. RED with `arcade_named_spends` made inert (the base's rule: the eviction records nothing on the hop):
/// the hop stays `spent = 0` and the press stands.
#[tokio::test]
async fn fix1016_a_an_eviction_naming_the_competitor_withholds_the_hops_press_at_once() {
    let db = Db::new();
    let (hop, sweep, beef, competitor) = hop_sweep_competitor();
    let (hop_txid, sweep_txid, comp) = (hop.id(), sweep.id(), competitor.id());
    seed_stranded_hop(&db, "owner", "g00", &hop_txid);
    drain_all(&db).await;
    let op = format!("{hop_txid}:0");
    let body = served(&db, "owner", Some(900_000)).await;
    let row = row_of(&body, &op).unwrap_or_else(|| panic!("{body}"));
    assert_eq!(row["family"], "hop-stranded", "{row}");
    assert_eq!(
        row["facts"]["claimable"], true,
        "the premise: the index-unspent hop presses its sweep: {row}"
    );

    // L56's live word, Arcade's own text (doubled prefix, the `[0]` suffix)
    let reason = live_reason(
        "REJECTED",
        &format!(
            "UTXO_SPENT (70): UTXO_SPENT (70): {hop_txid}:0 utxo already spent by tx {comp}[0]"
        ),
    );
    let _ = bsv_overlay_cloudflare::owed_events::drain();
    let repointed =
        repoint_arcade_named(&Shadow(&db), &sweep_txid, &reason, Some(beef.as_slice())).await;
    assert_eq!(
        repointed,
        vec![ReleasedSpend {
            table: "pot_records".into(),
            txid: hop_txid.clone(),
            vout: 0
        }],
        "the outpoint the eviction's ledger must still name"
    );
    let hop_row: (i64, Option<String>, i64) = db
        .conn
        .borrow()
        .query_row(
            "SELECT spent, spendingTxid, COALESCE(spentConfirmed,0) FROM pot_records WHERE txid=? AND outputIndex=0",
            [&hop_txid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        hop_row,
        (1, Some(comp.clone()), 0),
        "the hop is spent by Arcade's named competitor, unconfirmed until the chaser"
    );
    assert_eq!(
        db.int("SELECT COALESCE(MAX(downgrade),0) FROM owed_dirty WHERE identity='owner' AND gameId='g00'"),
        1,
        "the hop's change is a DOWNGRADE mark"
    );
    let notes = bsv_overlay_cloudflare::owed_events::drain();
    assert!(
        notes
            .iter()
            .any(|n| n.source == "pot" && n.key == format!("{hop_txid}:0")),
        "the hop's `pot` pair is framed: {:?}",
        notes
            .iter()
            .map(|n| (&n.source, &n.key))
            .collect::<Vec<_>>()
    );

    let body = served(&db, "owner", Some(900_000)).await;
    let row = row_of(&body, &op).unwrap_or_else(|| panic!("{body}"));
    assert_eq!(
        row["facts"]["claimable"], false,
        "the press is withheld at once (the downgrade pending): {row}"
    );
    drain_all(&db).await;
    let body = served(&db, "owner", Some(900_000)).await;
    if let Some(row) = row_of(&body, &op) {
        assert_ne!(
            row["facts"]["claimable"], true,
            "the judgement reads the hop spent by the competitor: {row}"
        );
        assert_ne!(row["family"], "hop-stranded", "{row}");
    }

    // a second eviction of the same subject (the door's re-eviction with the ledger's reason) re-points nothing:
    // the row is the competitor's
    assert!(
        repoint_arcade_named(&Shadow(&db), &sweep_txid, &reason, Some(beef.as_slice()))
            .await
            .is_empty()
    );
}

/// What names nothing (the row stays as today): a recorded word (the public bearer's text, even one quoting a live
/// word), an absence, a competitor named without its outpoint, a named outpoint the subject's bytes do not consume, a
/// competitor equal to the subject, no bytes held. `DOUBLE_SPEND_ATTEMPTED` with the same text names its spend, and the
/// door's scrubbed `utxo spent by tx` too.
#[tokio::test]
async fn fix1016_b_only_arcades_live_word_naming_an_outpoint_the_subject_spends_re_points() {
    let db = Db::new();
    let (hop, sweep, beef, competitor) = hop_sweep_competitor();
    let (hop_txid, sweep_txid, comp) = (hop.id(), sweep.id(), competitor.id());
    seed_stranded_hop(&db, "owner", "g00", &hop_txid);
    let extra = format!("{hop_txid}:0 utxo already spent by tx {comp}[0]");
    let named = |r: &str| arcade_named_spends(r).len();
    assert_eq!(named(&live_reason("REJECTED", &extra)), 1);
    assert_eq!(named(&live_reason("DOUBLE_SPEND_ATTEMPTED", &extra)), 1);
    assert_eq!(
        named(&live_reason(
            "REJECTED",
            &format!("{hop_txid}:0 utxo spent by tx {comp}")
        )),
        1,
        "the door's scrub drops `already`"
    );
    // the recorded word: the evidence check's own reason for a pushed status Arcade could not confirm
    let recorded = match evidence_verdict(
        &ArcadeLook::Recorded("REJECTED".into(), format!("(arcade live REJECTED: {extra}")),
        Some(false),
        Some(false),
        false,
    ) {
        EvidenceVerdict::Refused(r) => format!("REJECTED ({r})"),
        v => panic!("{v:?}"),
    };
    assert_eq!(named(&recorded), 0, "{recorded}");
    assert_eq!(named("REJECTED (arcade missing, both indexers absent)"), 0);
    assert_eq!(
        named(&live_reason(
            "REJECTED",
            &format!("utxo already spent by tx {comp}")
        )),
        0,
        "no outpoint, nothing named"
    );
    assert_eq!(
        named(&format!(
            "input {hop_txid}:0 spent by confirmed competitor {comp} at 1"
        )),
        0
    );

    let shadow = Shadow(&db);
    let other = "ee".repeat(32);
    for (why, reason, subject, bytes) in [
        (
            "an outpoint the subject does not spend",
            live_reason(
                "REJECTED",
                &format!("{other}:0 utxo already spent by tx {comp}"),
            ),
            sweep_txid.clone(),
            Some(beef.as_slice()),
        ),
        (
            "the competitor is the subject",
            live_reason(
                "REJECTED",
                &format!("{hop_txid}:0 utxo already spent by tx {sweep_txid}"),
            ),
            sweep_txid.clone(),
            Some(beef.as_slice()),
        ),
        (
            "no bytes held",
            live_reason("REJECTED", &extra),
            sweep_txid.clone(),
            None,
        ),
        (
            "a recorded word",
            recorded.clone(),
            sweep_txid.clone(),
            Some(beef.as_slice()),
        ),
    ] {
        assert!(
            repoint_arcade_named(&shadow, &subject, &reason, bytes)
                .await
                .is_empty(),
            "{why}"
        );
        assert_eq!(
            db.int(&format!(
                "SELECT spent FROM pot_records WHERE txid='{hop_txid}'"
            )),
            0,
            "{why}: the hop stays as today"
        );
    }
}

/// The wiring: every eviction through `evict_txid_everywhere` (the `/arc-ingest` refusal job, the pending watch, the
/// door's re-evictions) runs the re-point first and hands its outpoints to the eviction's ledger; the conflict pass's
/// own path (`evict_conflict` over `evict_txid_everywhere_repointed`) is unchanged.
#[test]
fn fix1016_c_the_eviction_re_points_before_it_runs_and_the_conflict_path_is_unchanged() {
    let src = include_str!("../../overlay-cloudflare/src/admit_fast.rs");
    let f = &src[src.find("pub async fn evict_txid_everywhere(").unwrap()..];
    let f = &f[..f.find("\n}\n").unwrap()];
    let rp = f
        .find("repoint_arcade_named(db, txid, reason, beef.as_deref())")
        .expect("the re-point");
    let ev = f
        .find("evict_txid_everywhere_repointed(db, txid, reason, asked_ms, &repointed)")
        .expect("its outpoints to the ledger");
    assert!(rp < ev);
    let conflict = &src[src.find("impl ConflictEviction for D1Database").unwrap()..];
    let conflict = &conflict[..conflict.find("\n}\n").unwrap()];
    assert!(
        conflict.contains("evict_txid_everywhere_repointed(self, txid, reason, now_ms, repointed)")
    );
    assert!(!conflict.contains("repoint_arcade_named"));
    let job = &src[src.find("pub async fn refusal_job(").unwrap()..];
    assert!(job[..job.find("\n}\n").unwrap()].contains("evict_txid_everywhere(db, &txid,"));
}
