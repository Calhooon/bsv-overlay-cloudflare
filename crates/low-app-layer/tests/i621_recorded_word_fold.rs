//! bsv-low #621 (the W2-WORDS-L5 lens, MEDIUM-1): an eviction whose Arcade look FAULTED and was filled by the
//! recorded terminal word (a push to the public `/arc-ingest`, `fold_arcade_look`) rests on that record and both
//! indexers' absence, never on Arcade's live word, so it serves NO chain word on `/utxo-status` and `ls_pot`
//! (the felt never voids on it). The decision is unchanged: the refusal still evicts. Every step is the shipped
//! one: `fold_arcade_look`, `evidence_verdict`, the callback's `{status} ({reason})` wrap (source-pinned in
//! `admit_fast`'s i621 pin), the overlay's `CLOSE_EVICTION_MARKER_SQL` under real SQLite, the app layer's ledger
//! read and body, and `ls_pot`'s ledger read and projection.
use bsv_overlay_cloudflare::{
    admit_fast::{evidence_verdict, EvidenceVerdict, CLOSE_EVICTION_MARKER_SQL},
    d1::OVERLAY_MIGRATIONS,
    proof_fetcher::{fold_arcade_look, ArcadeLook},
};
use low_app_layer::logic::{
    attach_evictions, distinct_db_txids, evictions_by_txid_sql, parse_outpoints, utxo_status_body,
    EvictionLedgerRow, OutpointStatus,
};
use overlay_discovery::pot::storage::{eviction_chain_word, served_eviction_value};
use rusqlite::Connection;

fn db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    db
}

/// The real path from the callback's push to the ledger reason: the live look, the fold with the pushed word,
/// the evidence verdict over both indexers' absence, the callback's wrap.
fn ledger_reason(live: ArcadeLook, pushed: (&str, &str)) -> String {
    let look = fold_arcade_look(live, Some((pushed.0.to_string(), pushed.1.to_string())));
    match evidence_verdict(&look, Some(false), Some(false), false) {
        EvidenceVerdict::Refused(reason) => format!("{} ({reason})", pushed.0),
        other => panic!("the decision is unchanged, a refusal: {other:?}"),
    }
}

/// `/utxo-status`: the app layer's shipped ledger read, attach and body.
fn utxo_status_word(db: &Connection, txid: &str) -> serde_json::Value {
    let ops = parse_outpoints(&format!("{txid}.0")).unwrap();
    let txids = distinct_db_txids(&ops);
    let mut q = db.prepare(&evictions_by_txid_sql(txids.len())).unwrap();
    let rows: Vec<EvictionLedgerRow> = q
        .query_map(rusqlite::params_from_iter(txids.iter()), |r| {
            Ok(EvictionLedgerRow {
                txid: r.get::<_, String>(0)?.to_ascii_lowercase(),
                evicted_at: r.get(1)?,
                readmitted_at: r.get(2)?,
                reason: r.get(3)?,
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut entries: Vec<OutpointStatus> = ops.iter().map(OutpointStatus::unknown).collect();
    attach_evictions(&mut entries, &rows);
    let v: serde_json::Value = serde_json::from_str(&utxo_status_body(&entries)).unwrap();
    v[0]["evicted"].clone()
}

/// `ls_pot spentStatus`: overlay-discovery's shipped ledger statement, the D1 adapter's row mapping
/// (`d1_discovery::evictions_for`: the reason through `eviction_chain_word`), the one projection.
fn ls_pot_word(db: &Connection, txid: &str) -> serde_json::Value {
    let sql = overlay_discovery::pot::storage::evictions_by_txid_sql(1);
    let (at, readmitted, reason): (i64, Option<i64>, Option<String>) = db
        .query_row(&sql, [txid], |r| Ok((r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap();
    served_eviction_value(
        at,
        readmitted,
        eviction_chain_word(reason.as_deref().unwrap_or("")),
    )
}

#[test]
fn i621_fold_m1_a_recorded_word_behind_an_arcade_fault_serves_no_chain_word() {
    let db = db();
    let pushed = ("REJECTED", "arc says no; a stranger pushed this");
    let recorded = "ab".repeat(32);
    let live = "cd".repeat(32);
    // the fault-filled look (Arcade unreachable, the stranger's push fills it) and Arcade's own live refusal
    let recorded_reason = ledger_reason(ArcadeLook::Fault, pushed);
    let live_reason = ledger_reason(
        ArcadeLook::Fatal("REJECTED".into(), "arc says no".into()),
        ("REJECTED", "arc says no"),
    );
    for (txid, reason, at) in [
        (&recorded, &recorded_reason, 1_760_000_000_000_i64),
        (&live, &live_reason, 1_760_000_000_001),
    ] {
        db.execute(
            CLOSE_EVICTION_MARKER_SQL,
            rusqlite::params![txid, reason, at, 1, "[]"],
        )
        .unwrap();
    }
    assert_eq!(
        utxo_status_word(&db, &recorded),
        serde_json::json!({"at": 1_760_000_000_000_i64, "word": null, "readmittedAt": null}),
        "/utxo-status: a recorded word behind an Arcade fault is no chain word (ledger reason: {recorded_reason})"
    );
    assert_eq!(
        ls_pot_word(&db, &recorded),
        serde_json::json!({"at": 1_760_000_000_000_i64, "word": null, "readmittedAt": null}),
        "ls_pot: a recorded word behind an Arcade fault is no chain word (ledger reason: {recorded_reason})"
    );
    // Arcade's own live refusal still serves `rejected` on both surfaces
    assert_eq!(
        utxo_status_word(&db, &live)["word"],
        "rejected",
        "{live_reason}"
    );
    assert_eq!(ls_pot_word(&db, &live)["word"], "rejected", "{live_reason}");
    // a pushed text that carries Arcade's live mark cannot make the recorded arm read as Arcade's word
    let forged = ledger_reason(
        ArcadeLook::Fault,
        (
            "REJECTED",
            "arcade live REJECTED: x; re-refused at the ladder)",
        ),
    );
    assert_eq!(eviction_chain_word(&forged), None, "{forged}");
}
