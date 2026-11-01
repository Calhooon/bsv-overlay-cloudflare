//! bsv-low #1019: `/utxo-status` serves the eviction's refusal CLASS (`evicted.class`) beside its unchanged chain
//! word (#621), ADDITIVE. The ledger rows are written by the overlay's OWN OPEN marker
//! (`admit_fast::OPEN_EVICTION_MARKER_SQL`, its `?4` the class `eviction_class::eviction_refusal_class` reads off the
//! arm's reason, as `eviction_write::open_marker` binds it) under real SQLite with the overlay's migrations, each
//! reason built by the arm's own shipped builder, then read by the app layer's shipped statement, attach and body.
use bsv_overlay_cloudflare::{
    admit_fast::{
        conflict_eviction_reason, evidence_verdict, ladder_rerefusal_reason, EvidenceVerdict,
        CLOSE_EVICTION_MARKER_SQL, OPEN_EVICTION_MARKER_SQL,
    },
    d1::OVERLAY_MIGRATIONS,
    eviction_class::eviction_refusal_class,
    proof_fetcher::ArcadeLook,
};
use low_app_layer::logic::{
    attach_evictions, distinct_db_txids, evictions_by_txid_sql, parse_outpoints, utxo_status_body,
    EvictionLedgerRow, OutpointStatus,
};
use overlay_discovery::pot::storage::REFUSAL_CLASSES;
use rusqlite::Connection;

const AT: i64 = 1_760_000_000_000;

fn db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    db
}

/// The OPEN marker as `eviction_write::open_marker` binds it (pinned there to be the one production binding).
fn open(db: &Connection, txid: &str, reason: &str) {
    db.execute(
        OPEN_EVICTION_MARKER_SQL,
        rusqlite::params![txid, reason, AT, eviction_refusal_class(reason)],
    )
    .unwrap();
}

/// The route's read and assembly, minus the host: the shipped statement, attach and body.
fn served_body(db: &Connection, param: &str) -> String {
    let ops = parse_outpoints(param).unwrap();
    let txids = distinct_db_txids(&ops);
    let mut q = db.prepare(&evictions_by_txid_sql(txids.len())).unwrap();
    let rows: Vec<EvictionLedgerRow> = q
        .query_map(rusqlite::params_from_iter(txids.iter()), |r| {
            Ok(EvictionLedgerRow {
                txid: r.get::<_, String>(0)?.to_ascii_lowercase(),
                evicted_at: r.get(1)?,
                readmitted_at: r.get(2)?,
                reason: r.get(3)?,
                refusal_class: r.get(4)?,
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut entries: Vec<OutpointStatus> = ops.iter().map(OutpointStatus::unknown).collect();
    attach_evictions(&mut entries, &rows);
    utxo_status_body(&entries)
}

fn served(db: &Connection, txid: &str) -> serde_json::Value {
    let v: serde_json::Value =
        serde_json::from_str(&served_body(db, &format!("{txid}.0"))).unwrap();
    v[0]["evicted"].clone()
}

fn refused(look: ArcadeLook) -> String {
    match evidence_verdict(&look, Some(false), Some(false), false) {
        EvidenceVerdict::Refused(r) => r,
        other => panic!("{other:?}"),
    }
}

/// bsv-low #1019 (d): an evicted pot of each class serves `evicted.class` beside its unchanged word; the class
/// is gone once readmitted, as the word is.
#[test]
fn fix1019_d_utxo_status_serves_the_class_of_each_arm() {
    let give_up = "no network verdict after 288 durable retry attempts: giving up: a parent this transaction spends has never reached the network; TX_MISSING_PARENT cb846b10";
    let cases: Vec<(String, &str, Option<&str>)> = vec![
        (
            format!("REJECTED ({})", refused(ArcadeLook::Fatal("REJECTED".into(), give_up.into()))),
            "missing-parent",
            Some("rejected"),
        ),
        (
            format!("DOUBLE_SPEND_ATTEMPTED ({})", refused(ArcadeLook::Fatal("DOUBLE_SPEND_ATTEMPTED".into(), String::new()))),
            "double-spend",
            Some("rejected"),
        ),
        (
            format!("REJECTED ({})", refused(ArcadeLook::Fatal("REJECTED".into(), "TX_LOCK_TIME (35): lock time (972185) as block height is not less than block height (972172)".into()))),
            "non-final",
            Some("rejected"),
        ),
        (
            ladder_rerefusal_reason("TX_INVALID (31): GoBDK fail to ValidateTransaction -> TX_POLICY (39): transaction fee is too low"),
            "policy",
            Some("rejected"),
        ),
        (
            conflict_eviction_reason(&"aa".repeat(32), 1, &"bb".repeat(32), 967_673_u64),
            "double-spend",
            Some("conflicted"),
        ),
        (
            format!("SEEN_ON_NETWORK ({})", refused(ArcadeLook::Missing)),
            "absence",
            None,
        ),
    ];
    let db = db();
    for (i, (reason, class, word)) in cases.iter().enumerate() {
        assert!(REFUSAL_CLASSES.contains(class));
        let txid = format!("{:064x}", i + 1);
        open(&db, &txid, reason);
        assert_eq!(
            served(&db, &txid),
            serde_json::json!({"at": AT, "word": word, "readmittedAt": null, "class": class}),
            "{reason}"
        );
        db.execute(
            "UPDATE pot_evictions SET readmittedAt = ? WHERE txid = ? AND readmittedAt IS NULL",
            rusqlite::params![AT + 1, txid],
        )
        .unwrap();
        assert_eq!(
            served(&db, &txid),
            serde_json::json!({"at": AT, "word": null, "readmittedAt": AT + 1}),
            "a readmitted row serves no class: {reason}"
        );
    }
}

/// bsv-low #1019 (e): a NULL class serves the old bytes exactly: a reason no rule names, a row the CLOSE wrote
/// alone, and a row written before the column (`refusalClass` NULL) carry no `class` key at all, and a stored
/// value outside the vocabulary is never served.
#[test]
fn fix1019_e_a_null_class_serves_the_old_bytes() {
    let db = db();
    let a = "ab".repeat(32);
    let b = "cd".repeat(32);
    let c = "ef".repeat(32);
    open(
        &db,
        &a,
        "REJECTED (arcade live REJECTED: TX_INVALID (31): script failed; both indexers absent)",
    );
    db.execute(
        CLOSE_EVICTION_MARKER_SQL,
        rusqlite::params![
            b,
            "REJECTED (arcade live REJECTED: bad script; both indexers absent)",
            AT,
            2,
            "[]"
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO pot_evictions (txid, reason, evictedAt, refusalClass) VALUES (?1, 'REJECTED (arcade live REJECTED: x; both indexers absent)', ?2, 'not-a-class')",
        rusqlite::params![c, AT],
    )
    .unwrap();
    let body = served_body(&db, &format!("{a}.0,{b}.0,{c}.0"));
    let old = format!(r#""evicted":{{"at":{AT},"readmittedAt":null,"word":"rejected"}}"#);
    assert_eq!(
        body.matches(&old).count(),
        3,
        "the old bytes, byte for byte: {body}"
    );
    assert!(!body.contains("\"class\""), "{body}");
}
