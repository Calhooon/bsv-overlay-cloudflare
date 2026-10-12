use bsv_rs::transaction::MerklePath;
use bsv_tracker::{Header, HeaderError, Proof};
use low_app_layer::tracker_host::HeaderSnapshot;
#[path = "support/tracker_stored.rs"]
mod stored;
use std::collections::BTreeMap;
use stored::*;
fn snapshot(root: &str) -> HeaderSnapshot {
    HeaderSnapshot {
        tip: Ok(10),
        headers: BTreeMap::from([(
            10,
            Ok(Some(Header {
                hash: "02".repeat(32),
                merkle_root: root.into(),
            })),
        )]),
    }
}
#[test]
fn ns3_state_roundtrip_rechecks_one_named_header_and_refutes_reorg() {
    let id = "01".repeat(32);
    let mut r = Record::new(&id);
    r.append_checked(
        Proof::new(&id, MerklePath::from_coinbase_txid(&id, 10)).unwrap(),
        &snapshot(&id),
    )
    .unwrap();
    let bytes = serde_json::to_string(&r).unwrap();
    let restored: Record = serde_json::from_str(&bytes).unwrap();
    assert_eq!(
        rechecked(&restored, &snapshot(&id)).unwrap().1["word"],
        "mined"
    );
    let (_, stale) = rechecked(&restored, &snapshot(&"03".repeat(32))).unwrap();
    assert_eq!(stale["word"], "stale");
    assert!(stale["verifyPath"].is_null());
    let unavailable = HeaderSnapshot {
        tip: Err(HeaderError("transport".into())),
        headers: BTreeMap::new(),
    };
    let (_, unknown) = rechecked(&restored, &unavailable).unwrap();
    assert_eq!(unknown["word"], "mined");
    assert!(unknown["verifyPath"].is_null());
    assert!(unknown["cannotCheck"].is_string());
}
#[test]
fn ns3_hint_and_retire_tick_roundtrip_never_writes_rejected_or_absent() {
    let mut r = Record::new(&"01".repeat(32));
    r.events.push(Event::Build);
    r.events.push(Event::Seen {
        source: "woc".into(),
        observed: 123,
    });
    r.events.push(Event::Arc {
        body: serde_json::json!({"txStatus":"REJECTED","extraInfo":"fee"}),
        observed: 124,
    });
    r.events.push(Event::Tick { now: 200 });
    let s = r.replay().unwrap();
    assert_eq!(
        low_app_layer::tracker_host::served::fields(&s)["word"],
        "seen"
    );
    assert!(s.evidence().is_none());
    assert!(s.reask().is_some());
}
#[test]
fn ns3_tx_words_migration_and_lookup_have_one_primary_key_statement() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(CREATE).unwrap();
    let r = Record::new(&"01".repeat(32));
    let body = serde_json::to_string(&r).unwrap();
    db.execute(UPSERT, rusqlite::params![r.txid, body, 100])
        .unwrap();
    db.execute(UPSERT, rusqlite::params![r.txid, "older", 99])
        .unwrap();
    let result: String = db
        .query_row("SELECT state FROM tx_words WHERE txid=?1", [&r.txid], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(body, result);
}
