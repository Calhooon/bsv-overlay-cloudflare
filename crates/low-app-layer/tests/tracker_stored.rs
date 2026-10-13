#[path = "support/tracker_stored.rs"]
mod stored;
use stored::*;

#[test]
fn ns3_l2_pins_use_production_schema() {
    assert_eq!(CREATE, low_app_layer::tracker_host::stored::CREATE);
    assert_eq!(CREATE, bsv_overlay_cloudflare::d1::TX_WORDS_CREATE);
    assert!(bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS
        .contains(&bsv_overlay_cloudflare::d1::TX_WORDS_CREATE));
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(CREATE).unwrap();
    let id = "01".repeat(32);
    for at in [100, 99] {
        db.execute(
            UPSERT,
            rusqlite::params![
                id,
                "unknown",
                "host",
                Option::<i64>::None,
                Option::<String>::None,
                Option::<i64>::None,
                Option::<String>::None,
                Option::<String>::None,
                "woc",
                at,
                at,
                Option::<String>::None
            ],
        )
        .unwrap();
    }
    let (word, at): (String, i64) = db
        .query_row(
            "SELECT word,updated_at FROM tx_words WHERE txid=?1",
            [&id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((word, at), ("unknown".into(), 100));
}
