//! Local D1 executes these exact native-pinned cursor pages and reports rows_read for each.
use low_app_layer::owed_cursor::{self, Source, PAGE};
use rusqlite::{
    types::{Value, ValueRef},
    Connection,
};
use serde_json::json;

fn query(db: &Connection, sql: &str, binds: &[Value]) -> (String, Vec<serde_json::Value>) {
    let mut stmt = db.prepare(sql).unwrap();
    let names = stmt
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    for (i, v) in binds.iter().enumerate() {
        stmt.raw_bind_parameter(i + 1, v).unwrap();
    }
    let expanded = stmt.expanded_sql().unwrap();
    let mut rows = stmt.raw_query();
    let mut out = Vec::new();
    while let Some(r) = rows.next().unwrap() {
        let mut v = serde_json::Map::new();
        for (i, k) in names.iter().enumerate() {
            v.insert(
                k.clone(),
                match r.get_ref(i).unwrap() {
                    ValueRef::Null => serde_json::Value::Null,
                    ValueRef::Integer(n) => json!(n),
                    ValueRef::Real(n) => json!(n),
                    ValueRef::Text(s) => json!(std::str::from_utf8(s).unwrap()),
                    ValueRef::Blob(s) => json!(hex::encode(s)),
                },
            );
        }
        out.push(serde_json::Value::Object(v));
    }
    (expanded, out)
}

/// bsv-low #644: the signers-backfill candidate read on local D1 (docs/D1-BUDGETS.md).
/// Measured 2 with no row pending (22,591 before); the reads are the pending set's.
const SIGNERS_CANDIDATES_READ_CEILING: u64 = 6;
/// bsv-low #644: the payouts-backfill candidate read on local D1.
/// Measured 2 with no row pending (22,591 before).
const PAYOUTS_CANDIDATES_READ_CEILING: u64 = 6;
pub fn write(db: &Connection, path: &str, now: u64, identity: &str) {
    let mut sql = String::new();
    let mut expected = Vec::new();
    let mut party = Vec::new();
    let mut hops = Vec::new();
    let (expanded, rows) = query(
        db,
        &owed_cursor::initial_party_sql(),
        &[Value::Text(identity.into())],
    );
    sql.push_str(&format!("{expanded};\n"));
    expected.push(json!({"name":"combined initial party cursor","rows":rows,"sql":expanded,"ceiling":owed_cursor::PAGE_READ_CEILING}));
    for source in [Source::Party, Source::Link, Source::Hop] {
        for _ in 0..1 {
            let mut cursor = owed_cursor::Cursor::default();
            loop {
                let (nulls, seek) = cursor.phase();
                let mut binds = vec![Value::Text(identity.into())];
                if seek {
                    binds.extend(cursor.bind_numbers(nulls).into_iter().map(Value::Integer));
                }
                let (expanded, rows) = query(
                    db,
                    &owed_cursor::candidates_sql(source, nulls, seek, cursor.next_stamp),
                    &binds,
                );
                sql.push_str(&format!("{expanded};\n"));
                expected.push(json!({"name":format!("{source:?} nulls={nulls} page={}",expected.len()),"rows":rows,"sql":expanded,"ceiling":owed_cursor::PAGE_READ_CEILING}));
                let more = cursor.advance(&rows, nulls);
                match source {
                    Source::Hop => hops.extend(rows),
                    _ => party.extend(rows),
                }
                if !more {
                    break;
                }
            }
        }
    }
    for (name, selected, hydration) in [
        (
            "results",
            owed_cursor::select(party.clone(), None, 10),
            owed_cursor::hydrate_sql(&low_app_layer::results::results_sql(None, 0)),
        ),
        (
            "paired hydration",
            owed_cursor::select(party.clone(), None, 10),
            owed_cursor::hydrate_party_pair_sql(),
        ),
        (
            "refund",
            owed_cursor::select(party, None, 10),
            owed_cursor::hydrate_sql(&low_app_layer::refund_view::refund_view_sql(None, 0)),
        ),
        (
            "hops",
            owed_cursor::select_hops(hops, None, now as i64),
            owed_cursor::hydrate_hops_sql(),
        ),
    ] {
        for (page, rows) in selected.chunks(PAGE).enumerate() {
            let (expanded, rows) = query(
                db,
                &hydration,
                &[Value::Text(serde_json::to_string(rows).unwrap())],
            );
            sql.push_str(&format!("{expanded};\n"));
            expected.push(json!({"name":format!("{name} hydration page={page}"),"rows":rows,"sql":expanded,"ceiling":owed_cursor::PAGE_READ_CEILING}));
        }
    }
    // bsv-low #644 (W1.6): the hot passes' reads on the fleet fixture, each under its own ceiling
    // (docs/D1-BUDGETS.md).
    for (name, statement, binds, ceiling, unordered) in [
        (
            "#644 signers-backfill candidates",
            bsv_overlay_cloudflare::d1_discovery::settle_signers_candidates_sql(20),
            vec![],
            SIGNERS_CANDIDATES_READ_CEILING,
            true,
        ),
        (
            "#644 payouts-backfill candidates",
            bsv_overlay_cloudflare::d1_discovery::spender_payouts_candidates_sql(20),
            vec![],
            PAYOUTS_CANDIDATES_READ_CEILING,
            true,
        ),
    ] {
        let (expanded, rows) = query(db, &statement, &binds);
        sql.push_str(&format!("{expanded};\n"));
        expected.push(json!({"name":name,"rows":rows,"sql":expanded,"ceiling":ceiling,"unordered":unordered,"bound":true}));
    }
    // Compile-time and runtime proof this diagnostic never changes the fixture.
    assert!(sql.lines().all(|s| s.starts_with("SELECT ")));
    std::fs::write(path, sql).unwrap();
    std::fs::write(
        format!("{path}.expected.json"),
        serde_json::to_vec(&expected).unwrap(),
    )
    .unwrap();
}
