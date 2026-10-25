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
/// bsv-low #643: the tip mark's subquery at a block whose window holds no pot.
/// Measured 1 (6,944 before).
const TIP_MARK_READ_CEILING: u64 = 5;
/// bsv-low #645: `/health/invariants.networkSeenTotal`.
/// Measured 1, and 1 for the backfill's re-run (27,000 before).
const SEEN_TOTAL_READ_CEILING: u64 = 5;
/// NS-COST-BAR-1: the proofless-watch GC's read set (its predicate as a SELECT) on the fixture's 221 watched rows.
const GC_READ_CEILING: u64 = 1_000;
/// NS-COST-BAR-1: a trigger-kept count row's read, and a count backfill's SELECT on a re-run.
const COUNT_ROW_READ_CEILING: u64 = 5;
/// NS-COST-BAR-1: the backlog read: three count rows and the hour's young pots (none, two hours on).
const BACKLOG_READ_CEILING: u64 = 10;
/// NS-COST-BAR-1: a relatch census mid-sweep reads the rows after the cursor (half the table) and the NULL tier.
/// The fixture's `hopparty_records` rows are latched (`markerValid = 1`), as `/record` files them: measured 3,777
/// of the old aggregate's 7,552 (local D1).
const HOPPARTY_CENSUS_READ_CEILING: u64 = 5_000;
/// The fixture's `potparty_records` rows carry NO `sigValid` (production's `/record` writes it at filing,
/// `POTPARTY_FILE_SQL`, and the relatch converges the rest), so on the fixture the NULL tier is the WHOLE table:
/// the census's worst case, measured 23,018 against the old aggregate's 15,360 (stated in `docs/D1-BUDGETS.md`).
const POTPARTY_CENSUS_READ_CEILING: u64 = 25_000;
/// NS-COST-BAR-1: the BEFORE shapes are measured beside the AFTER ones (each line prints its reads), under a
/// ceiling that only says they ran: they are the figures `docs/D1-BUDGETS.md` quotes.
const BEFORE_SHAPE_CEILING: u64 = 10_000_000;

/// The GC before NS-COST-BAR-1 (`d1_hot_bounds.rs` pins the shipped one deletes exactly its rows).
const PROOFLESS_WATCH_GC_BEFORE_SELECT: &str = "SELECT txid FROM proofless_watch WHERE \
     txid IN (SELECT txid FROM pot_beefs WHERE has_proof = 1) OR \
     txid IN (SELECT txid FROM transactions WHERE has_proof = 1) OR \
     txid IN (SELECT txid FROM pot_beefs WHERE structurally_unprovable = 1) OR \
     txid IN (SELECT txid FROM transactions WHERE retired_ms IS NOT NULL) OR \
     (txid NOT IN (SELECT txid FROM pot_beefs) AND \
      txid NOT IN (SELECT txid FROM transactions))";

/// The index janitor's backlog before NS-COST-BAR-1.
const INDEX_JANITOR_BACKLOG_BEFORE_SELECT: &str =
    "SELECT lockKind, COUNT(*) AS c FROM pot_records \
     WHERE spent = 0 AND (createdAt IS NULL OR createdAt <= unixepoch() - 3600) GROUP BY lockKind";

/// NS-COST-BAR-1's statements on the fixture: `(name, sql, ceiling)`, the before shapes beside the shipped ones.
fn ns_cost_statements() -> Vec<(&'static str, String, u64)> {
    let gc = bsv_overlay_cloudflare::ops::PROOFLESS_WATCH_GC_SQL.replacen(
        "DELETE FROM proofless_watch WHERE",
        "SELECT txid FROM proofless_watch WHERE",
        1,
    );
    let mut out = vec![
        (
            "NS-COST before: the proofless-watch GC",
            PROOFLESS_WATCH_GC_BEFORE_SELECT.to_string(),
            BEFORE_SHAPE_CEILING,
        ),
        ("NS-COST the proofless-watch GC", gc, GC_READ_CEILING),
        (
            "NS-COST before: retiredUnprovableTotal",
            "SELECT COUNT(*) AS c FROM pot_beefs WHERE structurally_unprovable = 1".to_string(),
            BEFORE_SHAPE_CEILING,
        ),
        (
            "NS-COST retiredUnprovableTotal",
            bsv_overlay_cloudflare::ops::RETIRED_UNPROVABLE_TOTAL_SQL.to_string(),
            COUNT_ROW_READ_CEILING,
        ),
        (
            "NS-COST before: txRetiredTotal",
            "SELECT COUNT(*) AS c FROM transactions WHERE retired_ms IS NOT NULL".to_string(),
            BEFORE_SHAPE_CEILING,
        ),
        (
            "NS-COST txRetiredTotal",
            bsv_overlay_cloudflare::ops::TX_RETIRED_TOTAL_SQL.to_string(),
            COUNT_ROW_READ_CEILING,
        ),
        (
            "NS-COST before: the expired-unproven total",
            "SELECT COUNT(*) AS c FROM transactions WHERE retired_ms IS NOT NULL AND retired_reason = 'expired-unproven'".to_string(),
            BEFORE_SHAPE_CEILING,
        ),
        (
            "NS-COST the expired-unproven total",
            bsv_overlay_cloudflare::proof_window::EXPIRED_COUNT_SQL.to_string(),
            COUNT_ROW_READ_CEILING,
        ),
        // The seed stamps every pot this hour, so both backlog shapes are measured as of two hours later (the
        // hour's young range empty, as on beta, whose 14,313 backlog rows are all older than an hour).
        (
            "NS-COST before: the indexJanitor backlog, two hours on",
            INDEX_JANITOR_BACKLOG_BEFORE_SELECT.replace("unixepoch()", "(unixepoch() + 7200)"),
            BEFORE_SHAPE_CEILING,
        ),
        (
            "NS-COST the indexJanitor backlog, two hours on",
            bsv_overlay_cloudflare::ops::INDEX_JANITOR_BACKLOG_SQL
                .replace("unixepoch()", "(unixepoch() + 7200)"),
            BACKLOG_READ_CEILING,
        ),
    ];
    // The relatch censuses mid-sweep (the cursor at the table's middle rowid: the sweep's average place), the old
    // single aggregate beside each.
    for (table, column, census, ceiling) in [
        (
            "potparty_records",
            "sigValid",
            bsv_overlay_cloudflare::d1_discovery::potparty_relatch_census_sql(),
            POTPARTY_CENSUS_READ_CEILING,
        ),
        (
            "hopparty_records",
            "markerValid",
            bsv_overlay_cloudflare::d1_discovery::hopparty_relatch_census_sql(),
            HOPPARTY_CENSUS_READ_CEILING,
        ),
    ] {
        let mid = format!("(SELECT MAX(rowid) / 2 FROM {table})");
        out.push((
            "NS-COST before: a relatch census, mid-sweep",
            format!(
                "SELECT COALESCE(SUM(CASE WHEN rowid > {mid} THEN 1 ELSE 0 END), 0) AS remaining, \
                        COALESCE(SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END), 0) AS stillNull FROM {table}"
            ),
            BEFORE_SHAPE_CEILING,
        ));
        out.push((
            "NS-COST a relatch census, mid-sweep",
            census.replace("?1", &mid),
            ceiling,
        ));
    }
    // Each count backfill's SELECT on a re-run (the row present): the stamp alone.
    for backfill in bsv_overlay_cloudflare::ops::COUNT_ROW_BACKFILLS
        .iter()
        .filter(|b| **b != bsv_overlay_cloudflare::ops::NETWORK_SEEN_COUNT_BACKFILL)
    {
        let select = &backfill[backfill.find("SELECT").unwrap()..];
        out.push((
            "NS-COST a count backfill's re-run (its SELECT)",
            select.to_string(),
            COUNT_ROW_READ_CEILING,
        ));
    }
    out
}

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
    // bsv-low #643, #644, #645 (W1.6): the hot passes' reads on the fleet fixture, each under its own ceiling
    // (docs/D1-BUDGETS.md). The tip mark is its UPDATE's subquery at a block whose window holds no pot.
    let tip_sql = low_app_layer::owed_judge::MARK_TIP_SQL;
    let tip_select = &tip_sql[tip_sql.find("SELECT identity").unwrap()..tip_sql.find(" ON CONFLICT").unwrap()];
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
        (
            "#643 tip mark at a block reaching no pot",
            tip_select.to_string(),
            vec![Value::Integer(958_900),Value::Integer(0)],
            TIP_MARK_READ_CEILING,
            false,
        ),
        (
            "#645 the count's backfill on a re-run (its SELECT)",
            bsv_overlay_cloudflare::ops::NETWORK_SEEN_COUNT_BACKFILL
                [bsv_overlay_cloudflare::ops::NETWORK_SEEN_COUNT_BACKFILL
                    .find("SELECT")
                    .unwrap()..]
                .to_string(),
            vec![],
            SEEN_TOTAL_READ_CEILING,
            false,
        ),
        (
            "#645 networkSeenTotal",
            bsv_overlay_cloudflare::ops::NETWORK_SEEN_TOTAL_SQL.to_string(),
            vec![],
            SEEN_TOTAL_READ_CEILING,
            false,
        ),
    ] {
        let (expanded, rows) = query(db, &statement, &binds);
        sql.push_str(&format!("{expanded};\n"));
        expected.push(json!({"name":name,"rows":rows,"sql":expanded,"ceiling":ceiling,"unordered":unordered,"bound":true}));
    }
    for (name, statement, ceiling) in ns_cost_statements() {
        let (expanded, rows) = query(db, &statement, &[]);
        sql.push_str(&format!("{expanded};\n"));
        expected.push(json!({"name":name,"rows":rows,"sql":expanded,"ceiling":ceiling,"unordered":true,"bound":true}));
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
