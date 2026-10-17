//! bsv-low #638: hot routes must never scan a fleet table.
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use rusqlite::{params_from_iter, Connection};

fn fixture() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_fleet::rows(1_790_000_000) {
        d1_fleet::execute_row(&db, &sql);
    }
    db.execute_batch("COMMIT").unwrap();
    db
}

fn plan(db: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let binds = vec![""; stmt.parameter_count()];
    stmt.query_map(params_from_iter(binds), |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn hot_routes_never_scan_fleet_tables() {
    let db = fixture();
    println!("shipped migrations: {}", OVERLAY_MIGRATIONS.len());
    let mut failures = Vec::new();
    for arity in [1, 24, 100] {
        for detail in plan(
            &db,
            &low_app_layer::results::legacy_hop_seat_markers_sql(arity),
        ) {
            assert!(
                !detail.starts_with("SCAN hopparty_records"),
                "legacy indexed lookup: {detail}"
            );
        }
    }
    // bsv-low L25 (the HOMEPROOF lens's NOTE-2): the byte-free `homeproof:` latch read runs on every /owed
    // recompute; each OR'd PK-range term must stay on the hop_chain_probes PRIMARY KEY autoindex (SEARCH /
    // MULTI-INDEX OR), never a scan, at every arity up to the chunk bound. A future WITHOUT ROWID or a dropped
    // PK index would regress it silently without this pin.
    for n in [1, 24, low_app_layer::owed::HOME_LATCH_READ_CHUNK] {
        for detail in plan(&db, &low_app_layer::owed::home_latch_range_read_sql(n)) {
            assert!(
                !detail.starts_with("SCAN"),
                "the homeproof latch range read must stay index-backed (arity {n}): {detail}"
            );
        }
    }
    for (route, sql) in [
        (
            "/results:view",
            low_app_layer::results::results_sql(None, 0),
        ),
        (
            "/results:view hop seats",
            low_app_layer::results::hop_seat_markers_sql(24),
        ),
        (
            "owed recompute refund",
            low_app_layer::refund_view::refund_view_sql(None, 0),
        ),
        (
            "owed recompute hops",
            low_app_layer::hops_view::hops_view_sql(false, None, 0),
        ),
        (
            "owed recompute proofs",
            low_app_layer::hopsweep::sweep_proofs_sql(16),
        ),
        (
            "/recovery-view",
            low_app_layer::logic::recovery_view_sql(None, 0),
        ),
        (
            "/refund-backups",
            low_app_layer::refund_backups::refund_backups_sql(None),
        ),
        (
            "owed recompute spenders",
            low_app_layer::owed::covenant_spenders_sql(16),
        ),
        (
            "owed recompute backups",
            low_app_layer::owed::valid_refunds_sql(16),
        ),
        // bsv-low #642: every statement `/leaderboard:view` runs, at the chunk bound.
        (
            "/leaderboard:view candidates",
            low_app_layer::logic::chain_wins_candidates_sql(None),
        ),
        (
            "/leaderboard:view candidates (era)",
            low_app_layer::logic::chain_wins_candidates_sql(Some(1)),
        ),
        (
            "/leaderboard:view statuses",
            low_app_layer::logic::batch_where_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS),
        ),
        (
            "/leaderboard:view decoded pots",
            low_app_layer::results::decoded_pots_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS),
        ),
        (
            "/leaderboard:view pot beefs",
            low_app_layer::logic::leaderboard_pot_beefs_sql(
                low_app_layer::logic::D1_CHUNK_OUTPOINTS,
            ),
        ),
        (
            "/leaderboard:view pot markers",
            low_app_layer::logic::pot_markers_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS),
        ),
        (
            "/leaderboard:view proof pointers",
            low_app_layer::logic::proof_pointers_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS),
        ),
        (
            "/leaderboard:view proof posts",
            low_app_layer::proof_post::proof_posted_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS),
        ),
    ] {
        for detail in plan(&db, &sql) {
            if detail.contains("SEARCH x USING INDEX idx_potparty_identity") {
                failures.push(format!("{route}: quadratic exclusion: {detail}"));
            }
            // Aliases are SQLite's table names in these shipped queries.
            let name = detail
                .strip_prefix("SCAN ")
                .and_then(|s| s.split_whitespace().next());
            if name.is_some_and(|n| {
                [
                    "pot_records",
                    "potparty_records",
                    "hopparty_records",
                    "potrefund_records",
                    "hopsweep_records",
                    "transactions",
                    "result_markers_v2",
                    "proof_markers",
                    "pot_beefs",
                    "network_seen",
                    "hop_pot_links",
                    "pot_hop_seats",
                    "link",
                    "seat",
                    "hp",
                    "hop",
                    "pot",
                    "x",
                    "pr",
                ]
                .contains(&n)
            }) {
                failures.push(format!("{route}: {detail}"));
            }
        }
    }
    println!("hot route fleet SCAN list: {failures:?}");
    assert!(failures.is_empty(), "fleet scans:\n{}", failures.join("\n"));
}

#[test]
fn marker_writer_stores_the_canonical_settle_key() {
    use bsv_overlay_cloudflare::{d1::QVal, d1_discovery::hopparty_write::hopparty_insert_query};
    use overlay_discovery::hopparty::storage::HoppartyRecord;
    let record = HoppartyRecord {
        identity: "02aa".into(),
        opponent_identity: "03bb".into(),
        game_id: "cc".repeat(32),
        hop_vout: 0,
        hop_sats: 1,
        seat_settle_pubkey: "03AB".into(),
        seat_sig_hex: "00".into(),
        identity_sig_hex: "00".into(),
        hop_lock_hex: None,
        hop_sats_on_chain: None,
        container_outputs: 2,
        size_bytes: None,
        fee_sats: None,
        txid: "dd".repeat(32),
        output_index: 1,
        created_at: 1,
    };
    let insert = hopparty_insert_query(&record, 1);
    assert!(matches!(&insert.query().params()[5], QVal::Text(s) if s == "03ab"));
}

fn rows_json(db: &Connection, sql: &str, identity: &str) -> String {
    use rusqlite::types::ValueRef;
    let mut stmt = db.prepare(sql).unwrap();
    let names = stmt
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    let mut rows = stmt.query([identity]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let mut value = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            let v = match row.get_ref(i).unwrap() {
                ValueRef::Null => serde_json::Value::Null,
                ValueRef::Integer(n) => serde_json::json!(n),
                ValueRef::Real(n) => serde_json::json!(n),
                ValueRef::Text(s) => serde_json::json!(std::str::from_utf8(s).unwrap()),
                ValueRef::Blob(s) => serde_json::json!(hex::encode(s)),
            };
            value.insert(name.clone(), v);
        }
        out.push(value);
    }
    serde_json::to_string(&out).unwrap()
}

#[test]
fn fleet_view_inputs_are_byte_identical_to_the_original_queries() {
    let db = fixture();
    let new_party = low_app_layer::logic::party_candidates_sql();
    let old_party = include_str!("support/old_party.sql");
    for after in [0, 100, 1000] {
        for (route, sql) in [
            (
                "/results:view",
                low_app_layer::results::results_sql(None, after),
            ),
            (
                "owed recompute refund",
                low_app_layer::refund_view::refund_view_sql(None, after),
            ),
            (
                "/recovery-view",
                low_app_layer::logic::recovery_view_sql(None, after),
            ),
            (
                "/refund-backups",
                low_app_layer::refund_backups::refund_backups_sql(None),
            ),
        ] {
            let old = sql.replace(&new_party, old_party);
            assert_ne!(old, sql, "the control must use the old reconstruction");
            assert_eq!(
                rows_json(&db, &old, d1_fleet::WHALE),
                rows_json(&db, &sql, d1_fleet::WHALE),
                "{route}, after {after}: the unchanged assemblers receive identical ordered inputs"
            );
        }
    }
}

fn values(db: &Connection, sql: &str, binds: &[rusqlite::types::Value]) -> Vec<serde_json::Value> {
    use rusqlite::types::ValueRef;
    let mut stmt = db.prepare(sql).unwrap();
    let names = stmt
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    stmt.query_map(params_from_iter(binds.iter()), |r| {
        let mut v = serde_json::Map::new();
        for (i, k) in names.iter().enumerate() {
            v.insert(
                k.clone(),
                match r.get_ref(i)? {
                    ValueRef::Null => serde_json::Value::Null,
                    ValueRef::Integer(n) => serde_json::json!(n),
                    ValueRef::Real(n) => serde_json::json!(n),
                    ValueRef::Text(s) => serde_json::json!(std::str::from_utf8(s).unwrap()),
                    ValueRef::Blob(s) => serde_json::json!(hex::encode(s)),
                },
            );
        }
        Ok(serde_json::Value::Object(v))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn cursor_candidates(
    db: &Connection,
    sources: &[low_app_layer::owed_cursor::Source],
) -> Vec<serde_json::Value> {
    use low_app_layer::owed_cursor::{candidates_sql, Cursor, PAGE};
    use rusqlite::types::Value;
    let mut all = Vec::new();
    for source in sources {
        for _ in 0..1 {
            let mut cursor = Cursor::default();
            loop {
                let (nulls, seek) = cursor.phase();
                let sql = candidates_sql(*source, nulls, seek, cursor.next_stamp);
                let mut binds = vec![Value::Text(d1_fleet::WHALE.into())];
                if seek {
                    binds.extend(cursor.bind_numbers(nulls).into_iter().map(Value::Integer));
                }
                let page = values(db, &sql, &binds);
                assert!(page.len() <= PAGE);
                // A bound applies before all keyed joins; no fullscan of any fleet source.
                for detail in plan(db, &sql) {
                    assert!(
                        !detail.starts_with("SCAN pot")
                            && !detail.starts_with("SCAN hp")
                            && !detail.starts_with("SCAN hop"),
                        "{detail}"
                    );
                }
                let more = cursor.advance(&page, nulls);
                all.extend(page);
                if !more {
                    break;
                }
            }
        }
    }
    all
}

#[test]
fn created_at_cursor_union_equals_unpaged_ranked_views_byte_for_byte() {
    use low_app_layer::owed_cursor::{self, Source};
    use rusqlite::types::Value;
    let db = fixture();
    // Include NULL/equal stamps, replay representatives and all validity tiers.
    db.execute_batch("UPDATE potparty_records SET createdAt=NULL WHERE rowid%19=0; UPDATE potparty_records SET sigValid=CASE WHEN rowid%3=0 THEN 1 WHEN rowid%3=1 THEN 0 ELSE NULL END; UPDATE hopparty_records SET createdAt=NULL WHERE rowid%23=0").unwrap();
    // Opposite sides of a mixed era: the pot stamp wins COALESCE; NULL falls back to marker.
    let pots: Vec<_> = (0..4).map(|i| format!("d0{i:062x}")).collect();
    for (i, pot) in pots.iter().enumerate() {
        let pot_stamp = match i {
            0 => "1789999999",
            1 => "1790000001",
            _ => "NULL",
        };
        let marker_stamp = if i % 2 == 0 {
            1_790_000_001
        } else {
            1_789_999_999
        };
        db.execute_batch(&format!("UPDATE pot_records SET createdAt={pot_stamp} WHERE txid='{pot}'; UPDATE potparty_records SET createdAt={marker_stamp},sigValid=1 WHERE potTxid='{pot}';")).unwrap();
    }
    let candidates = cursor_candidates(&db, &[Source::Party, Source::Link]);
    let hop_candidates = cursor_candidates(&db, &[Source::Hop]);
    for era in [
        Some(1_790_000_000_000),
        None,
        Some(1_790_000_000_001),
        Some(1_789_999_999_000),
    ] {
        let selected = owed_cursor::select(candidates.clone(), era, 10);
        if era == Some(1_790_000_000_000) {
            let included: Vec<_> = pots
                .iter()
                .map(|pot| selected.iter().any(|row| row["potTxid"] == *pot))
                .collect();
            assert_eq!(
                included,
                [false, true, true, false],
                "mixed era uses pot stamp, with NULL fallback to marker"
            );
        }
        for original in [
            low_app_layer::results::results_sql(era, 0),
            low_app_layer::refund_view::refund_view_sql(era, 0),
        ] {
            let unpaged = original
                .replace("finalRank <= 0 + 101", "finalRank <= 1000000")
                .replace("LIMIT 101", "LIMIT 1000000");
            let mut binds = vec![Value::Text(d1_fleet::WHALE.into())];
            if let Some(era) = era {
                binds.push(Value::Integer(era));
            }
            let expected = values(&db, &unpaged, &binds);
            let mut got = Vec::new();
            for page in selected.chunks(owed_cursor::PAGE) {
                got.extend(values(
                    &db,
                    &owed_cursor::hydrate_sql(&original),
                    &[Value::Text(serde_json::to_string(page).unwrap())],
                ));
            }
            assert_eq!(
                serde_json::to_vec(&expected).unwrap(),
                serde_json::to_vec(&got).unwrap(),
                "party era {era:?}"
            );
            let mut paired = Vec::new();
            for page in selected.chunks(owed_cursor::PAGE) {
                paired.extend(values(
                    &db,
                    &owed_cursor::hydrate_party_pair_sql(),
                    &[Value::Text(serde_json::to_string(page).unwrap())],
                ));
            }
            assert_eq!(expected.len(), paired.len());
            for (expected, mut paired) in expected.iter().zip(paired) {
                if original.contains("backupMarkerPresent") {
                    paired["spenderProofVerified"] = paired["refundSpenderProofVerified"].clone();
                }
                let projection: serde_json::Map<String, serde_json::Value> = expected
                    .as_object()
                    .unwrap()
                    .keys()
                    .map(|k| (k.clone(), paired[k].clone()))
                    .collect();
                assert_eq!(
                    *expected,
                    serde_json::Value::Object(projection),
                    "shared hydration retains each production projection"
                );
            }
        }
        let original = low_app_layer::hops_view::hops_view_sql(false, era, 0);
        let cap = (low_app_layer::hops_view::HOPS_VIEW_MAX_OUTPOINTS + 1)
            * low_app_layer::hops_view::HOPS_VIEW_ROWS_PER_OUTPOINT;
        let unpaged = original
            .replace("finalRank <= 0 + 101", "finalRank <= 1000000")
            .replace(&format!("LIMIT {cap}"), "LIMIT 1000000");
        let mut binds = vec![Value::Text(d1_fleet::WHALE.into())];
        if let Some(era) = era {
            binds.push(Value::Integer(era));
        }
        let expected = values(&db, &unpaged, &binds);
        let selected = owed_cursor::select_hops(hop_candidates.clone(), era, 1_790_000_000);
        let mut got = Vec::new();
        for page in selected.chunks(owed_cursor::PAGE) {
            got.extend(values(
                &db,
                &owed_cursor::hydrate_hops_sql(),
                &[Value::Text(serde_json::to_string(page).unwrap())],
            ));
        }
        assert_eq!(
            serde_json::to_vec(&expected).unwrap(),
            serde_json::to_vec(&got).unwrap(),
            "hops era {era:?}"
        );
    }
}

#[test]
fn initial_cursor_uses_maximum_even_when_union_arm_rows_are_reordered() {
    let rows = vec![
        serde_json::json!({"createdAt": 10, "cursorRowid": 9}),
        serde_json::json!({"createdAt": null, "cursorRowid": 100}),
        serde_json::json!({"createdAt": 10, "cursorRowid": 3}),
    ];
    let mut cursor = low_app_layer::owed_cursor::Cursor::default();
    cursor.start(&rows);
    assert_eq!(
        cursor.last,
        Some((Some(10), 9)),
        "cursor must use maximum, not last UNION arm row"
    );
    cursor.start(&[
        rows[1].clone(),
        serde_json::json!({"createdAt": null, "cursorRowid": 2}),
    ]);
    assert_eq!(cursor.last, Some((None, 100)));
    cursor.start(&[]);
    assert_eq!(cursor.last, None);
}

/// bsv-low #642: each candidate arm walks `idx_pot_records_chain_wins` in its
/// own ORDER BY (no sort inside the arm), so its LIMIT stops the walk; a sort
/// there would read every counted win of the era before the LIMIT, the
/// whole-board read again. The #403 spine is the contrast: it reads every
/// decoded pot (`idx_pot_params_undecoded`) to group them.
#[test]
fn the_leaderboard_candidate_arms_stop_at_their_limit() {
    let db = fixture();
    for era in [None, Some(1)] {
        let p = plan(&db, &low_app_layer::logic::chain_wins_candidates_sql(era));
        println!("candidates plan (era {era:?}):\n{}", p.join("\n"));
        for arm in ["(subquery-1)", "(subquery-3)"] {
            let at = p
                .iter()
                .position(|d| *d == format!("CO-ROUTINE {arm}"))
                .unwrap_or_else(|| panic!("the arm {arm} is a co-routine: {p:?}"));
            assert_eq!(
                p[at + 1],
                "SEARCH p USING INDEX idx_pot_records_chain_wins (outputIndex=? AND verdict=? AND spent=?)",
                "the arm walks the chain-wins index"
            );
            assert_eq!(
                p[at + 2],
                "SEARCH ns USING COVERING INDEX sqlite_autoindex_network_seen_1 (txid=?) LEFT-JOIN"
            );
            assert_eq!(
                p[at + 3],
                format!("SCAN {arm}"),
                "no sort inside the arm: {p:?}"
            );
        }
        assert!(
            p.iter().any(|d| d == "MATERIALIZE wins"),
            "one seat resolution per row"
        );
    }
    let spine = plan(&db, &low_app_layer::logic::chain_wins_spine_sql(None));
    println!("#403 spine plan:\n{}", spine.join("\n"));
    assert!(
        spine
            .iter()
            .any(|d| d.contains("USE TEMP B-TREE FOR GROUP BY")),
        "the whole-board spine groups every counted win"
    );
}
