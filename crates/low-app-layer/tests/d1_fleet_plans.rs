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
    // bsv-low #693 fold-2: the `proofmined:` latch read is the same statement with one range more (the reorg
    // tombstone's, `owed::proof_latch_range_binds`), so its arity reaches the chunk bound + 1
    for n in [
        1,
        24,
        low_app_layer::owed::HOME_LATCH_READ_CHUNK,
        low_app_layer::owed::HOME_LATCH_READ_CHUNK + 1,
    ] {
        for detail in plan(&db, &low_app_layer::owed::home_latch_range_read_sql(n)) {
            assert!(
                !detail.starts_with("SCAN"),
                "the homeproof latch range read must stay index-backed (arity {n}): {detail}"
            );
        }
    }
    // bsv-low #693 fold-3 (R2-HIGH-1 (a)): the moved latches' delete in the /owed snapshot batch searches the PK
    let moved = plan(&db, low_app_layer::owed::PROOF_LATCH_MOVED_DELETE_SQL);
    assert!(
        moved
            .iter()
            .any(|d| d.starts_with("SEARCH hop_chain_probes USING INDEX")),
        "the moved latches' delete must stay index-backed: {moved:?}"
    );
    assert!(
        !moved.iter().any(|d| d.starts_with("SCAN hop_chain_probes")),
        "the moved latches' delete must never scan: {moved:?}"
    );
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
        // bsv-low #723: `/spent-any`'s index-proof read, at its most spenders (one per outpoint)
        (
            "/spent-any index proofs",
            low_app_layer::hopsweep::sweep_proofs_sql(
                low_app_layer::results::SPENT_ANY_MAX_OUTPOINTS,
            ),
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
        // bsv-low #831: the paired hydration carries the funding latch's two PK probes (`fundingHeld`)
        (
            "owed recompute paired hydration",
            low_app_layer::owed_cursor::hydrate_party_pair_sql(),
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
        // bsv-low #643, #644, #645 (W1.6; their own pins are `d1_hot_bounds.rs`).
        (
            "overlay tick signers-backfill candidates",
            bsv_overlay_cloudflare::d1_discovery::settle_signers_candidates_sql(20),
        ),
        (
            "overlay tick payouts-backfill candidates",
            bsv_overlay_cloudflare::d1_discovery::spender_payouts_candidates_sql(20),
        ),
        (
            "/internal/tip-changed tip mark",
            low_app_layer::owed::OWED_STALE_ON_TIP_SQL.to_string(),
        ),
        (
            "/health/invariants networkSeenTotal",
            bsv_overlay_cloudflare::ops::NETWORK_SEEN_TOTAL_SQL.to_string(),
        ),
        // bsv-low #674: the table read (its own pin, `the_board_table_read_and_its_triggers_never_scan`, holds the
        // plans line by line).
        (
            "/leaderboard:view board owners",
            low_app_layer::logic::BOARD_OWNERS_PAGE_SQL.to_string(),
        ),
        (
            "/leaderboard:view board owners after the cursor",
            low_app_layer::logic::BOARD_OWNERS_AFTER_SQL.to_string(),
        ),
        (
            "/leaderboard:view board cursor rank",
            low_app_layer::logic::BOARD_CURSOR_RANK_SQL.to_string(),
        ),
        (
            "/leaderboard:view board cards",
            low_app_layer::logic::board_cards_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS),
        ),
        (
            "/leaderboard:view board wins",
            low_app_layer::logic::board_wins_owners_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS),
        ),
        (
            "/leaderboard:view board hands pots",
            low_app_layer::logic::board_wins_pots_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS),
        ),
        (
            "/leaderboard:view board hands",
            low_app_layer::logic::BOARD_HANDS_SQL.to_string(),
        ),
        // NS-COST-BAR-1 (their own pins are `d1_hot_bounds.rs` `ns_cost_*`).
        (
            "overlay tick proofless-watch GC",
            bsv_overlay_cloudflare::ops::PROOFLESS_WATCH_GC_SQL.to_string(),
        ),
        (
            "/health/invariants retiredUnprovableTotal",
            bsv_overlay_cloudflare::ops::RETIRED_UNPROVABLE_TOTAL_SQL.to_string(),
        ),
        (
            "/health/invariants txRetiredTotal",
            bsv_overlay_cloudflare::ops::TX_RETIRED_TOTAL_SQL.to_string(),
        ),
        (
            "/health/invariants expired-unproven total",
            bsv_overlay_cloudflare::proof_window::EXPIRED_COUNT_SQL.to_string(),
        ),
        (
            "/health/invariants indexJanitor backlog",
            bsv_overlay_cloudflare::ops::INDEX_JANITOR_BACKLOG_SQL.to_string(),
        ),
        (
            "overlay tick potparty relatch census",
            bsv_overlay_cloudflare::d1_discovery::potparty_relatch_census_sql(),
        ),
        (
            "overlay tick hopparty relatch census",
            bsv_overlay_cloudflare::d1_discovery::hopparty_relatch_census_sql(),
        ),
        (
            "overlay tick result relatch census",
            bsv_overlay_cloudflare::d1_discovery::result_relatch_census_sql(),
        ),
        (
            "overlay tick hand relatch census",
            bsv_overlay_cloudflare::d1_discovery::hand_relatch_census_sql(),
        ),
        // bsv-low #837 (the FIX-837 lens's LOW-5): the owed belt's two reads and the overlay's guarded hop-spend write.
        (
            "/owed #837 belt seat pots",
            low_app_layer::owed::JOIN_SEAT_POTS_SQL.to_string(),
        ),
        (
            "/owed #837 belt pot bytes",
            low_app_layer::owed::JOIN_POT_BEEF_SQL.to_string(),
        ),
        (
            "overlay #837 guarded hop-spend write",
            bsv_overlay_cloudflare::d1_discovery::MARK_SPENT_IF_UNPOINTED_SQL.to_string(),
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
                    "fs",
                    "fb",
                    "hop_pot_links",
                    "pot_hop_seats",
                    "board_wins",
                    "board_owners",
                    "board_hands",
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

/// bsv-low #674: every statement of the table read is an index SEARCH with no temp B-tree for its ORDER BY (the
/// owner page on `idx_board_owners_rank`, the page owners' wins on `idx_board_wins_owner`, the hands on
/// `idx_board_hands_rank`), so the top N read N rows whatever the era holds; and every statement a #674 trigger
/// runs on a write is keyed by the written row (no SCAN of any table), so a settle hook's cost does not grow with
/// the fleet.
#[test]
fn the_board_table_read_and_its_triggers_never_scan() {
    use bsv_overlay_cloudflare::derived_board as board;
    use low_app_layer::logic as l;
    let db = fixture();
    let chunk = l::D1_CHUNK_OUTPOINTS;
    let mut failures = Vec::new();
    for (name, sql, must) in [
        ("ready", l::BOARD_READY_SQL.to_string(), vec![]),
        (
            "owner page",
            l::BOARD_OWNERS_PAGE_SQL.to_string(),
            vec!["SEARCH board_owners USING COVERING INDEX idx_board_owners_rank (wins>?)"],
        ),
        (
            "owner page after the cursor",
            l::BOARD_OWNERS_AFTER_SQL.to_string(),
            vec![
                "SEARCH board_owners USING COVERING INDEX idx_board_owners_rank (wins=? AND owner>?)",
                "SEARCH board_owners USING COVERING INDEX idx_board_owners_rank (wins<?)",
            ],
        ),
        ("cursor rank", l::BOARD_CURSOR_RANK_SQL.to_string(), vec![]),
        (
            "page owners' cards",
            l::board_cards_sql(chunk),
            vec!["SEARCH board_owners USING INDEX sqlite_autoindex_board_owners_1 (owner=?)"],
        ),
        (
            "page owners' wins (an owner past its card)",
            l::board_wins_owners_sql(chunk),
            vec!["SEARCH board_wins USING INDEX idx_board_wins_owner (owner=?)"],
        ),
        ("hands pots' wins", l::board_wins_pots_sql(chunk), vec![]),
        (
            "hands",
            l::BOARD_HANDS_SQL.to_string(),
            vec!["SCAN h USING INDEX idx_board_hands_rank"],
        ),
    ] {
        let p = plan(&db, &sql);
        println!("{name}:\n  {}", p.join("\n  "));
        for d in &p {
            if d.contains("TEMP B-TREE") {
                failures.push(format!("{name}: {d}"));
            }
            // A co-routine or the constant row is not a table.
            let scanned = d
                .strip_prefix("SCAN ")
                .and_then(|s| s.split_whitespace().next())
                .filter(|t| *t != "CONSTANT" && !t.starts_with('('));
            // The hands walk their rank index in order and stop at the LIMIT: the one index-ordered SCAN allowed.
            if scanned.is_some_and(|t| t != "h" || !d.contains("USING INDEX idx_board_hands_rank")) {
                failures.push(format!("{name}: {d}"));
            }
        }
        for m in must {
            if !p.iter().any(|d| d == m) {
                failures.push(format!("{name}: no `{m}` in {p:?}"));
            }
        }
    }
    // Each trigger's statements, with the written row's columns as literals.
    for trigger in [
        board::WIN_INSERT,
        board::WIN_DELETE,
        board::WIN_FRAG,
        board::POT_FRAG,
        board::PROOF_INSERT,
        board::PROOF_UPDATE,
        board::PROOF_DELETE,
        board::POST_INSERT,
        board::POST_UPDATE,
        board::POST_DELETE,
        board::MARKER_INSERT,
        board::MARKER_UPDATE,
        board::MARKER_DELETE,
        board::POT_INSERT,
        board::POT_UPDATE,
        board::POT_DELETE,
        board::SEEN_INSERT,
        board::SEEN_DELETE,
        board::PARTY_INSERT,
        board::PARTY_UPDATE,
        board::PARTY_DELETE,
    ] {
        let name = trigger.split_whitespace().nth(5).unwrap();
        let body = &trigger[trigger.find(" BEGIN ").unwrap() + 7..trigger.rfind(" END").unwrap()];
        let mut literal = body.to_string();
        for col in [
            "potCreatedAt",
            "potTxid",
            "txid",
            "outputIndex",
            "owner",
            "identityIsKey",
            "frag",
            "gameId",
            "winner",
            "settleTxid",
            "verdict",
            "pubA",
            "pubB",
            "settleSigners",
            "identityA",
            "identityB",
        ] {
            for row in ["NEW", "OLD"] {
                let v = if matches!(col, "outputIndex" | "identityIsKey" | "potCreatedAt") {
                    "0"
                } else {
                    "'ab'"
                };
                literal = literal.replace(&format!("{row}.{col}"), v);
            }
        }
        assert!(
            !literal.contains("NEW.") && !literal.contains("OLD."),
            "{name}: {literal}"
        );
        let mut searches = 0;
        for stmt in literal.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let p = plan(&db, stmt);
            println!("trigger {name}: {}", p.join(" | "));
            searches += p.iter().filter(|d| d.contains("SEARCH ")).count();
            // A co-routine (the #674 fold's frag: its markers, pointers and guard) is the output of a subquery the
            // plan lists on its own lines; scanning it reads no table.
            let coroutines: Vec<&str> = p
                .iter()
                .filter_map(|d| d.strip_prefix("CO-ROUTINE "))
                .collect();
            for d in &p {
                let table = d
                    .strip_prefix("SCAN ")
                    .and_then(|s| s.split_whitespace().next())
                    .filter(|t| *t != "CONSTANT" && !t.starts_with('(') && !coroutines.contains(t));
                if table.is_some() {
                    failures.push(format!(
                        "trigger {name}: {d} in `{}`",
                        &stmt[..stmt.len().min(90)]
                    ));
                }
            }
        }
        assert!(searches > 0, "{name}: its statements were planned");
    }
    // bsv-low #674, the cutoff: the guard's condition, asked on every `board_wins` insert, reads the one `board_era`
    // row by its key (a trigger body cannot be planned alone: its `RAISE` exists only in a trigger program). The
    // re-derivation (`ERA_UPDATE`) scans `pot_records` by design, once a cutoff move, as the backfill does; it is no
    // hook's statement, so it is not held here.
    let guard = &board::WIN_ERA
        [board::WIN_ERA.find(" WHEN ").unwrap() + 6..board::WIN_ERA.find(" BEGIN ").unwrap()];
    let p = plan(
        &db,
        &format!("SELECT {}", guard.replace("NEW.potCreatedAt", "0")),
    );
    println!("trigger board_win_era (its condition): {}", p.join(" | "));
    let tables: Vec<&String> = p
        .iter()
        .filter(|d| {
            (d.starts_with("SEARCH ") || d.starts_with("SCAN ")) && *d != "SCAN CONSTANT ROW"
        })
        .collect();
    if tables.len() != 2
        || !tables
            .iter()
            .all(|d| *d == "SEARCH e USING INTEGER PRIMARY KEY (rowid=?)")
    {
        failures.push(format!("board_win_era: {p:?}"));
    }
    let p = plan(&db, l::BOARD_READY_SQL);
    if p.iter()
        .filter(|d| d.contains("SEARCH board_era USING INTEGER PRIMARY KEY (rowid=?)"))
        .count()
        != 2
    {
        failures.push(format!("ready: the board_era row by its key: {p:?}"));
    }
    assert!(failures.is_empty(), "board plans:\n{}", failures.join("\n"));
}

/// bsv-low #831: the paired hydration's `fundingHeld` names an UNSPENT pot's funding as `seen` (the pot txid's own
/// `network_seen` latch), `proof` (a verified `pot_beefs` proof of it) or `none`, and is NULL for a spent pot; both
/// probes are primary-key searches inside the one statement (no new statement, no scan).
/// bsv-low #837 (the FIX-837 lens's LOW-5): the owed belt's two hot reads and the overlay's guarded hop-spend write
/// are each a key search, never a scan of any table: the seat pots by `idx_hopparty_hop`, the seat's
/// `idx_pot_hop_seats_marker` and the pot's primary key; the pot's bytes by `pot_beefs`' primary key; the write by
/// `pot_records`' primary key.
#[test]
fn fix837f_l5_the_belts_reads_and_the_guarded_write_are_key_searches() {
    let db = fixture();
    for (name, sql, wanted) in [
        (
            "JOIN_SEAT_POTS_SQL",
            low_app_layer::owed::JOIN_SEAT_POTS_SQL,
            &[
                "SEARCH hp USING INDEX idx_hopparty_hop",
                "idx_pot_hop_seats_marker",
                "SEARCH pot USING",
            ][..],
        ),
        (
            "JOIN_POT_BEEF_SQL",
            low_app_layer::owed::JOIN_POT_BEEF_SQL,
            &["SEARCH pot_beefs USING"][..],
        ),
        (
            "MARK_SPENT_IF_UNPOINTED_SQL",
            bsv_overlay_cloudflare::d1_discovery::MARK_SPENT_IF_UNPOINTED_SQL,
            &["SEARCH pot_records USING"][..],
        ),
    ] {
        let details = plan(&db, sql);
        println!("{name}: {details:?}");
        assert!(
            details.iter().all(|d| !d.starts_with("SCAN")),
            "{name} scans: {details:?}"
        );
        for w in wanted {
            assert!(
                details.iter().any(|d| d.contains(w)),
                "{name}: {w} in {details:?}"
            );
        }
    }
}

#[test]
fn fix831_the_paired_hydration_reads_the_funding_latch_by_key() {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    let t = |b: char| b.to_string().repeat(64);
    let (seen, proof, none, spent, unproven) = (t('a'), t('b'), t('c'), t('d'), t('e'));
    for (txid, spent_flag) in [
        (&seen, 0),
        (&proof, 0),
        (&none, 0),
        (&spent, 1),
        (&unproven, 0),
    ] {
        db.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?1, 0, ?2, 1790000000)",
            rusqlite::params![txid, spent_flag],
        )
        .unwrap();
    }
    db.execute("INSERT INTO network_seen (txid) VALUES (?1)", [&seen])
        .unwrap();
    db.execute("INSERT INTO network_seen (txid) VALUES (?1)", [&spent])
        .unwrap();
    db.execute(
        "INSERT INTO pot_beefs (txid, beef, proof_verified) VALUES (?1, x'00', 1)",
        [&proof],
    )
    .unwrap();
    db.execute(
        "INSERT INTO pot_beefs (txid, beef, proof_verified) VALUES (?1, x'00', 0)",
        [&unproven],
    )
    .unwrap();
    let page: Vec<serde_json::Value> = [&seen, &proof, &none, &spent, &unproven]
        .iter()
        .enumerate()
        .map(|(i, txid)| {
            serde_json::json!({"identity": d1_fleet::WHALE, "gameId": t('f'), "potTxid": txid, "potVout": 0,
                "createdAt": 1790000000, "markerRowid": i, "tier": 0, "potBestSigRank": 2, "finalRank": i + 1})
        })
        .collect();
    let sql = low_app_layer::owed_cursor::hydrate_party_pair_sql();
    let mut stmt = db.prepare(&sql).unwrap();
    let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let at = names
        .iter()
        .position(|n| n == "fundingHeld")
        .expect("fundingHeld column");
    let tx_at = names.iter().position(|n| n == "potTxid").unwrap();
    let got: std::collections::BTreeMap<String, Option<String>> = stmt
        .query_map([serde_json::to_string(&page).unwrap()], |r| {
            Ok((r.get::<_, String>(tx_at)?, r.get::<_, Option<String>>(at)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(got[&seen].as_deref(), Some("seen"));
    assert_eq!(got[&proof].as_deref(), Some("proof"));
    assert_eq!(
        got[&none].as_deref(),
        Some(low_app_layer::owed::FUNDING_HELD_NONE)
    );
    assert_eq!(
        got[&unproven].as_deref(),
        Some("none"),
        "an unverified proof row holds nothing"
    );
    assert_eq!(got[&spent], None, "a spent pot is not probed");
    let details = plan(&db, &sql);
    for alias in ["fs", "fb"] {
        assert!(
            details
                .iter()
                .any(|d| d.starts_with(&format!("SEARCH {alias} USING")) && d.contains("INDEX")),
            "the {alias} probe is a key search: {details:?}"
        );
        assert!(
            !details
                .iter()
                .any(|d| d.starts_with(&format!("SCAN {alias}"))),
            "the {alias} probe never scans: {details:?}"
        );
    }
}

/// bsv-low #838 (FIX-838): the gated door's eviction releases the spend pointers its subject left
/// (`admit_fast::release_spends_of`). Loop 44's hop sweep 422 answered `maxReads=7895`: the release's UPDATE
/// was planned on `idx_pot_records_confirmed_height (spentConfirmed=?)`, the index of its CONSTANT term, so it
/// walked every unconfirmed pot (beta held 7,805 such rows when measured, `rows_read` 7,805 for the predicate;
/// 0 with the term unindexed). Its SELECT sibling searched `idx_pot_spending` all along. Each statement that
/// releases or reads a subject's spend pointers searches by the SPENDER, and stays within a per-row bound when run
/// over the fleet fixture (every pot unconfirmed: beta's shape at its worst).
#[test]
fn fix838_the_eviction_release_searches_by_the_spender() {
    use bsv_overlay_cloudflare::admit_fast::{
        release_pot_spends_sql, select_pot_spends_sql, ColumnInfo,
    };
    use rusqlite::StatementStatus;
    let db = fixture();
    let cols: Vec<ColumnInfo> = db
        .prepare("PRAGMA table_info(pot_records)")
        .unwrap()
        .query_map([], |r| {
            Ok(ColumnInfo {
                name: r.get(1)?,
                ty: r.get(2)?,
                notnull: r.get(3)?,
                dflt_value: r.get(4)?,
            })
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let unconfirmed: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM pot_records WHERE spentConfirmed = 0",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        unconfirmed > 20_000,
        "the fixture's pots are unconfirmed: {unconfirmed}"
    );
    // the spender of fixture hop 3 (`d1_fleet::rows`: hop i spends pot i)
    let spender = format!("d0{:062x}", 3u32);
    for (what, sql) in [
        ("the release", release_pot_spends_sql(&cols)),
        ("its read", select_pot_spends_sql(&cols)),
    ] {
        let detail = plan(&db, &sql);
        assert!(
            detail
                .iter()
                .any(|d| d.starts_with("SEARCH pot_records USING INDEX idx_pot_spending")),
            "{what} must search by the spender: {detail:?}"
        );
        assert!(
            !detail
                .iter()
                .any(|d| d.contains("idx_pot_records_confirmed_height")
                    || d.starts_with("SCAN pot_records")),
            "{what} must never walk the unconfirmed pots: {detail:?}"
        );
        db.execute_batch("SAVEPOINT fix838").unwrap();
        let mut stmt = db.prepare(&sql).unwrap();
        let mut rows = stmt.query([spender.as_str()]).unwrap();
        let mut n = 0;
        while rows.next().unwrap().is_some() {
            n += 1;
        }
        drop(rows);
        let steps = stmt.get_status(StatementStatus::VmStep);
        drop(stmt);
        db.execute_batch("ROLLBACK TO fix838; RELEASE fix838")
            .unwrap();
        println!(
            "fix838 {what}: {n} row(s) back, {steps} VM steps over {unconfirmed} unconfirmed pots"
        );
        assert!(
            steps < 2_000,
            "{what} runs in steps bounded by the spender's rows, not the fleet's: {steps}"
        );
    }
}
