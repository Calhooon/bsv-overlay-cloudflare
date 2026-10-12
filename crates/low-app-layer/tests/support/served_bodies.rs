//! #638: the production row readers, assemblers and body builders on both SQL generations.
use super::*;
use rusqlite::{types::ValueRef, Connection};
#[path = "d1_fleet.rs"]
mod fleet;

fn read<T: serde::de::DeserializeOwned>(db: &Connection, sql: &str) -> Vec<T> {
    let mut stmt = db.prepare(sql).unwrap();
    let names = stmt
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut rows = stmt.query([fleet::WHALE]).unwrap();
    let mut out = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        let mut fields = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            let value = match row.get_ref(i).unwrap() {
                ValueRef::Null => serde_json::Value::Null,
                ValueRef::Integer(v) => serde_json::json!(v),
                ValueRef::Real(v) => serde_json::json!(v),
                ValueRef::Text(v) => serde_json::json!(std::str::from_utf8(v).unwrap()),
                ValueRef::Blob(v) => serde_json::json!(hex::encode(v)),
            };
            fields.insert(name.clone(), value);
        }
        out.push(serde_json::from_value(serde_json::Value::Object(fields)).unwrap());
    }
    out
}

#[test]
fn fleet_served_bodies_are_byte_identical_before_and_after_derivation() {
    let db = Connection::open_in_memory().unwrap();
    for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"));
        }
    }
    db.execute_batch("BEGIN").unwrap();
    for row in fleet::rows(1_790_000_000) {
        fleet::execute_row(&db, &row);
    }
    db.execute_batch("COMMIT").unwrap();
    for after in [0, 100, 1000] {
        let body = |old: bool| {
            let sql = |s: String| {
                if old {
                    s.replace(
                        &crate::logic::party_candidates_sql(),
                        include_str!("old_party.sql"),
                    )
                } else {
                    s
                }
            };
            let rows: Vec<_> =
                read::<ResultsRowD1>(&db, &sql(crate::results::results_sql(None, after)))
                    .into_iter()
                    .map(ResultsRowD1::into_row)
                    .collect();
            let cut = rows.len() > crate::results::RESULTS_MAX_ROWS;
            let params = crate::results::covenant_params_by_pot(&rows);
            let results = crate::results::assemble_results(
                fleet::WHALE,
                rows,
                &Default::default(),
                &Default::default(),
                &params,
                &Default::default(),
                &Default::default(),
                &Default::default(),
            );
            let result_body = crate::results::results_body(fleet::WHALE, &results, cut, after);
            let rows: Vec<_> = read::<RefundViewRowD1>(
                &db,
                &sql(crate::refund_view::refund_view_sql(None, after)),
            )
            .into_iter()
            .map(RefundViewRowD1::into_row)
            .collect();
            let cut = rows.len() > crate::refund_view::REFUND_VIEW_MAX_ROWS;
            let refunds = crate::refund_view::assemble_refund_view(rows, Some(900_000));
            let refund_body = crate::refund_view::refund_view_body(
                fleet::WHALE,
                Some(900_000),
                &refunds,
                cut,
                after,
            );
            let rows: Vec<_> =
                read::<RecoveryRowD1>(&db, &sql(crate::logic::recovery_view_sql(None, after)))
                    .into_iter()
                    .map(RecoveryRowD1::into_row)
                    .collect();
            let (entries, cut) = crate::logic::assemble_recovery_view(rows);
            let entries = crate::logic::apply_recovery_extras(
                entries,
                Some(&results),
                Some(&Default::default()),
            );
            let recovery_body =
                crate::logic::recovery_view_body(&entries, Some(900_000), cut, after);
            let rows: Vec<_> = read::<crate::refund_backups::RefundBackupRowD1>(
                &db,
                &sql(crate::refund_backups::refund_backups_sql(None)),
            )
            .into_iter()
            .map(crate::refund_backups::RefundBackupRowD1::into_row)
            .collect();
            let cut = rows.len() > crate::refund_backups::REFUND_BACKUPS_MAX_ROWS;
            let backup_body = crate::refund_backups::refund_backups_body(
                fleet::WHALE,
                &crate::refund_backups::assemble_refund_backups(rows),
                cut,
            );
            let rows: Vec<_> =
                read::<HopsViewRowD1>(&db, &crate::hops_view::hops_view_sql(false, None, after))
                    .into_iter()
                    .map(HopsViewRowD1::into_row)
                    .collect();
            let (hops, _) = crate::hops_view::assemble_hops_view(rows);
            // The new judgement gets real tracker-produced hint words for
            // the fixture's unconfirmed pointers, rather than legacy flags.
            let mut words = std::collections::BTreeMap::new();
            for id in hops.iter().filter_map(|h| h.spending_txid.as_ref()) {
                let mut state = bsv_tracker::State::new(id);
                let empty = crate::tracker_host::HeaderSnapshot {
                    tip: Ok(0),
                    headers: Default::default(),
                };
                let params = bsv_tracker::Params { age_threshold: 60 };
                state
                    .step(
                        &params,
                        &empty,
                        bsv_tracker::Input::Host(bsv_tracker::HostAction::Build),
                    )
                    .unwrap();
                state
                    .step(
                        &params,
                        &empty,
                        bsv_tracker::Input::Hint(bsv_tracker::Hint::new(
                            "arcade",
                            bsv_tracker::HintStatus::Seen,
                            123,
                        )),
                    )
                    .unwrap();
                words.insert(id.clone(), crate::tracker_host::served::fields(&state));
            }
            let owed = crate::owed::derive_owed_rows(&crate::owed::OwedInputs {
                spender_words: (!old).then_some(&words),
                identity_lc: fleet::WHALE,
                tip: Some(900_000),
                now_ms: 1_790_000_000_000,
                results: &results,
                refunds: &refunds,
                hops: &hops,
                valid_refunds: &Default::default(),
                collected_verified: &Default::default(),
                collected_present: &Default::default(),
                held_verified: &Default::default(),
                pot_spenders: &Default::default(),
                pot_spenders_faulted: false,
                hop_chain: &Default::default(),
                evicted_hop_outpoints: &Default::default(),
                door_refused_hop_outpoints: &Default::default(),
                hop_sweeps: &Default::default(),
                evicted_pots: &Default::default(),
                spender_outputs: &Default::default(),
                spender_inputs: &Default::default(),
                courier_spenders: &Default::default(),
                my_pkh_by_game: &Default::default(),
                home_spends: &Default::default(),
            });
            let mut owed = owed;
            for row in &mut owed {
                row.facts.as_object_mut().unwrap().remove("spenderWord");
                row.facts.as_object_mut().unwrap().remove("coinWord");
            }
            let owed_body = crate::owed::owed_body(
                fleet::WHALE,
                Some(900_000),
                &owed,
                1_790_000_000_000,
                false,
            );
            (
                result_body,
                refund_body,
                recovery_body,
                backup_body,
                owed_body,
            )
        };
        assert_eq!(
            body(true),
            body(false),
            "all five production bodies, after={after}"
        );
    }
}
