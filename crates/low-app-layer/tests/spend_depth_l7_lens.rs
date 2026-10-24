//! L7-DEPTH-LENS (bsv-low #862): the lens's negative cases on PR 167, beside the lane's eight pins.
use low_app_layer::tracker_host::runtime;
use low_app_layer::{logic, txany};
use serde_json::{json, Value};
use std::collections::BTreeMap;

fn tx(n: u8) -> String {
    format!("{n:02x}").repeat(32)
}

fn status(txid: &str, spent: Option<bool>, spender: Option<&str>) -> logic::OutpointStatus {
    logic::OutpointStatus {
        txid: txid.into(),
        vout: 0,
        known: spent.is_some(),
        spent,
        spending_txid: spender.map(str::to_string),
        spent_confirmed: Some(false),
        spender_seen: Some(true),
        spender_final: Some(true),
        reason: None,
        funding_seen: Some(true),
        funding_seen_by: None,
        evicted: logic::ServedEviction::NotRead,
    }
}

fn answer() -> txany::TxAnyAnswer {
    txany::decide_tx_any(
        None,
        None,
        Some(&txany::TxObservation::FirstPartyPresent {
            confirmed: false,
            raw_hex: Some("0100".into()),
        }),
        txany::AbsenceCorroboration::Unknown,
    )
}

/// The var's edges: what the server serves for each value an operator could set.
#[test]
fn lens_the_vars_edge_values() {
    for (raw, want) in [
        ("1", (1, true)),
        ("01", (1, true)),
        ("006", (6, true)),
        ("\t2\n", (2, true)),
        ("144", (144, true)),
        ("145", (1, false)),
        ("4294967295", (1, false)),
        ("4294967296", (1, false)),
        ("\u{ff12}", (1, false)),
        ("1 2", (1, false)),
        ("1e2", (1, false)),
        ("\u{0}", (1, false)),
    ] {
        let d = runtime::spend_depth_of(Some(raw));
        assert_eq!((d.depth, d.from_var), want, "SPEND_DEPTH={raw:?}");
    }
}

/// The lens's MEDIUM-1, FOLDED (lane L7-DEPTH-FOLD): a well-formed depth past the client's bound
/// (`SERVED_SPEND_DEPTH_MAX` 144 in bsv-low `chainReads.ts`, the server's `SPEND_DEPTH_MAX`) is malformed on the
/// server too: the floor 1 is served, `source: "floor"`, so the host never serves what the client reads as the floor.
/// (As the lens wrote it, this pinned 145 served as 145, `source: "var"`.)
#[test]
fn lens_m1_a_depth_past_the_clients_bound_is_the_floor() {
    const CLIENT_MAX: u64 = 144;
    assert_eq!(runtime::SPEND_DEPTH_MAX, CLIENT_MAX);
    for raw in ["145", "1000", "4294967295"] {
        let d = runtime::spend_depth_of(Some(raw));
        assert_eq!((d.depth, d.from_var), (1, false), "{raw}: {d:?}");
        let h = runtime::spend_depth_health_json(Some(raw));
        assert_eq!(h["source"], "floor", "{h}");
        let mut body: Value = serde_json::from_str(&txany::tx_any_body(&tx(1), &answer())).unwrap();
        runtime::decorate(&mut body, Some(&tx(1)), &BTreeMap::new());
        runtime::stamp_spend_depth(&mut body, d.depth);
        assert_eq!(body["spendDepth"], json!(1));
    }
}

/// The legacy bodies a word-store fault serves (no `tracker`, no `lock`) gain nothing on every route shape,
/// whatever the depth.
#[test]
fn lens_a_legacy_fault_body_gains_nothing() {
    let single = txany::tx_any_body(&tx(1), &answer());
    let batch =
        txany::tx_any_batch_body(&[(tx(1), answer()), (tx(2), answer())], &[(tx(3), "fault")]);
    let entries = [
        status(&tx(5), Some(true), Some(&tx(1))),
        status(&tx(5), Some(false), None),
        status(&tx(6), None, None),
    ];
    for raw in [
        single,
        batch,
        logic::utxo_status_body(&entries),
        txany::spent_any_body(&entries, &[false; 3]),
    ] {
        for depth in [1, 6, 144] {
            let mut v: Value = serde_json::from_str(&raw).unwrap();
            runtime::stamp_spend_depth(&mut v, depth);
            assert_eq!(
                v.to_string(),
                raw,
                "a legacy body gained a key at depth {depth}"
            );
        }
    }
}

/// A batch whose store read held no row for any id: every answer carries `tracker: unknown` and the depth; the
/// ids the batch could not answer (`unknown`, `reasons`) gain nothing.
#[test]
fn lens_a_batch_with_an_unanswered_id_stamps_only_its_answers() {
    let mut v: Value = serde_json::from_str(&txany::tx_any_batch_body(
        &[(tx(1), answer())],
        &[(tx(3), "fault")],
    ))
    .unwrap();
    runtime::decorate(&mut v, None, &BTreeMap::new());
    runtime::stamp_spend_depth(&mut v, 3);
    assert_eq!(v["answers"][tx(1)]["tracker"]["word"], "unknown", "{v}");
    assert_eq!(v["answers"][tx(1)]["spendDepth"], 3, "{v}");
    assert!(
        v.get("spendDepth").is_none(),
        "the batch's top level has no tracker: {v}"
    );
    assert_eq!(v["unknown"], json!([tx(3)]), "{v}");
    assert_eq!(v["reasons"], json!({ tx(3): "fault" }), "{v}");
}

/// `/utxo-status` entries for outpoints with no word row (the store answered, empty): each carries a `lock`
/// (`locked` with an unknown spender word, `free`, `unknown`) and the depth beside it.
#[test]
fn lens_an_outpoint_with_no_word_row_carries_lock_and_depth() {
    let entries = [
        status(&tx(5), Some(true), Some(&tx(9))),
        status(&tx(5), Some(false), None),
        status(&tx(6), None, None),
    ];
    let mut v: Value = serde_json::from_str(&logic::utxo_status_body(&entries)).unwrap();
    let none = BTreeMap::new();
    runtime::decorate(&mut v, None, &none);
    runtime::decorate_outpoints(&mut v, &none, "free", "host");
    runtime::stamp_spend_depth(&mut v, 2);
    let words: Vec<_> = (0..3).map(|i| v[i]["lock"]["word"].clone()).collect();
    assert_eq!(
        words,
        vec![json!("locked"), json!("free"), json!("unknown")],
        "{v}"
    );
    for i in 0..3 {
        assert_eq!(v[i]["spendDepth"], 2, "entry {i}: {v}");
        assert_eq!(v[i]["fundingWord"]["word"], "unknown", "entry {i}: {v}");
    }
}

/// The stamp is idempotent and touches only the named objects: stamping twice is stamping once, a nested
/// `tracker` or `lock` (not a subject, an answer or an entry) is not stamped, a top-level `lock` on an object is
/// not an outpoint entry, and a scalar body is untouched.
#[test]
fn lens_the_stamp_touches_only_its_objects() {
    let mut v =
        json!({"tracker": {"word": "seen"}, "nested": {"tracker": {}, "lock": {}}, "lock": {}});
    runtime::stamp_spend_depth(&mut v, 4);
    let once = v.clone();
    runtime::stamp_spend_depth(&mut v, 4);
    assert_eq!(v, once);
    assert_eq!(v["spendDepth"], 4);
    assert!(v["nested"].get("spendDepth").is_none(), "{v}");
    let mut a = json!([{"tracker": {}}, {"lock": {}}, 7, null]);
    runtime::stamp_spend_depth(&mut a, 4);
    assert_eq!(
        a,
        json!([{"tracker": {}}, {"lock": {}, "spendDepth": 4}, 7, null])
    );
    let mut s = json!("x");
    runtime::stamp_spend_depth(&mut s, 4);
    assert_eq!(s, json!("x"));
}

/// No request input reaches the depth: the only reader is the env var (`spend_depth(env)`), and every stamp site
/// passes `spend_depth(..).depth`, never a value from the request or a stored row.
#[test]
fn lens_only_the_var_moves_the_depth() {
    let rt = include_str!("../src/tracker_host/runtime.rs");
    let routes = include_str!("../src/routes.rs");
    for src in [rt, routes] {
        for (i, _) in src.match_indices("stamp_spend_depth(") {
            let call = &src[i..i + src[i..].find(");").unwrap()];
            if call.contains("value: &mut Value") {
                continue; // the definition
            }
            assert!(
                call.contains("spend_depth(env).depth")
                    || call.contains("spend_depth(&ctx.env).depth"),
                "{call}"
            );
        }
    }
    let reader = &rt[rt.find("pub fn spend_depth(env").unwrap()..];
    let reader = &reader[..reader.find("\n}\n").unwrap()];
    assert!(reader.contains("env.var(SPEND_DEPTH_VAR)"), "{reader}");
}
