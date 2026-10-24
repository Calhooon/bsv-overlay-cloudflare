//! bsv-low #862 (L7-DEPTH, the L6 app-layer half): the host's SPEND DEPTH rides beside every served chain word, an
//! additive integer `spendDepth` beside `tracker` on `/tx-any` (the subject and every `answers[id]`) and beside `lock`
//! on every `/utxo-status` and `/spent-any` entry, read from the render's `SPEND_DEPTH` var; a missing or malformed
//! var serves the floor (1) and counts `spend_depth_var_malformed_total` once per isolate; a body the decoration did
//! not reach (a word-store fault) is byte-identical, and a decorated body is its old shape plus exactly one key.
use bsv_tracker::{Hint, HintStatus, HostAction, Input, Params, State};
use low_app_layer::tracker_host::{runtime, served};
use low_app_layer::{courier, logic, txany};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

fn tx(n: u8) -> String {
    format!("{n:02x}").repeat(32)
}

struct NoHeaders;
impl bsv_tracker::Headers for NoHeaders {
    fn header_at(&self, _: u32) -> Result<Option<bsv_tracker::Header>, bsv_tracker::HeaderError> {
        Ok(None)
    }
    fn tip_height(&self) -> Result<u32, bsv_tracker::HeaderError> {
        Ok(20)
    }
}

fn seen(id: &str) -> Value {
    let p = Params { age_threshold: 60 };
    let mut s = State::new(id);
    s.step(&p, &NoHeaders, Input::Host(HostAction::Build))
        .unwrap();
    s.step(
        &p,
        &NoHeaders,
        Input::Hint(Hint::new("arcade", HintStatus::Seen, 7)),
    )
    .unwrap();
    served::fields(&s)
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

fn views() -> BTreeMap<String, Value> {
    BTreeMap::from([(tx(1), seen(&tx(1))), (tx(2), seen(&tx(2)))])
}

/// Each route's body as the production decoration leaves it today (the words, no depth), with the route's name and
/// the paths of the objects the depth must ride: `(route, decorated body, json pointers)`.
fn decorated() -> Vec<(&'static str, Value, Vec<String>)> {
    let answer = txany::decide_tx_any(
        None,
        None,
        Some(&txany::TxObservation::FirstPartyPresent {
            confirmed: false,
            raw_hex: Some("0100".into()),
        }),
        txany::AbsenceCorroboration::Unknown,
    );
    let v = views();
    let mut subject: Value = serde_json::from_str(&txany::tx_any_body(&tx(1), &answer)).unwrap();
    runtime::decorate(&mut subject, Some(&tx(1)), &v);
    let mut batch: Value = serde_json::from_str(&txany::tx_any_batch_body(
        &[(tx(1), answer.clone()), (tx(2), answer.clone())],
        &[(tx(3), "fault")],
    ))
    .unwrap();
    runtime::decorate(&mut batch, None, &v);
    let batch_paths: Vec<String> = batch["answers"]
        .as_object()
        .unwrap()
        .keys()
        .map(|k| format!("/answers/{k}"))
        .collect();
    assert!(
        batch_paths.len() >= 2,
        "the batch carries its answers: {batch}"
    );
    let entries = [
        status(&tx(5), Some(true), Some(&tx(1))),
        status(&tx(5), Some(false), None),
        status(&tx(6), None, None),
    ];
    let entry_paths: Vec<String> = (0..entries.len()).map(|i| format!("/{i}")).collect();
    let mut utxo: Value = serde_json::from_str(&logic::utxo_status_body(&entries)).unwrap();
    runtime::decorate(&mut utxo, None, &v);
    runtime::decorate_outpoints(&mut utxo, &v, "free", "host");
    let mut spent: Value =
        serde_json::from_str(&txany::spent_any_body(&entries, &[false; 3])).unwrap();
    runtime::decorate_outpoints(&mut spent, &v, "unspentAsFarAsAsked", "hint");
    runtime::decorate(&mut spent, None, &v);
    vec![
        ("/tx-any", subject, vec![String::new()]),
        ("/tx-any batch", batch, batch_paths),
        ("/utxo-status", utxo, entry_paths.clone()),
        ("/spent-any", spent, entry_paths),
    ]
}

/// The field: `spendDepth` is the var's value beside `tracker` (the subject, every answer) and beside `lock` (every
/// outpoint entry), on all three routes.
#[test]
fn l7_the_depth_rides_beside_every_served_word_on_the_three_routes() {
    for (raw, want) in [(Some("1"), 1u64), (Some("3"), 3), (Some(" 6 "), 6)] {
        let depth = runtime::spend_depth_of(raw);
        assert_eq!(depth.depth, want, "SPEND_DEPTH={raw:?}");
        assert!(depth.from_var, "SPEND_DEPTH={raw:?} is a well-formed var");
        for (route, mut body, paths) in decorated() {
            runtime::stamp_spend_depth(&mut body, depth.depth);
            for p in &paths {
                let at = body.pointer(p).unwrap();
                let beside = if route.starts_with("/tx-any") {
                    "tracker"
                } else {
                    "lock"
                };
                assert!(at.get(beside).is_some(), "{route} {p}: no `{beside}`");
                assert_eq!(
                    at["spendDepth"],
                    json!(want),
                    "{route} {p}: spendDepth beside `{beside}` is the var's value: {at}"
                );
            }
        }
    }
}

/// The byte-identity: each stamped object is its old shape plus exactly one key, and with that key taken out the
/// whole body is byte-identical to the decorated body of today. A body the decoration never reached (the legacy
/// body a word-store fault serves) gains nothing: the decoration never fails an available legacy answer.
#[test]
fn l7_the_old_shape_plus_exactly_one_key() {
    for (route, before, paths) in decorated() {
        let mut after = before.clone();
        runtime::stamp_spend_depth(&mut after, 2);
        for p in &paths {
            let old: BTreeSet<_> = before
                .pointer(p)
                .unwrap()
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect();
            let new: BTreeSet<_> = after
                .pointer(p)
                .unwrap()
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect();
            let added: Vec<_> = new.difference(&old).cloned().collect();
            assert_eq!(
                added,
                vec!["spendDepth".to_string()],
                "{route} {p}: the keys added"
            );
            assert!(old.is_subset(&new), "{route} {p}: a key went missing");
        }
        let mut stripped = after.clone();
        for p in &paths {
            stripped
                .pointer_mut(p)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove("spendDepth");
        }
        assert_eq!(
            stripped.to_string(),
            before.to_string(),
            "{route}: a byte moved beside the one key"
        );
    }
    // the legacy bodies (no word decoration reached them) are untouched, byte for byte
    let entries = [
        status(&tx(5), Some(true), Some(&tx(1))),
        status(&tx(6), None, None),
    ];
    for raw in [
        logic::utxo_status_body(&entries),
        txany::spent_any_body(&entries, &[false; 2]),
        txany::tx_any_batch_body(&[], &[(tx(3), "fault")]),
    ] {
        let mut v: Value = serde_json::from_str(&raw).unwrap();
        let legacy = v.to_string();
        runtime::stamp_spend_depth(&mut v, 4);
        assert_eq!(
            v.to_string(),
            legacy,
            "an undecorated legacy body gained a key"
        );
    }
}

/// The floor: a missing var serves 1, and so does every malformed one; each is counted
/// `spend_depth_var_malformed_total` ONCE per isolate (a native test thread is the isolate's thread-local state).
#[test]
fn l7_a_missing_var_is_the_floor_counted_once() {
    let d = runtime::spend_depth_of(None);
    assert_eq!((d.depth, d.from_var), (runtime::SPEND_DEPTH_FLOOR, false));
    assert_eq!(runtime::SPEND_DEPTH_FLOOR, 1);
    for _ in 0..3 {
        assert_eq!(runtime::spend_depth_noted(None).depth, 1);
    }
    let pending = courier::take_pending_if_due(0.0);
    let count = pending
        .iter()
        .find(|(k, _)| k == runtime::SPEND_DEPTH_MALFORMED_COUNTER)
        .map(|(_, n)| *n);
    assert_eq!(count, Some(1), "counted once per isolate: {pending:?}");
    assert_eq!(
        runtime::SPEND_DEPTH_MALFORMED_COUNTER,
        "spend_depth_var_malformed_total"
    );
}

#[test]
fn l7_a_malformed_var_is_the_floor_counted_once() {
    for raw in [
        "",
        " ",
        "0",
        "-1",
        "1.5",
        "abc",
        "2x",
        "+2",
        "0x2",
        "99999999999999999999",
    ] {
        let d = runtime::spend_depth_of(Some(raw));
        assert_eq!(
            (d.depth, d.from_var),
            (1, false),
            "SPEND_DEPTH={raw:?} is malformed: the floor"
        );
    }
    for raw in ["abc", "0", "abc"] {
        assert_eq!(runtime::spend_depth_noted(Some(raw)).depth, 1);
    }
    // a well-formed var is never counted
    assert_eq!(runtime::spend_depth_noted(Some("2")).depth, 2);
    let pending = courier::take_pending_if_due(0.0);
    let count = pending
        .iter()
        .find(|(k, _)| k == runtime::SPEND_DEPTH_MALFORMED_COUNTER)
        .map(|(_, n)| *n);
    assert_eq!(count, Some(1), "counted once per isolate: {pending:?}");
}

/// The health read: the app layer's `/health` serves `spendDepth` (the depth in force, its source, the floor and the
/// var as set) so an operator reads what the host says.
#[test]
fn l7_health_serves_the_depth_read() {
    let h = runtime::spend_depth_health_json(Some("1"));
    assert_eq!(h["depth"], 1, "{h}");
    assert_eq!(h["source"], "var", "{h}");
    assert_eq!(h["floor"], 1, "{h}");
    assert_eq!(h["var"], "1", "{h}");
    let h = runtime::spend_depth_health_json(Some("zero"));
    assert_eq!(
        (h["depth"].clone(), h["source"].clone()),
        (json!(1), json!("floor")),
        "{h}"
    );
    assert_eq!(h["var"], "zero", "{h}");
    let h = runtime::spend_depth_health_json(None);
    assert_eq!(
        (h["depth"].clone(), h["source"].clone()),
        (json!(1), json!("floor")),
        "{h}"
    );
    assert!(h["var"].is_null(), "{h}");
    let routes = include_str!("../src/routes.rs");
    let health = &routes[routes.find("pub async fn health(").unwrap()..];
    let health = &health[..health.find("\n}\n").unwrap()];
    assert!(
        health.contains("body[\"spendDepth\"]") && health.contains("spend_depth_health_json("),
        "/health serves no spendDepth read"
    );
}

/// The wiring: every production boundary of the three routes stamps the depth it read from the env, after the words.
#[test]
fn l7_the_production_boundaries_stamp_the_depth() {
    let rt = include_str!("../src/tracker_host/runtime.rs");
    for f in ["pub async fn body_loaded(", "pub async fn outpoint_body("] {
        let body = &rt[rt.find(f).unwrap()..];
        let body = &body[..body.find("\n}\n").unwrap()];
        assert!(
            body.contains("stamp_spend_depth(") && body.contains("spend_depth(env)"),
            "{f} serves no spendDepth"
        );
    }
    let batch = &rt[rt.find("pub async fn batch_body(").unwrap()..];
    assert!(batch[..batch.find("\n}\n").unwrap()].contains("body_loaded("));
    let routes = include_str!("../src/routes.rs");
    let spent = &routes[routes
        .find("crate::txany::spent_any_body_sourced(")
        .unwrap()..];
    let spent = &spent[..spent.find("\n}\n").unwrap()];
    assert!(
        spent.contains("runtime::stamp_spend_depth(")
            && spent.contains("runtime::spend_depth(&ctx.env)"),
        "/spent-any serves no spendDepth"
    );
    let tx_any = &routes[routes.find("pub async fn tx_any(").unwrap()..];
    assert!(tx_any[..tx_any.find("\n}\n").unwrap()].contains("runtime::body_loaded("));
}

/// Both renders set `SPEND_DEPTH = "1"` (the value at risk per R3(b) is the owner's later word).
#[test]
fn l7_both_renders_set_spend_depth_1() {
    let toml = include_str!("../wrangler.toml");
    let prod = &toml[toml.find("\n[vars]\n").unwrap()..];
    let prod = &prod[..prod.find("\n[[").unwrap()];
    let beta = &toml[toml.find("\n[env.beta.vars]\n").unwrap()..];
    let beta = &beta[..beta.find("\n[[").unwrap()];
    for (render, vars) in [("prod", prod), ("beta", beta)] {
        assert!(
            vars.lines().any(|l| l.trim() == "SPEND_DEPTH = \"1\""),
            "the {render} render sets no SPEND_DEPTH = \"1\""
        );
    }
}

/// `/health/invariants` (the overlay's) serves the malformed-var count from 0: the overlay seeds the name, and the
/// app layer lists it among its mirrored names (so `obs_628_every_name_is_seeded_by_the_overlay` holds both sides).
#[test]
fn l7_the_overlay_seeds_the_malformed_count() {
    let name = runtime::SPEND_DEPTH_MALFORMED_COUNTER.to_string();
    assert!(
        bsv_overlay_cloudflare::applayer_counters::names().contains(&name),
        "the overlay's /health/invariants does not seed {name}"
    );
    assert!(runtime::tracker_mirrored_counter_names().contains(&name));
}
